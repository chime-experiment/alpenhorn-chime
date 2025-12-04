"""SciNetHPSS I/O class.

This Node I/O class contains the code necessary to manage files on SciNet HPSS.

NB: This is not (yet?) feature-complete.
"""

import logging
import os
import pathlib
import time
from collections.abc import Hashable
from typing import IO

import peewee as pw

from alpenhorn.common.util import pretty_bytes
from alpenhorn.daemon import UpdateableNode
from alpenhorn.daemon.scheduler import FairMultiFIFOQueue, Task
from alpenhorn.db import (
    ArchiveFile,
    ArchiveFileCopy,
    ArchiveFileCopyRequest,
    StorageNode,
)
from alpenhorn.io.base import BaseNodeRemote
from alpenhorn.io.default import DefaultNodeIO

from .ops import HPSSOps

log = logging.getLogger(__name__)


class SciNetHPSSNodeRemote(BaseNodeRemote):
    """The Remote Node class for SciNet HPSS.

    This class represents a SciNet StorageNode when used as the source for a
    pull request (when it might not be local to the daemon initiating the request.
    """

    def __init__(self, node: StorageNode, config: dict) -> None:
        super().__init__(node, config)

        self.staging_root = pathlib.Path(config["staging_root"])

    def pull_ready(self, file: ArchiveFile) -> bool:
        """Returns False.

        Recall from SciNet is not yet implemented.  Returning
        False here discourages attempts to pull files out of
        SciNet."""
        return False

    def remote_pull_ok(self, host: str) -> bool:
        """HPSS doesn't permit remote pulls."""
        return False


class SciNetHPSSNodeIO(DefaultNodeIO):
    """SciNet HPSS Node I/O.

    Required io_config keys:
        * staging_root: the absolute path to the staging directory
        * staging_max: the maximum size (in bytes) of data in the staging
            directory

    Optional io_config keys:
        * scan_period_hours: how often to re-scan the staging directory to
            detect inconsistency, in hours.  If this is non-positive, scanning
            is disabled (other than the initial scan done when the node first
            becomes available).  Defaults to 24 (i.e. once a day).
        * recall_max: the maximum number of files to gang together in a single
            transfer out of HPSS.  Default is 40.
        * ingest_max: the maximum number of files to gang together in a single
            transfer into HPSS.  Default is 500.
        * bundle_max: the maximum size (in bytes) for a single transfer in or
            out of HPSS.  Default is 800 GiB.
        * time_max: the maximum time, in seconds, to wait before processing a
            HPSS file transfer (stage/archive).  If there are pending transfers
            that have waited at least this long to be processed, a job will
            be submitted even if it doesn't have recall_max or ingest_max files
            to process.  Default is 3600 (one hour).
    """

    remote = SciNetHPSSNodeRemote

    def __init__(
        self,
        node: UpdateableNode,
        config: dict,
        queue: FairMultiFIFOQueue,
        fifo: Hashable,
    ) -> None:
        super().__init__(node, config, queue, fifo)

        self.staging_max = int(config["staging_max"])
        self.staging_root = pathlib.Path(config["staging_root"])

        # Anything else in the I/O config is used by HPSSOps
        self._ops = HPSSOps(self.node, config)

        # Init (node start-up):
        self.need_talkback_init = True
        self.need_staging_init = True

        # Running total of space used by staging, in bytes
        self.staging_used = 0

        # Time of last scan of staging.  This will be set to
        # a meaningful value during staging init.
        self.last_scan = 0

        # Scan period.  Defaults to once a day.  The config value
        # is in hours, so we convert to seconds here.
        self.scan_period = float(config.get("scan_period_hours", 24.0)) * 3600
        # Use zero to indicate scanning is disabled.
        if self.scan_period < 0:
            self.scan_period = 0

        # True whenever a talkback handler task is queued/running
        self.talkback_running = False

    def _staging_path(self, file: ArchiveFile) -> str:
        """Path to a staged file.

        Files in staging are simply named by their ArchiveFile.id
        to avoid having"""
        return self.staging_root.joinpath(str(file.id))

    def scan_staging(self) -> None:
        """Scan the staging directory and update internal state.

        This is run at init time to make sure the internal state reflects the
        filesystem state.

        After that, it's periodically re-run to catch filesystem updates.
        (The staging directory at SciNet is in /scratch.  We should expect files
        to get removed from time-to-time.)
        """

        # I/O
        def _async(task, io):
            # The set of (properly) staged files
            staged = set()

            # size of all staged files
            staging_used = 0

            # Scan staging
            for entry in os.scandir(io.staging_root):
                # Skip non-files
                if not entry.is_file(follow_symlinks=False):
                    continue

                # Filename should be an int
                try:
                    file_id = int(entry.name)
                except ValueError:
                    log.warning(f'Removing spurious file "{entry.name}" from staging')
                    pathlib.Path(entry.path).unlink(missing_ok=True)
                    continue

                # Find the copy record
                copy = ArchiveFileCopy.get_or_none(file_id=file_id, node=io.node)

                # Don't allow staging files which aren't archived
                if copy is None or copy.has_file == "N":
                    log.warning(f"Removing unarchived file #{file_id} from staging")
                    pathlib.Path(entry.path).unlink(missing_ok=True)
                    continue

                # Otherwise, mark the copy as staged, if needed
                if not copy.ready:
                    copy.ready = True
                    copy.last_update = pw.utcnow()
                    copy.save()

                # And add it to the staged list
                staged.add(file_id)

                # Update running total
                staging_used += copy.file.size_b

            # Finally, replace the old set of staged files with the new set
            # This takes care of removing files that are now gone from the set
            io.staged_files = staged
            io.staging_used = staging_used

            # If this was the staging init, record success
            if io.need_staging_init:
                log.info(f"Staging init complete for node {io.node.name}")
                io.need_staging_init = False

        # This I/O task is exclusive, so we can be sure there's nothing updating
        # staging while we scan it.
        Task(
            func=_async,
            queue=self._queue,
            key=self.fifo,
            args=(self,),
            exclusive=True,
            name=f"Scan staging for {self.node.name}",
        )

    def handle_talkback(self) -> None:
        """Deal with the results of HPSS jobs.

        The HPSS jobs create files in <staging-root>/talkback indicating
        successful operations.  We need to deal with these talkback files to
        keep our internal state correct.

        Talkback files are empty: all information is encoded in their filename.
        """

        # Everything happens in a job
        def _async(task, io):
            # We lower the talkback_running flag in a task cleanup function so that
            # it always happens and the flag can never get stuck on
            def _talkback_done(io):
                io.talkback_running = False

            task.on_cleanup(_talkback_done, args=(io,))

            # Call the ops's 'update' genrator function, which will yield file ids
            # of new files added to the archive
            for req_id, file_id, deltat, success in io._ops.update():
                # req_id is non-zero for ingest requests
                if req_id:
                    # This is an ingest
                    #
                    # Find the AFCR record
                    try:
                        req = ArchiveFileCopyRequest.get(id=req_id)
                    except pw.DoestNotExist:
                        log.warning(f"Ignoring bad request#{req_id} in talkback.")
                        continue

                    # Check for an already-finished request.  This could happen due to
                    # queue congestion and/or alpenhorn restarting.  If that's the case
                    # there's nothing needing update in the database.
                    if req.completed or req.cancelled:
                        log.debug(f"Ignoring already-completed request#{req_id}.")
                        continue

                    # Update the request
                    req.finish(
                        node_to=self.node,
                        size=None,
                        success=success,
                        md5ok=True,
                        # Fake the start time so the rate calcuation comes out correct(-ish)
                        start_time=time.time() - deltat,
                        # On failure, we always do a check of the source file
                        check_src=True,
                        # Generic message for failure
                        stderr="HPSS ingest failed",
                    )
                else:
                    # This is a recall
                    #
                    # Mark the file copy as staged (ready)
                    updated = (
                        ArchiveFileCopy.update(ready=True, last_update=pw.utcnow())
                        .where(file_id=file_id, node=io.node)
                        .execute()
                    )

                    # If the update happened, add it to the running
                    # list of staged files.  If it didn't happen
                    # (which will be because there was no filecopy
                    # record), delete it from staging.
                    if updated:
                        file = ArchiveFile.get(id=file_id)

                        io.staged_files.add(file_id)
                        io.staging_used += file.size_b
                    else:
                        log.warning(
                            f"Deleting unregistered file #{file_id} from staging"
                        )
                        self._staging_path(file).unlink(missing_ok=True)

            # If this was the talkback init, record success
            if io.need_talkback_init:
                log.info(f"Talkback init complete for node {io.node.name}")
                io.need_talkback_init = False

        # Only queue this task once
        if not self.talkback_running:
            # The task will lower this flag when it completes
            self.talkback_running = True
            Task(
                func=_async,
                queue=self._queue,
                key=self.fifo,
                args=(self,),
                name=f"Check talkback for {self.node.name}",
            )

    def clean_staging(self) -> None:
        """Delete files from staging to stay below the max.

        This is called at the start of a node update (from
        `before_update`), but only if the update is going
        to happen (i.e. the node is currently idle).
        """

        overage = self.staging_used - self.staging_max

        # Nothing to do
        if overage <= 0:
            return

        def _async(task, node, lfs, overage):
            total_files = 0
            total_bytes = 0

            # loop through file copies until we've released enough
            # (or we run out of files)
            for copy in (
                ArchiveFileCopy.select()
                .where(ArchiveFileCopy.file << self.staged_files)
                .order_by(ArchiveFileCopy.last_update)
            ):
                log.debug(
                    f"unstaging file copy {copy.path} "
                    f"[={pretty_bytes(copy.file.size_b)}] for node {self.node.name}"
                )
                # Update copy record
                ArchiveFileCopy.update(ready=False, last_update=pw.utcnow()).where(
                    ArchiveFileCopy.id == copy.id
                ).execute()

                # Actually delete the file
                self._staging_path(copy.file).unlink(missing_ok=True)

                # Remove from the internal list
                self.staged_files.discard(copy.file.id)

                total_files += 1
                total_bytes += copy.file.size_b
                if total_bytes >= overage:
                    break
            log.info(
                f"Unstaged {pretty_bytes(total_bytes)} in {total_files} "
                f"{'file' if total_files == 1 else 'files'} "
                f"for node {self.node.name}"
            )
            return

        # Do the rest asynchronously
        Task(
            func=_async,
            queue=self._queue,
            key=self.fifo,
            args=(self.node, self._lfs, overage),
            name=f"Node {self.node.name}: unstage {pretty_bytes(overage)}",
        )

    # Normal Alpenhorn methods

    def before_update(self, idle: bool) -> bool:
        """Pre-update hook.

        Takes care of the staging init.

        After that, if the node is idle (i.e. the update will
        happen), then call `self.release_files to potentially
        free up space in staging.

        Parameters
        ----------
        idle : bool
            Is the node currently idle?

        Returns
        -------
        True if init has completed; False, otherwise.
        """
        # Initialise the internal staging directory monitoring state
        if self.need_talkback_init or self.need_staging_init:
            log.info(f"Cancelling update for {self.node.name}: node start-up running")
            if self.need_talkback_init:
                self.handle_talkback()

            if self.need_staging_init:
                # This one is an exclusive job, so it's going
                # to wait for handle_talkback to finish, if that
                # was also submitted now.
                self.scan_staging()

            # Skip update until init completes
            return False

        # Clear staging space, if necessary
        # DISABLED: recall isn't implemented
        # if idle:
        #    self.clean_staging()

        # Handle the job talkback
        self.handle_talkback()

        # Update the status of running slurm jobs
        self._ops.check_jobs()

        # Check whether pending HPSS requests are over-time
        self._ops.recall(None)
        self._ops.ingest(None)

        # Continue with the update
        return True

    def idle_update(self, newly_idle) -> None:
        """Idle update hook.

        Occasionally we re-scan staging when the node is idle.
        """

        # Every once-in-a-while re-scan staging
        if self.last_scan and self.scan_period:
            if time.time() - self.last_scan > self.scan_period:
                self.scan_staging()

    # I/O METHODS

    def bytes_avail(self, fast: bool = False) -> int | None:
        """Return None.

        We don't track available space in HPSS.
        """
        return None

    def check(self, copy: ArchiveFileCopy) -> None:
        """Check a file in HPSS.

        If the file is unstaged, we trigger a restore and wait
        for it to become available.

        Then the file is verified.

        Parameters
        ----------
        copy : ArchiveFileCopy
            the file copy to check
        """
        # HSI can give us the MD5 sum of a file on tape.  We should use that...
        raise NotImplementedError("check not supported")

    def check_init(self) -> bool:
        """Check that this node is initialised.

        ScinetHPSS is considered initialised if the "talkback" and "scripts"
        directories exist in staging.

        Returns True if they are, or False if at least one of them aren't.
        """
        talkback = self.staging_root.joinpath("talkback")
        if not talkback.is_dir():
            log.warning(f"No such directory: {talkback}")
            return False

        scripts = self.staging_root.joinpath("scripts")
        if not scripts.is_dir():
            log.warning(f"No such directory: {scripts}")
            return False

        return True

    def delete(self, copies: list[ArchiveFileCopy]) -> None:
        """Delete files from HPSS.

        Not implemented.
        """
        raise NotImplementedError("deletion not supported")

    def exists(self, path: pathlib.PurePath) -> bool:
        """Does `path` exist?

        We can't figure that out on HPSS, so we just return False and
        hope for the best.

        Parameters
        ----------
        path : pathlib.PurePath
            path relative to `node.root`

        Returns
        -------
        bool
            False
        """
        return False

    def storage_used(self, path: pathlib.Path) -> None:
        """Returns None.

        This method is supposed to return, if possible, the amount of
        space consumed by `path`.  But that's not a well defined value in
        HPSS, so this method just returns None to indicate that.

        Parameters
        ----------
        path: path-like
            The filepath to check the size of.  May be absolute or relative
            to `node.root`.

        Returns
        -------
        None
            None.
        """
        return None  # noqa: RET501

    def fits(self, size_b: int) -> bool:
        """Does `size_b` bytes fit on this node?

        Returns True: everything fits in HPSS.
        """
        return True

    def init(self) -> bool:
        """Initialise this node.

        This creates the talkback subdirectory in staging.
        """
        try:
            self.staging_root.joinpath("talkback").mkdir(mode=0o0755, exist_ok=True)
            self.staging_root.joinpath("scripts").mkdir(mode=0o0755, exist_ok=True)
        except OSError:
            return False
        return True

    def open(self, path: os.PathLike | str, binary: bool = True) -> IO:
        """Open the file specified by `path` for reading.

        Used during import.  Not implemented.

        Parameters:
        -----------
        path : pathlike
            Relative to `node.root`
        binary : bool, optional
            If True, open the file in binary mode, otherwise open the file in
            text mode.

        Returns
        -------
        file : file-like
            An open, read-only file.
        """
        raise NotImplementedError("import not supported")

    def pull(self, req: ArchiveFileCopyRequest, did_search: bool) -> None:
        """Pull file specified by copy request `req` onto `self.node`.

        Doesn't actually pull, just queues an "ingest" request for the file.

        Parameters
        ----------
        req : ArchiveFileCopyRequest
            the copy request to fulfill.  We are the destination node (i.e.
            `req.group_to == self.node.group`).
        did_search : boolean
            True if a group-level pre-pull search for an existing file was
            performed.  False otherwise.  Ignored here.
        """

        # If this is a non-local pull, cancel it.
        if not req.node_from.local:
            log.info(
                f"Cancelling non-local pull for {req.file.path} onto node {self.node.name}"
            )
            req.cancel("non-local")
            return

        # Otherwise, add a new ingest request
        self._ops.ingest(req)

    def ready_path(self, path: os.PathLike) -> bool:
        """Stage the specified path so it can be read.

        Not implemented: SciNetHPSS doesn't support auto-import.

        Parameters
        ----------
        path : path-like
            The path that we want to perform I/O on.

        Returns
        -------
        ready : bool
            True if `path` is ready for I/O.  False otherwise.
        """
        raise NotImplementedError("auto-import not supported")

    def ready_pull(self, req: ArchiveFileCopyRequest) -> None:
        """Ready a file to be pulled as specified by `req`.

        Parameters
        ----------
        req : ArchiveFileCopyRequest
            the copy request to ready.  We are the source node (i.e.
            `req.node_from == self.node`).
        """

        # If the file is already staged, there's nothing to do
        if req.file.id in self.staged_files:
            return

        # Otherwise, add a new recall request
        self._ops.recall(req.file)

    def release_bytes(self, size: int) -> None:
        """Does nothing."""
        pass

    def reserve_bytes(self, size: int, check_only: bool = False) -> bool:
        """Returns True."""
        return True
