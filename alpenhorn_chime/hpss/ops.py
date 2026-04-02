"""HPSS Operations

This module implements the ingest and recall logic for the
SciNetHPSS Node I/O class.
"""

import json
import logging
import os
import pathlib
import threading
import time
from typing import IO

from collections import deque

from alpenhorn.common import config
from alpenhorn.common.util import pretty_bytes
from alpenhorn.db import ArchiveFile, ArchiveFileCopyRequest, StorageNode
from alpenhorn.daemon import RemoteNode
from alpenhorn.daemon.proc import run_command

log = logging.getLogger(__name__)

# TODO: make this a config parameter, or something
PENDING_JOB_MAX = 1


class OpsQueue:
    """Job queue for a single HPSS operation.

    This is where all the SLURM interaction happens.

    Parameters
    ----------
    op : str
        One of "ingest" or "recall" or "check" indicating the
        operation being managed.
    name : str
        The name of the StorageNode
    node_id : int
        The StorageNode.id
    count_max : int
        Maximum number of files in a single job
    bundle_max : int
        Maximum total size in bytes of files in a single job
    time_max : int
        Maximum time in seconds for any item in the wait queue
    hpss_root : str
        Base path in HPSS
    staging_root : pathlib.Path
        Path to the staging directory
    """

    def __init__(
        self,
        op: str,
        name: str,
        node_id: int,
        count_max: int,
        bundle_max: int,
        time_max: int,
        hpss_root: str,
        staging_root: pathlib.Path,
    ) -> None:
        # === CONFIG ATTRIBUTES

        # Check for valid op
        if op in ("ingest", "check", "recall"):
            self.op = op
        else:
            raise ValueError(f"unknown op: {op}")
        self.name = name
        self.node_id = node_id
        self.count_max = count_max
        self.bundle_max = bundle_max
        self.time_max = time_max
        self.hpss_root = hpss_root
        self.staging_root = staging_root
        self.talkback_dir = staging_root.joinpath("talkback")
        self.script_dir = staging_root.joinpath("scripts")
        self.username = os.getlogin()  # Used when running squeue

        # Number of pending jobs allowed in the slurm queue
        self.pending_job_max = PENDING_JOB_MAX

        # === DATA ATTRIBUTES

        # a threading.Lock used to lock access the internals
        self._lock = threading.Lock()

        # a deque of files waiting to be put into a slurm job
        self._waiting = deque()

        # The total size of files in the wait queue
        self._wait_size = 0

        # a set of file_ids of file which are in active slurm jobs
        self._running = set()

        # a dict of jobs, with sets of file_ids like above
        self._jobs = dict()

    def push(
        self, file: ArchiveFile = None, req: ArchiveFileCopyRequest = None
    ) -> None:
        """Add something to the queue.

        Adds either `file` or `req` (or both) to the queue, if not already
        present.  Then perhaps queues a new job, if we've reached the limits
        for that.

        Normally, the `req` is used to push to the ingest queue and `file` to
        the recall and check queues.  That's not enforced, but odd things might
        happen if it's not the case.

        If neither `file` nor `req` are given, then nothing is added but the
        queue check still happens.

        Parameters
        ----------
        file : ArchiveFile
            An ArchiveFile to add to the queue.
        req : ArchiveFileCopyRequest
            An ArchiveFileCopyRequest to add to the queue.
        """
        # Set file if not given
        if req and not file:
            file = req.file

        # Lock
        with self._lock:
            # Is there something to add?
            if file is not None:
                # First check whether the request is already in a running job
                if file.id in self._running:
                    log.debug(
                        f"Skipping {self.op} of {file.path}: already in progress on node {self.name}"
                    )
                    do_add = False
                else:
                    do_add = True

                # Next check whether the request is already waiting.
                if do_add:
                    for waiting in self._waiting:
                        if waiting[1] == file:
                            log.debug(
                                f"Skiping {self.op} of {file.path}: already waiting on node {self.name}"
                            )
                            do_add = False
                            break

                # Add the new request, if needed
                if do_add:
                    log.info(
                        f"Added {file.path} to {self.op} FIFO for node {self.name}"
                    )
                    # req might be None here: that's fine
                    self._waiting.append((time.monotonic(), file, req))
                    if file.size_b:
                        self._wait_size += file.size_b

            # Whether or not we added a file, now check if we need to queue a new job
            wait_count = len(self._waiting)

            if wait_count == 0:
                # Nothing in the queue
                return
            if self._wait_size >= self.bundle_max:
                # Reached file size max; queue job
                pass
            elif wait_count >= self.count_max:
                # Reached file count max; queue job
                pass
            elif time.monotonic() - self._waiting[0][0] >= self.time_max:
                # Reached file wait-time max; queue job
                pass
            else:
                # Otherwise, no need to queue a job, just return
                return

            # Create and queue a new slurm job.
            self.queue_job()

    def slurm_command(self, command: list[str]) -> dict | None:
        """Run slurm command and return parsed JSON.

        Parameters
        ----------
        command : list
            The command to run, as a list of strings.  A --json
            flag will be appended to the command.

        Returns
        -------
        dict or None
            None if the command failed, or the output JSON couldn't
            be decoded.  Otherwise a dict representation of the JSON
            output from the command.
        """

        command.append("--json")

        ret, out, err = run_command(command, timeout=600)
        if ret is None or ret != 0:
            command = " ".join(command)
            log.warning(f"failed command (ret={ret}): {command}\n{err}")
            return None

        try:
            return json.loads(out)
        except json.JSONDecodeError as e:
            log.warning(f"Unable to decode {command[0]} output:\n{e}")
            log.debug(f"Command output: {out}")
            return None

    def slurm_jobs(self) -> list:
        """Retrieve the list of slurm jobs from the queue.

        Only counts pending jobs

        Returns
        -------
        list
            The list of jobs.
        """

        out = self.slurm_command(["squeue", "-u", self.username])
        if not out or "jobs" not in out:
            return []
        return out["jobs"]

    def slurm_pending_count(self) -> int:
        """Report the number of pending jobs."""

        count = 0
        for job in self.slurm_jobs():
            if "PENDING" in job["job_state"]:
                count += 1
        return count

    def check_jobs(self) -> None:
        """Check the status of queued slurm jobs.

        We're interested in finding jobs which have
        finished, so we can remove them from our list
        of running jobs.
        """
        # Loop over running jobs
        completed = []
        for job in self._jobs:
            # Get status from slurm
            out = self.slurm_command(["sacct", "--name", job])
            if not out or "jobs" not in out:
                continue
            # Really, there should only be one job here (since we
            # assume job names are unique, so we'll take any
            # positive result.
            for slurm_job in out["jobs"]:
                try:
                    if "COMPLETED" in slurm_job["state"]["current"]:
                        # Job is done.  Remove the files in this job from
                        # the running set
                        self._running -= self._jobs[job]
                        # And add it to the list of completed jobs
                        completed.append(job)
                        break
                except IndexError:
                    pass

        # Remove completed jobs from the job list
        for job in completed:
            log.info(f'Job "{job}" for node "{self.name}" complete.')
            del self._jobs[job]

    def _write_script_loop(self, transfers: list, f: IO) -> None:
        """Write the unrolled operational loop to a script.

        Parameters
        ----------
        transfers : list
            The list of transfers to create a job for.  These
            are tuples with elements:
                - time.monotonic() value
                - ArchiveFile
                - ArchiveFileCopyRequest or None
        f : IO
            The file to write to.
        """
        # Get format string for operation, if any
        if self.op == "ingest":
            loop = _INGEST_LOOP
        elif self.op == "check":
            loop = _CHECK_LOOP
        else:
            raise NotImplementedError(f"Unsupported op: {self.op}")

        for item in transfers:
            _, file, req = item

            # Specal stuff for ingest
            if self.op == "ingest":
                if req is None:
                    log.warning(f"Ignoring ingest request for {file.name}: no req.")
                    continue

                req_id = req.id

                remote = RemoteNode(req.node_from)
                ext_path = remote.io.file_path(req.file)
            else:
                ext_path = ""
                req_id = 0

            f.write(
                loop.format(
                    ext_path=ext_path,
                    acq_dir=file.acq.name,
                    file_id=file.id,
                    file_name=file.name,
                    md5sum=file.md5sum,
                    req_id=req_id,
                )
            )

    def _gen_script(self, transfers: list) -> pathlib.Path | None:
        """Write job script to disk.

        Parameters
        ----------
        transfers : list
            The list of transfers to create a job for.

        Returns
        -------
        str or None
            If an error occurs, this is None.  Otherwise
            it is the name of the job script written into
            the talkback directory.
        """
        job_name = str(int(time.time())) + "_" + str(self.node_id)
        # Create a new job script
        job_path = self.script_dir.joinpath(job_name)

        try:
            # Open the job script for writing
            with job_path.open("wt") as f:
                # Write the header
                f.write(
                    _HEADER.format(
                        job_name=job_name,
                        hpss_root=self.hpss_root,
                        talkback_dir=self.talkback_dir,
                        staging_root=self.staging_root,
                        node_id=self.node_id,
                    )
                )
                self._write_script_loop(transfers, f)
        except (OSError, NotImplementedError) as e:
            log.error(f"Unable to create script {job_path}: {e}")
            job_path.unlink(missing_ok=True)
            return None

        log.debug(f"Wrote job script {job_path}")
        return job_name

    def queue_job(self) -> None:
        """Create and queue a new slurm job."""

        # If we already have the max number of jobs pending, do nothing.
        if self.slurm_pending_count() >= self.pending_job_max:
            log.debug(f"Skipping {self.op} for node {self.name}: queue full")
            return

        transfer_size = 0
        count = 0
        transfers = list()

        # Collect files to transfer
        while True:
            try:
                item = self._waiting.popleft()
            except IndexError:
                break
            transfers.append(item)
            count += 1
            transfer_size += item[1].size_b

            if count >= self.count_max:
                break
            if transfer_size >= self.bundle_max:
                break

        log.debug(
            f"Bundled {count} files ({pretty_bytes(transfer_size)}) for {self.op}"
        )

        # Compose the job script
        job_name = self._gen_script(transfers)

        # if script generation failed return the stuff removed to the waiting list
        if not job_name:
            self._waiting.extendleft(transfers)
            return

        # Temporarily change to the script directory to submit the job
        cwd = os.getcwd()
        try:
            os.chdir(self.script_dir)
            ret, _, stderr = run_command(
                ["sbatch", str(self.script_dir.joinpath(job_name))]
            )
            if ret:
                log.error(f"job sumission failed: {stderr}")
                # put all the things back
                self._waiting.extendleft(transfers)
        finally:
            # Always return to the working directory, regardless of
            # what happened
            os.chdir(cwd)

        # We're done, if job submission failed
        if ret:
            return

        log.info(
            f'submitted job "{job_name}" for node {self.name} '
            f"[files: {len(transfers)}/{pretty_bytes(transfer_size)}]"
        )

        # On success, decrement the size
        self._wait_size -= transfer_size

        # Create the file set for this job
        files = {item[1].id for item in transfers}

        # Add the items to the running list
        self._running |= files

        # Add to the job list
        self._jobs[job_name] = files


class HPSSOps:
    """HPSS Operations handler.

    Most of the actual HPSS work is done in the OpsQueue class, which
    we instantiate for each operation:
    * ingestq: the ingest (into HPSS) queue.  Implemented and working
    * recallq: the recall (out from HPSS) queue.  Partially implemented but disabled
    * checkq: the check/verify queue.  Unimplemented.

    Parameters
    ----------
    node : StorageNode
        The StorageNode.
    config : dict
        The I/O config from alpenhorn.
    """

    def _positive_default(self, name, default):
        """Get a value from the config which must be positive.

        If it's not present, or not positive, the default is returned.
        """
        val = int(config.get(name, default))
        if val < 1:
            log.warning(f"ignoring non-positive {name} for Node {self.name}")
            return default
        return val

    def __init__(self, node: StorageNode, config: dict) -> None:
        # Node info
        self.name = node.name
        self.node_id = node.id

        # Set-up from the config

        # Destination directory for recalls
        self.staging_root = pathlib.Path(config["staging_root"])

        # Talkback location and the location of the job script
        self.talkback_dir = self.staging_root.joinpath("talkback")

        # The rest of the config values are use to configure the OpsQueues
        # and don't need to be stored in this object instance

        # Recall and ingest max file count for a job.  There is a large
        # asymmetry here in the defaults: putting files into HPSS is _much_
        # more efficient than pulling them out.  (ingest_max is also used
        # for the check queue).
        recall_max = self._positive_default("recall_max", 40)
        ingest_max = self._positive_default("ingest_max", 500)

        # Config common to all ops queues
        common_queue_config = {
            # Name
            "name": node.name,
            # ID
            "node_id": node.id,
            # Bundle max: the maximum allowed total size for files in a single
            # HPSS job.  This is not different for in vs. out
            "bundle_max": self._positive_default("bundle_max", 800 * 1024**3),
            # Max wait time for any HPSS request
            "time_max": self._positive_default("time_max", 3600),
            # Paths
            "hpss_root": node.root,
            "staging_root": self.staging_root,
        }

        # The queues for ingest, check, and recall
        self.ingestq = OpsQueue(
            op="ingest", count_max=ingest_max, **common_queue_config
        )
        self.checkq = OpsQueue(op="check", count_max=ingest_max, **common_queue_config)
        self.recallq = OpsQueue(
            op="recall", count_max=recall_max, **common_queue_config
        )

    def check_jobs(self):
        """Update current slurm job status.

        This function just dispatches to the OpsQueues.
        """
        self.ingestq.check_jobs()
        self.checkq.check_jobs()
        self.recallq.check_jobs()

    def update(self):
        """Iterate over talkback files.

        Loops over files in the talkback directory for this
        node, and successively yields a 4-tuple for each (properly
        formatted) file found for this node.

        Yields
        ------
        int
            ArchiveFileCopyRequest.id for ingest.  0 for recall.
        int
            ArchiveFile.id
        int
            Seconds taken to perform the operation
        str
            Name of the operation ("ingest", "check", "recall")
        bool
            True if the operation succeded.  False otherwise.
        """
        # Loop over files in talkback dir
        count = 0
        count_in = 0
        count_out = 0
        for root, _, files in self.staging_root.joinpath("talkback").walk():
            for file in files:
                count += 1

                # Parse the talkback filename.  All talkback files have
                # eight dash-separated fields:
                #   <start>-<job#>-<node>-<deltat>-<file>-<req>-<opname>-<result>
                #
                # - opname is "ingest", "check" or "recall"
                # - result is "success" or "fail"
                # - req_id is 0 for recall requests (!=0 for ingest)
                bad_talkback = False
                data = file.split("-")
                try:
                    # Start time and job num (=data[0,1]) we ignore
                    node_id = int(data[2])
                    deltat = int(data[3])
                    file_id = int(data[4])
                    req_id = int(data[5])
                    opname = data[6]
                    result = data[7]
                except (TypeError, ValueError, IndexError) as e:
                    log.debug(f"Talback parse error: {e}")
                    bad_talkback = True

                if bad_talkback:
                    log.warning(f"deleting malformed talkback: {file}")
                elif node_id != self.node_id:
                    # Ignore talkback file for other nodes.  This is the
                    # only instance where we don't delete the talkback file
                    # after parsing it.
                    continue
                else:
                    if opname == "check":
                        # For check, just return everything back to the caller
                        yield req_id, file_id, deltat, opname, not (result != "success")
                    elif result != "success":
                        # For ingest and recall, on failure, log it
                        log.warning(f"Failed {opname} for file #{file_id}")
                        # Return this file_id to the caller
                        yield req_id, file_id, deltat, opname, False
                    elif opname == "ingest":
                        count_in += 1
                        # Return this file_id to the caller
                        yield req_id, file_id, deltat, opname, True
                    elif opname == "recall":
                        count_out += 1

                        # Who knows how old this talkback is?  Check the
                        # staging directory to see if the file actually exists
                        staged_path = self.staging_root.joinpath(str(file_id))

                        if staged_path.exists():
                            # Return this file_id to the caller
                            yield 0, file_id, deltat, opname, True
                        else:
                            # If the staged file doesn't exist, we essentially act as if
                            # this talkback didn't exist either, and do nothing (other than
                            # warn about it).
                            log.warning(
                                f"File #{file_id} missing from staging during talkback"
                            )
                    else:
                        log.warning(f"Ignoring talkback with unknown op ({opname})")

                # Whether good or bad, we remove the talkback file
                (root / file).unlink(missing_ok=True)

        if count:
            log.info(
                f"Processed {count} talkback: {count_in} archived; {count_out} staged"
            )

    def ingest(self, req: ArchiveFileCopyRequest | None) -> None:
        """Add a new ingest request.

        If req is not None, it is added to the ingest_waiting FIFO, if not
        already present.

        Then, whether a new request was added or not, create an ingest job,
        if there's a reason to.

        Parameters
        ----------
        req : ArchiveFileCopyRequest or None
            If not None, the request to add to the ingest list.  If None,
            nothing is added, and only the job-creation checks are performed.
        """
        # Hand this off to the ops queue
        self.ingestq.push(req=req)

    def check(self, file: ArchiveFile | None) -> None:
        """Add a new check request.

        If file is not None, it is added to the recall waiting FIFO, if not
        already present.

        Then, whether a new request was added or not, create a check job,
        if there's a reason to.

        Parameters
        ----------
        file : ArchiveFile or None
            If not None, the file to add to the check list.  If None,
            nothing is added, and only the job-creation checks are performed.
        """
        # Hand this off to the ops queue
        self.checkq.push(file=file)

    def recall(self, file: ArchiveFile | None) -> None:
        """Add a new recall request.

        If file is not None, it is added to the recall waiting FIFO, if not
        already present.

        Then, whether a new request was added or not, create a recall job,
        if there's a reason to.

        Parameters
        ----------
        file : ArchiveFile or None
            If not None, the file to add to the recall list.  If None,
            nothing is added, and only the job-creation checks are performed.
        """
        # Hand this off to the ops queue
        self.recallq.push(file=file)


# JOB SCRIPT FRAGMENTS
#
# The rest of this file are the job script fragments
#

# This is the header for all scripts
#
# Vars:
# - job_name     : the name of this job
# - hpss_root    : root path in HPSS
# - talkback_dir : where to write talkback files
# - extern_dir   : source dir for ingest.  staging dir for recall.
# - node_id      : HPSS StorageNode.id
#
_HEADER = """#!/bin/bash
#SBATCH -t 4:00:00
#SBATCH -p archivelong
#SBATCH -J {job_name}
#SBATCH -N 1

START_TIME=$(date +%s)      # Seconds since the epoch
JOBNUM=$SLURM_JOB_ID

# Paths
HPSS_DIR={hpss_root}
TALKBACK_DIR={talkback_dir}
STAGING_DIR={staging_root}   # Ignored for ingest
NODE_ID={node_id}

"""

# The looping section for file checking
#
# Vars:
#   acq_dir   : ArchiveFile.acq.name
#   file_id   : ArchiveFile.id
#   file_name : ArchiveFile.name
#   md5sum    : ArchiveFile.md5sum
_CHECK_LOOP = """

#
#
# Check of {acq_dir}/{file_name}:

ITEM_START=$(date +%s)
echo
echo "Start of check of {acq_dir}/{file_name} with hash {md5sum}"

# Check for an existing file by trying to hash it.  hsi prints the
# hash as the first word to stderr.
HPSS_HASH=$(hsi -q lshash $HPSS_DIR/{acq_dir}/{file_name} 2>&1 | awk '{{print $1}}')

# If this worked, and the hash is fine, the check succeeded
if [ "x$HPSS_HASH" == 'x{md5sum}' ]; then
    echo "  Hash of stored file correct"
    RESULT="success"
else
    # Otherwise, the check failed
    echo "  Hash of existing copy (if any) incorrect"
    RESULT="fail"
fi

# Create the talkback
DELTAT=$(expr $(date +%s) - $ITEM_START)
TALKBACK=$START_TIME-$JOBNUM-$NODE_ID-$DELTAT-{file_id}-{req_id}-check-$RESULT
touch $TALKBACK_DIR/$TALKBACK
echo "  Wrote talkback: $TALKBACK"
"""

# The looping section for ingest (into HPSS)
#
# Vars:
#   ext_path  : Full path to source file
#   req_id    : ArchiveFileCopyRequest.id
#   acq_dir   : ArchiveFile.acq.name
#   file_id   : ArchiveFile.id
#   file_name : ArchiveFile.name
#   md5sum    : ArchiveFile.md5sum
_INGEST_LOOP = """

#
#
# Ingest of {acq_dir}/{file_name}:

ITEM_START=$(date +%s)
echo
echo "Start of ingest of {acq_dir}/{file_name} with hash {md5sum}"

# Check for an existing file by trying to hash it.  hsi prints the
# hash as the first word to stderr.
HPSS_HASH=$(hsi -q lshash $HPSS_DIR/{acq_dir}/{file_name} 2>&1 | awk '{{print $1}}')

# If this worked, and the hash is fine, there's nothing to do
if [ "x$HPSS_HASH" == 'x{md5sum}' ]; then
    echo "  File already exists during ingest"
    RESULT="success"
else
    echo "  Hash of existing copy (if any) incorrect"

    # Ensure the acquisition directory exists.  This always succeeds
    hsi -q mkdir $HPSS_DIR/{acq_dir}

    # Copy the file into HPSS
    hsi -q put -c on -H md5 {ext_path} : $HPSS_DIR/{acq_dir}/{file_name}

    # Check the hash of the new file
    HPSS_HASH=$(hsi -q lshash $HPSS_DIR/{acq_dir}/{file_name} 2>&1  | awk '{{print $1}}')

    if [ "x$HPSS_HASH" == 'x{md5sum}' ]; then
        echo "  Successful write into HPSS"
        RESULT="success"
    else
        echo "  Failed write into HPSS"
        RESULT="fail"
    fi
fi

# Create the talkback
DELTAT=$(expr $(date +%s) - $ITEM_START)
TALKBACK=$START_TIME-$JOBNUM-$NODE_ID-$DELTAT-{file_id}-{req_id}-ingest-$RESULT
touch $TALKBACK_DIR/$TALKBACK
echo "  Wrote talkback: $TALKBACK"
"""
