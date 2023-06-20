"""Reserving alpenhorn I/O module.

Provides the ReservingNodeIO class needed by
alpenhorn StorageNodes with io_class=="Reserved".
"""
from __future__ import annotations
from typing import TYPE_CHECKING

import logging
from datetime import datetime

from alpenhorn.io.lfsquota import LFSQuotaNodeIO
from chimedb.cfm.tags import FileReservation

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from alpenhorn.archive import ArchiveFileCopy, ArchiveFileCopyRequest


class ReservingNodeIO(LFSQuotaNodeIO):
    """Node I/O class for Reserving nodes.

    This behaves like a LFSQuota node (which itself is almost the same as
    a Default node) except:
      * pulls to the node fail if there isn't a reservation record for the
        ArchiveFile already in the FileReservation table.
      * deletion from the node fails if there are any reservation records
        for the ArchiveFile in the FileReservation table.
    """

    def delete(self, copies: list[ArchiveFileCopy]) -> None:
        """Try to delete the list of file `copies`.

        Checks whether the files in the list have reservations.
        The ones that do are discarded from the list and the remainder
        are passed on to the parent's `delete` method.

        Parameters
        ----------
        copies : list of ArchiveFileCopy
            The list of file copies to delete
        """

        new_list = list()
        for copy in copies:
            tags = FileReservation.reserved_in_node(file=copy.file, node=self.node)
            if not tags:
                # No reservation, so this file is okay
                new_list.append(copy)
            else:
                log.warning(
                    f"Cancelling deletion of {copy.file.path}: "
                    f"file reserved by: {tags[0].name}"
                )

                # Cancel deletion
                copy.wants_file = "Y"
                copy.last_update = datetime.now()
                copy.save()

        # Pass the new list (which might now be empty) on to the next thing in the MRO
        super().delete(new_list)

    def pull(self, req: ArchiveFileCopyRequest) -> None:
        """Pull file specified by copy request `req` onto `self.node`.

        If there is no reservation record for the ArchiveFile, this fails.

        Parameters
        ----------
        req : ArchiveFileCopyRequest
            the copy request to fulfill.  We are the destination node (i.e.
            `req.group_to == self.node.group`).
        """

        if not FileReservation.reserved_in_node(file=req.file, node=self.node):
            log.warning(
                f"Cancelling pull of {req.file.path} onto {self.node.name}: "
                "not reserved."
            )
            req.cancelled = 1
            req.save()
            return

        # Otherwise, proceed
        super().pull(req)
