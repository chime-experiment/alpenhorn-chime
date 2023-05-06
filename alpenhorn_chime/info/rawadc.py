"""CHIME rawadc info tables."""
from __future__ import annotations
from typing import TYPE_CHECKING, BinaryIO

import h5py
import calendar
import peewee as pw

from .base import CHIMEAcqInfo, CHIMEFileInfo

if TYPE_CHECKING:
    import pathlib
    from alpenhorn.update import UpdateableNode


class RawadcAcqInfo(CHIMEAcqInfo):
    """Information about a rawadc acquisition.

    Attributes
    ----------
    acq : foreign key to ArchiveAcq
        The acquisition that the information is for.
    inst : foreign key to ArchiveInst
        The instrument that took the acquisition.
    start_time : double
        When the raw ADC acquisition was performed, in UNIX time.
    """

    start_time = pw.DoubleField(null=True)

    def _set_info(
        self,
        path: pathlib.Path,
        node: UpdateableNode,
        name_data: dict,
    ) -> dict:
        """Generate acq info.

        Parameters
        ----------
        node : StorageNode
            the node we're importing on
        path : pathlib.Path
            the path relative to `node.root` of the file
            being imported in this acquistion
        name_data : dict
            the value associated with key "acqtime" is used for "start_time"
        """

        info = super()._set_info(path=path, node=node, name_data=name_data)
        info["start_time"] = calendar.timegm(name_data["acqtime"].utctimetuple())
        return info


class RawadcFileInfo(CHIMEFileInfo):
    """Information about a rawadc data file.

    Attributes
    ----------
    file : foreign key to ArchiveFile
        The file this information is about.
    start_time : float
        Start of acquisition in UNIX time.
    finish_time : float
        End of acquisition in UNIX time.
    """

    start_time = pw.DoubleField(null=True)
    finish_time = pw.DoubleField(null=True)

    def _info_from_file(self, file: BinaryIO) -> dict:
        """Get rawadc file info.

        Parameters
        ----------
        file : open, read-only file
            the file being imported.
        """
        with h5py.File(file, "r") as f:
            times = f["timestamp"]["ctime"]
            start_time = times.min()
            finish_time = times.max()

        return {"start_time": start_time, "finish_time": finish_time}
