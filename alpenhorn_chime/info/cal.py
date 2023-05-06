"""Calibration broker info tables.

i.e gain, digitalgain, flaginput
"""
from __future__ import annotations
from typing import BinaryIO

import h5py
import peewee as pw

from .base import CHIMEFileInfo
from ..detection import ArchiveAcq, ArchiveFile


class CalibrationFileInfo(CHIMEFileInfo):
    """Base class for all calibration data types.

    Attributes
    ----------
    file : foreign key
        Reference to the file this information is about.
    start_time : float
        Start of data in the file in UNIX time.
    finish_time : float
        End of data in the file in UNIX time.
    """

    start_time = pw.DoubleField(null=True)
    finish_time = pw.DoubleField(null=True)

    def _info_from_file(self, file: BinaryIO) -> dict:
        """Get cal file info.

        Parameters
        ----------
        file : open, read-only file
            the file being imported.
        """
        with h5py.File(file, "r") as f:
            start_time = f["index_map/update_time"][0]
            finish_time = f["index_map/update_time"][-1]

        return {
            "start_time": start_time,
            "finish_time": finish_time,
        }


class DigitalGainFileInfo(CalibrationFileInfo):
    """Digital gain data file info."""


class CalibrationGainFileInfo(CalibrationFileInfo):
    """Gain data file info."""


class FlagInputFileInfo(CalibrationFileInfo):
    """Flag input file info."""


# This is the indirect file class function for "calibration" files
def cal_info_class(file: ArchiveFile) -> type[CalibrationFileInfo]:
    """Return calibration file info class for `file`.

    Calibration info classes depend on AcqType.  They probably
    shouldn't and there should probably just be a single one,
    because they're all the same anyways.

    Parameters
    ----------
    file : ArchiveFile
        the archive file object for which we're generating info
    """

    acqtype_name = file.acq.type.name

    if acqtype_name == "digitalgain":
        return DigitalGainFileInfo
    if acqtype_name == "gain":
        return CalibrationGainFileInfo
    if acqtype_name == "flaginput":
        return FlagInputFileInfo
    raise ValueError(f'unknown acqtype: {acqtype_name}"')
