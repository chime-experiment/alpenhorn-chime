"""Calibration broker info tables.

i.e gain, digitalgain, flaginput
"""

import re
import h5py
import peewee as pw

from .base import CHIMEAcqDetect, CHIMEFileInfo


# No-table acq info
class DigitalGainAcqDetect(CHIMEAcqDetect):
    pass


class CalibrationGainAcqDetect(CHIMEAcqDetect):
    pass


class FlagInputAcqDetect(CHIMEAcqDetect):
    pass


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

    @classmethod
    def _parse_filename(cls, name: str) -> None:
        """Check if cal file has the right form.

        Parameters
        ----------
        name : str
            Filename.

        Raises
        ------
        ValueError
            `name` didn't have the right form
        """
        if not re.match(r"[0-9]{8}\.h5", name):
            raise ValueError(f"bad cal data file: {name}")

    def _info_from_file(self, file):
        """Get cal file info.

        Parameters
        ----------
        file : open, read-only file
            the file being imported.
        """
        f = h5py.File(file, "r")
        start_time = f["index_map/update_time"][0]
        finish_time = f["index_map/update_time"][-1]
        f.close()

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
def cal_info_class(acqtype):
    """Return calibration file info class for `acqtype`."""

    if acqtype.name == "digitalgain":
        return DigitalGainFileInfo
    if acqtype.name == "gain":
        return CalibrationGainFileInfo
    if acqtype.name == "flaginput":
        return FlagInputFileInfo
    raise ValueError(f'unknown acqtype: {acqtype.name}"')
