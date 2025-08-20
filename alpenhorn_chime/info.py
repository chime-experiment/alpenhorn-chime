"""CHIME info classes."""

from chimedb.data_index.orm import (
    ArchiveFile,
    CalibrationFileInfo,
    CalibrationGainFileInfo,
    CorrAcqInfo,
    CorrFileInfo,
    DigitalGainFileInfo,
    FlagInputFileInfo,
    HFBAcqInfo,
    HFBFileInfo,
    RawadcAcqInfo,
    RawadcFileInfo,
    TimingCorrectionFileInfo,
    WeatherFileInfo,
)


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
    if acqtype_name == "timing":
        return TimingCorrectionFileInfo
    raise ValueError(f'unknown acqtype: {acqtype_name}"')
