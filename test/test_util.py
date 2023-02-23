"""Test util."""

from alpenhorn_chime import util
from alpenhorn.acquisition import AcqType, FileType, AcqFileTypes


def test_update_types(tables):
    """Test update_types()."""

    # Create the type data
    util.update_types()

    # Check
    type_data = {
        "corr": {"acq_class": "CorrAcqInfo", "file_class": "CorrFileInfo"},
        "hfb": {"acq_class": "HFBAcqInfo", "file_class": "HFBFileInfo"},
        "weather": {"acq_class": "WeatherAcqDetect", "file_class": "WeatherFileInfo"},
        "rawadc": {"acq_class": "RawadcAcqInfo", "file_class": "RawadcFileInfo"},
    }

    for name, data in type_data.items():
        at = AcqType.get(name=name)
        assert at.info_class == "alpenhorn_chime.info." + data["acq_class"]

        ft = FileType.get(name=name)
        assert ft.info_class == "alpenhorn_chime.info." + data["file_class"]

        fts = list(AcqFileTypes.select().where(AcqFileTypes.acq_type == at))
        assert len(fts) == 1
        assert fts[0].file_type == ft

    # The calibration types are different
    ft = FileType.get(name="calibration")
    assert ft.info_class == "=alpenhorn_chime.info.cal_info_class"

    for name, acqclass in [
        ("digitalgain", "DigitalGainAcqDetect"),
        ("gain", "CalibrationGainAcqDetect"),
        ("flaginput", "FlagInputAcqDetect"),
    ]:
        at = AcqType.get(name=name)
        assert at.info_class == "alpenhorn_chime.info." + acqclass

        fts = list(AcqFileTypes.select().where(AcqFileTypes.acq_type == at))
        assert len(fts) == 1
        assert fts[0].file_type == ft
