"""Test everything by running alpenhorn import on the test data."""

import pytest

import shutil
import pathlib
import tempfile
import subprocess
from time import sleep
from signal import SIGINT
from unittest.mock import patch

from chimedb.data_index import util

from alpenhorn.db import ArchiveFileCopy, StorageGroup, StorageNode

from chimedb.data_index.orm import (
    CorrAcqInfo,
    ArchiveAcq,
    ArchiveFile,
    ArchiveInst,
    CorrFileInfo,
    RawadcAcqInfo,
    RawadcFileInfo,
    WeatherFileInfo,
    DigitalGainFileInfo,
    CalibrationGainFileInfo,
    FlagInputFileInfo,
    AcqType,
    FileType,
)


@pytest.fixture
def testdata():
    """Yields the path to the testdata directory."""

    return str(pathlib.Path(__file__).with_name("testdata"))


@pytest.fixture
def tempdb():
    """Create the test database.

    Yields the path to the database.

    The database is deleted after the test completes."""

    with tempfile.NamedTemporaryFile(suffix=".sql") as sqldb:
        yield sqldb.name


@pytest.fixture
def chime_data(tables):
    """Ensure the CHIME data has been added to the database."""

    util.update_types()
    util.update_inst()


@pytest.fixture
def test_data(proxy, tables, testdata):
    """Set db data for the test."""

    # Storage group and node
    group = StorageGroup.create(name="group")
    StorageNode.create(
        name="node",
        group=group,
        root=testdata,
        host="alpenhost",
        active=True,
        auto_import=True,
        storage_type="A",
    )


@pytest.fixture
def set_env(testdata, tempdb):
    """Set up the environment for the test."""

    # We set both CHIMEDB vars, because while the test itself runs in test-safe mode,
    # alpenhorn itself won't be doing that.
    with patch.dict(
        "os.environ",
        ALPENHORN_CONFIG_FILE=str(pathlib.Path(testdata, "alpenhorn.yaml")),
        CHIMEDB_SQLITE=tempdb,
        CHIMEDB_TEST_SQLITE=tempdb,
    ):
        yield


def test_import(tempdb, set_env, tables, chime_data, test_data):
    """Test import of CHIME files."""

    alpenhornd = shutil.which("alpenhornd")
    assert alpenhornd is not None

    # Start alpenhorn daemon in a subprocess
    alp = subprocess.Popen([alpenhornd, "--exit-after-update", "--test-isolation"])

    # Wait for termination
    alp.wait()

    # Check
    chime_inst = ArchiveInst.get(name="chime")
    chimetiming_inst = ArchiveInst.get(name="chimetiming")
    ids = dict()

    acq_data = {
        "20201101T000000Z_chime_gain": (chime_inst, AcqType.get(name="gain")),
        "20210321T121000Z_chime_digitalgain": (
            chime_inst,
            AcqType.get(name="digitalgain"),
        ),
        "20230213T201433Z_chime_rawadc": (chime_inst, AcqType.get(name="rawadc")),
        "20220101T000000Z_chime_flaginput": (chime_inst, AcqType.get(name="flaginput")),
        "20221101T000000Z_chime_weather": (chime_inst, AcqType.get(name="weather")),
        "20220129T233553Z_chimetiming_corr": (
            chimetiming_inst,
            AcqType.get(name="corr"),
        ),
    }
    for acq in ArchiveAcq.select():
        assert acq.inst == acq_data[acq.name][0]
        assert acq.type == acq_data[acq.name][1]

        # Remember for later
        ids[acq.name] = acq.id

    file_data = {
        "000003.h5": ("20230213T201433Z_chime_rawadc", FileType.get(name="rawadc")),
        "00947042.h5": (
            "20220101T000000Z_chime_flaginput",
            FileType.get(name="calibration"),
        ),
        "20221106.h5": ("20221101T000000Z_chime_weather", FileType.get(name="weather")),
        "00358972.h5": (
            "20201101T000000Z_chime_gain",
            FileType.get(name="calibration"),
        ),
        "00001101.h5": (
            "20210321T121000Z_chime_digitalgain",
            FileType.get(name="calibration"),
        ),
        "00000000_0000.h5": (
            "20220129T233553Z_chimetiming_corr",
            FileType.get(name="corr"),
        ),
    }

    for file in ArchiveFile.select():
        assert file.acq_id == ids[file_data[file.name][0]]
        assert file.type == file_data[file.name][1]

        # Remember for later
        ids[file.name] = file.id

    # Now check all the info tables.
    assert DigitalGainFileInfo.select().count() == 1
    for info in DigitalGainFileInfo.select():
        assert info.file.id == ids["00001101.h5"]
        assert info.start_time == 1616329701.187867
        assert info.finish_time == 1616329701.187867

    assert CalibrationGainFileInfo.select().count() == 1
    for info in CalibrationGainFileInfo.select():
        assert info.file.id == ids["00358972.h5"]
        assert info.start_time == 1604547772.630382
        assert info.finish_time == 1604547796.087183

    assert FlagInputFileInfo.select().count() == 1
    for info in FlagInputFileInfo.select():
        assert info.file.id == ids["00947042.h5"]
        assert info.start_time == 1641942242.17755
        assert info.finish_time == 1641945009.510767

    assert WeatherFileInfo.select().count() == 1
    for info in WeatherFileInfo.select():
        assert info.file.id == ids["20221106.h5"]
        assert info.date == "20221106"
        assert info.start_time == 1667692800.0
        assert info.finish_time == 1667779199.0

    assert CorrFileInfo.select().count() == 1
    for info in CorrFileInfo.select():
        assert info.file.id == ids["00000000_0000.h5"]
        assert info.chunk_number == 0
        assert info.freq_number == 0
        assert info.start_time == 1643499353.1176474
        assert info.finish_time == 1643499353.1176474

    assert RawadcFileInfo.select().count() == 1
    for info in RawadcFileInfo.select():
        assert info.file.id == ids["000003.h5"]
        assert info.start_time == 1676325138.068710
        assert info.finish_time == 1676325566.065336

    assert CorrAcqInfo.select().count() == 1
    for info in CorrAcqInfo.select():
        assert info.acq.id == ids["20220129T233553Z_chimetiming_corr"]
        assert info.nfreq == 1024
        assert info.nprod == 120
        assert info.integration is None

    assert RawadcAcqInfo.select().count() == 1
    for info in RawadcAcqInfo.select():
        assert info.acq.id == ids["20230213T201433Z_chime_rawadc"]
        assert info.start_time == 1676319273.0
