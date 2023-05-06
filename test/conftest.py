"""Common fixtures."""

import pytest
import chimedb.core as db
from alpenhorn import db as adb
from alpenhorn.archive import ArchiveFileCopy, ArchiveFileCopyRequest
from alpenhorn.storage import StorageNode, StorageGroup

from alpenhorn_chime.detection import ArchiveAcq, ArchiveFile
from alpenhorn_chime.inst import ArchiveInst
from alpenhorn_chime.info import (
    CorrAcqInfo,
    CorrFileInfo,
    HFBAcqInfo,
    HFBFileInfo,
    RawadcAcqInfo,
    RawadcFileInfo,
    WeatherFileInfo,
)
from alpenhorn_chime.info.cal import (
    DigitalGainFileInfo,
    CalibrationGainFileInfo,
    FlagInputFileInfo,
)
from alpenhorn_chime.types import AcqType, FileType, AcqFileTypes


@pytest.fixture
def proxy():
    """Open a connection to the database.

    Returns the database proxy.
    """
    db.test_enable()
    db.connect(read_write=True)
    adb.database_proxy.initialize(db.proxy.obj)
    adb.EnumField.native = False
    yield db.proxy

    db.close()


@pytest.fixture
def tables(proxy):
    """Ensure all the tables are created."""

    proxy.create_tables(
        [
            AcqFileTypes,
            AcqType,
            ArchiveAcq,
            ArchiveFile,
            ArchiveFileCopy,
            ArchiveFileCopyRequest,
            ArchiveInst,
            CalibrationGainFileInfo,
            CorrAcqInfo,
            CorrFileInfo,
            DigitalGainFileInfo,
            FileType,
            FlagInputFileInfo,
            HFBAcqInfo,
            HFBFileInfo,
            RawadcAcqInfo,
            RawadcFileInfo,
            StorageGroup,
            StorageNode,
            WeatherFileInfo,
        ]
    )
