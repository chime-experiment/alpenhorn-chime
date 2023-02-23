"""Common fixtures."""

import pytest
import peewee as pw
import chimedb.core as db
from alpenhorn import db as adb
from alpenhorn.acquisition import (
    ArchiveAcq,
    ArchiveFile,
    AcqType,
    FileType,
    AcqFileTypes,
)
from alpenhorn.archive import ArchiveFileCopy, ArchiveFileCopyRequest
from alpenhorn.storage import StorageNode, StorageGroup

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
def ExtendedArchiveAcq(proxy):
    # Extended ArchiveAcq
    class ExtendedArchiveAcq(ArchiveAcq):
        inst = pw.ForeignKeyField(ArchiveInst, backref="acqs", null=True)

        class Meta:
            table_name = ArchiveAcq._meta.table_name

    return ExtendedArchiveAcq


@pytest.fixture
def tables(proxy, ExtendedArchiveAcq):
    """Ensure all the tables are created."""

    proxy.create_tables(
        [
            AcqFileTypes,
            AcqType,
            ArchiveFile,
            ArchiveFileCopy,
            ArchiveFileCopyRequest,
            ArchiveInst,
            CalibrationGainFileInfo,
            CorrAcqInfo,
            CorrFileInfo,
            DigitalGainFileInfo,
            ExtendedArchiveAcq,
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
