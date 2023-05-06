"""The ArchiveInst table.

Alpenhorn-2 does not support ArchiveInst, so we re-implement it here for CHIME.
"""

import peewee as pw
from alpenhorn.db import base_model


class ArchiveInst(base_model):
    """Instrument that took the data.

    Attributes
    ----------
    name : string
        Name of instrument.
    notes : string, optional
        Always NULL.
    """

    name = pw.CharField(max_length=64, unique=True)
    notes = pw.TextField(null=True)
