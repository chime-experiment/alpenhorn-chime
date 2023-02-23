"""Alpenhorn extensions for CHIME."""

import peewee as pw

from .inst import set_inst, ArchiveInst


def register_extension() -> dict:
    """Return alpenhorn extension data.

    This module extends ArchiveAcq to add the `inst` field.
    """
    return {
        "model": [
            {
                "table": "ArchiveAcq",
                "setter": set_inst,
                "fields": {
                    "inst": pw.ForeignKeyField(ArchiveInst, backref="acqs", null=True)
                },
            }
        ]
    }
