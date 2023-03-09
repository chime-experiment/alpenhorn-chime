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


def set_inst(acqname: str, **kwargs) -> dict:
    """Return extended ArchiveAcq data.

    Provides the value of `inst`.  This is the setter
    for the alpenhorn-chime model extension.

    Parameters
    ----------
    acqname : str
        the name of the acquisition
    kwargs : dict
        other, unused data from alpenhorn

    Returns
    -------
    ext_data : dict
        A dict with a single "inst" key providing the
    """

    # This is only called after a successful type detection,
    # so we know that the acqname has the right form and
    # we can just extract the middle bit.
    #
    # If acqname is "abc_def_ghi", this will be "def"
    _, instname, _ = acqname.split("_")

    return {"inst": ArchiveInst.get(name=instname)}
