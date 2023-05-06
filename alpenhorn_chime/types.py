"""Type tables for CHIME.

This re-implements AcqType and FileType from alpenhorn-1.  It also
adds a new junction table, AcqFileTypes to associate AcqTypes and
FileTypes.

The goal here is to move as much of the type-specific configuration
that litters chimedb.data_index into the data tables so that new
types can be added simply via DB update instead of having to
modify code in multiple different repositories, as is needed with
alpenhorn-1.
"""

import peewee as pw

from alpenhorn.db import base_model


class AcqType(base_model):
    """Acquistion type.

    Attributes
    ----------
    name : string
        Name of the type.  This appears as the last element of
        an acqusition name.
    info_class : string or None
        Name of the associated Info class under `alpenhorn_chime.info`.
    notes : string or None
        A human-readable description.
    """

    name = pw.CharField(max_length=64, unique=True)
    info_class = pw.CharField(max_length=64, null=True)
    notes = pw.TextField(null=True)

    @property
    def file_types(self) -> pw.ModelSelect:
        """An iterator over the FileTypes supported by this AcqType."""
        return FileType.select().join(AcqFileTypes).where(AcqFileTypes.acq_type == self)


class FileType(base_model):
    """File type.

    Attributes
    ----------
    name : string
        The name of this file type.
    info_class : string or None
        If not None, the name of the associated Info class, or an
        Info-class-providing function, located under `alpenhorn_chime.info`.
    pattern : string or None
        If not None, a regular expression to match against the filename.  This
        is the primary method of determining the type of a file.  If this is
        None, no matching is done, and alpenhorn will be unable to import a file
        of this type.
    notes : string or None
        Any notes or comments about this file type.
    """

    name = pw.CharField(max_length=64, unique=True)
    info_class = pw.CharField(max_length=64, null=True)
    pattern = pw.CharField(max_length=64, null=True)
    notes = pw.TextField(null=True)


class AcqFileTypes(base_model):
    """FileTypes supported by an AcqType.

    A junction table providing the many-to-many relationship
    indicating which FileTypes are supported by which AcqTypes.

    Attributes
    ----------
    acq_type : foreign key to AcqType
    file_type : foreign key to FileType

    Notes
    -----
    As this is a junction table, there is no id column.  The
    tuple (acq_type, file_type) itself is the primary key.
    """

    acq_type = pw.ForeignKeyField(AcqType, backref="acq_types")
    file_type = pw.ForeignKeyField(FileType, backref="file_types")

    class Meta:
        primary_key = pw.CompositeKey("acq_type", "file_type")
