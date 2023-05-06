"""CHIME Info base classes."""
from __future__ import annotations
from typing import TYPE_CHECKING

import peewee as pw
from alpenhorn.db import base_model

from ..detection import ArchiveAcq, ArchiveFile

if TYPE_CHECKING:
    import pathlib
    from alpenhorn.update import UpdateableNode


class InfoBase(base_model):
    """Abstract base class for CHIME Info tables.

    The keyword parameters "node_" and "path_", if present, are passed,
    along with the ArchiveAcq or ArchiveFile, to the method `_set_info`
    which must be re-implemented by subclasses.

    The dict returned from this `_set_info` call is merged into the list of
    keyword parameters (after removing the keywords listed above).  These
    merged keyword parameters is passed to the `peewee.Model` initialiser.

    This _set_info() call is only performed if the "node_" and
    "path_" keywords are provided.  The trailing underscore on these
    keywords prevents the potential for clashes with field names of
    the underlying table.
    """

    # Should be set to True for Acq Info classes
    is_acq = False

    def __init__(self, *args, **kwargs) -> None:
        """initialise an instance.

        Parameters
        ----------
        node_ : alpenhorn.update.UpdateableNode or None
            The node on which the imported file is
        path_ : pathlib.Path or None
            Path to the imported file.  Relative to `node.root`.

        Raises
        ------
        ValueError
            the keyword "path_" was given, but "node_" was not.
        """

        # Remove keywords we consume
        path = kwargs.pop("path_", None)
        node = kwargs.pop("node_", None)
        name_data = kwargs.pop("name_data_", tuple())

        # If we were given an "item_", convert it to an acq or file as apporpriate
        item = kwargs.pop("item_", None)
        if item is not None:
            if self.is_acq:
                kwargs["acq"] = item
            else:
                kwargs["file"] = item

        # Call _set_info, if necessary
        if path is not None:
            if node is None:
                raise ValueError("no node_ specified with path_")

            # Info returned is merged into kwargs so the peewee model can
            # ingest it.
            kwargs |= self._set_info(path=path, node=node, name_data=name_data)

        # Continue init
        super().__init__(*args, **kwargs)

    def _set_info(
        self,
        path: pathlib.Path,
        node: UpdateableNode,
        re_groups: tuple,
    ) -> dict:
        """Generate info table field data for this path.

        Calls to this method occur as part of object initialisation during
        an init() call.

        Subclasses must re-implement this method to generate metadata by
        inspecting the acqusition or file on disk given by `path`.  To
        access a file on the node, don't open() the path directly; use
        `node.io.open(path)`.

        Parameters
        ----------
        path : pathlib.Path
            path to the file being imported.  Note this is _always_
            the _file_ path, even for acqusitions.  Relative to
            `node.db.root`.
        node : alpenhorn.update.UpdateableNode
            node where the file has been imported
        re_groups : tuple
            For File Info classes, this is a possibly-empty tuple containing
            group matches from the pattern match that was performed on the
            file name.  For Acq Info classes, this is always an empty tuple.

        Returns
        -------
        info : dict
            table data to be passed on to the peewee `Model` initialiser.

        On error, implementations should raise an appropriate exception.
        """
        raise NotImplementedError("must be re-implemented by subclass")


class CHIMEAcqInfo(InfoBase):
    """Abstract base class for CHIME Acq Info tables.

    Add acq info base column `acq` and provides an implementation
    of `_set_info` for convenience which converts the call into a
    call to `_info_from_file`.

    Subclasses should add additional fields.  They should also
    either reimplement `_set_info` or else implement the method
    `_info_from_file` which will be passed an open, read-only
    file object.

    Attributes
    ----------
    acq : foreign key to ArchiveAcq (or ArchiveAcq)
        the corresponding acquisition record
    """

    is_acq = True

    acq = pw.ForeignKeyField(ArchiveAcq)

    def _set_info(
        self,
        path: pathlib.Path,
        node: UpdateableNode,
        name_data: dict,
    ) -> dict:
        """Set acq info from file `path` on node `node`.

        Calls `_info_from_file` to generate info data.

        Parameters
        ----------
        node : StorageNode
            the node we're importing on
        path : pathlib.Path
            the path relative to `node.root` of the file
            being imported in this acquistion
        name_data : dict
            ignored

        Returns
        -------
        info : dict
            any data returned by `_info_from_file`, if that
            method exists, or else an empty dict.

        Notes
        -----
        For acqusitions, the only key in `name_data` is "acqtime" which
        contains a `datetime` with the value of the acqusition timestamp.
        If that value is important, subclasses must re-implement this
        method to capture it.
        """
        if hasattr(self, "_info_from_file"):
            # Open the file
            with node.io.open(path) as file:
                # Get keywords from file
                return self._info_from_file(file)
        else:
            return dict()


class CHIMEFileInfo(InfoBase):
    """Abstract base info class for a CHIME file

    Subclasses must re-implement `_parse_filename` to implement
    type detection, and optionally data gathering.

    Subclasses should add additional fields.  They should also
    either reimplement `_set_info` or else implement the method
    `_info_from_file` which will be passed an open, read-only
    file object.
    """

    file = pw.ForeignKeyField(ArchiveFile)

    group_keys = tuple()

    def _set_info(
        self,
        path: pathlib.Path,
        node: UpdateableNode,
        name_data: dict,
    ) -> dict:
        """Set file info from file `path` on node `node`.

        If defined in the class, calls `_info_from_file` to
        populate the info record.

        Additional fields provided in `name_data` are also merged
        into the returned dict.

        Parameters
        ----------
        node : UpdateableNode
            the node we're importing on
        path : pathlib.Path
            the path relative to `node.root` of the file being imported
        name_data : dict
            other dict entries merged into the returned dict.  May be empty.

        Returns
        -------
        info : dict
            A dict containing data from `_info_from_file` and/or
            `name_data`.
        """
        if hasattr(self, "_info_from_file"):
            # Open the file
            with node.io.open(path) as file:
                # Get keywords from file
                info = self._info_from_file(file)
        else:
            info = dict()

        return info | name_data
