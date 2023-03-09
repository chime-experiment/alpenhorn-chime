"""Alpenhorn2-style CHIME Info base classes."""
from __future__ import annotations
from typing import TYPE_CHECKING

import calendar
import datetime
import peewee as pw
from alpenhorn.info_base import info_base, acq_info_base, file_info_base

from ..inst import ArchiveInst

if TYPE_CHECKING:
    import pathlib
    from alpenhorn.acquisition import ArchiveAcq, AcqType, ArchiveFile
    from alpenhorn.storage import StorageNode


class CHIMEAcqDetect(info_base):
    """Abstract base info class for a CHIME acqusition

    This handles parsing the standard CHIME acquisiton name
    format and determining whether or not this is a CHIME acq.

    This does not implement a table.  It can be used for any
    CHIME AcqType that doesn't have an Info table and only needs
    to do type detection.

    CHIME AcqTypes which _do_ have an Info table should subclass from
    from `CHIMEAcqInfo` and not this class.
    """

    @classmethod
    def is_type(cls, path: pathlib.Path, node: StorageNode) -> bool:
        """Is this a CHIME acquisition?

        Parameters
        ----------
        path : pathlib.Path
            the acqusition path to check
        node : StorageNode
            usused

        Returns
        -------
        is_type : bool
            True if `path` is a an acqusition of the class type.
        """
        # A standard CHIME acqusition name has the form:
        #
        #   <ISO-8601-datetime>_<inst-name>_<type-name>
        #
        # where the datetime has the form: YYYMMDDTHHMMSSZ

        # Split the path on underscores
        parts = str(path).split("_")

        # If we don't have three parts, the path is invalid
        if len(parts) != 3:
            return False

        # Check that the final part is the same as the type name
        if parts[2] != cls.type().name:
            return False

        # Check that the second part is a known ArchiveInst
        try:
            ArchiveInst.get(name=parts[1])
        except pw.DoesNotExist:
            return False

        # Check that the first part is a valid date
        try:
            cls.timestamp_from_name(parts[0])
        except ValueError:
            return False

        # Otherwise type detection succeeds
        return True

    @classmethod
    def timestamp_from_name(cls, path: pathlib.Path | str) -> int:
        """Return epoch timestamp from acq path `path`

        Parameters
        ----------
        path : path.Pathlib or str
            The name of the acqusition

        Returns
        -------
        timestamp : int
            The seconds-since-epoch of the acq time
            encoded in the acq path.

        Raises
        ------
        ValueError
            path was not a valid date
        """
        d = datetime.datetime.strptime(str(path)[0:16], "%Y%m%dT%H%M%SZ")
        return calendar.timegm(d.utctimetuple())


class CHIMEAcqInfo(CHIMEAcqDetect, acq_info_base):
    """Abstract base class for CHIME Acq Info tables.

    In addition to the type detection provided by CHIMEAcqDetect,
    this adds the standard Alpenhorn AcqInfo base table.

    Provides an implementation of `_set_info` for convenience which
    converts the call into a call to `_info_from_file`.

    Subclasses should add additional fields.  They should also
    either reimplement `_set_info` or else implement the method
    `_info_from_file` which will be passed an open, read-only
    file object.
    """

    def _set_info(
        self, node: StorageNode, path: pathlib.Path, item: ArchiveAcq
    ) -> dict:
        """Set info for new ArchiveAcq `item` from file `path`.

        Converts the standard Alpenhorn _set_info call
        into a `_info_from_file` call, if that method exists.

        Parameters
        ----------
        node : StorageNode
            the node we're importing on
        path : pathlib.Path
            the path relative to `node.root` of the file
            being imported in this acquistion
        item : alpenhorn.archive.ArchiveAcq
            the newly-created acq record

        Returns
        -------
        info : dict
            any data returned by `_info_from_file`, if that
            method exists, or else an empty dict.
        """
        if hasattr(self, "_info_from_file"):
            # Open the file
            with node.io.open(path) as file:
                # Get keywords from file
                return self._info_from_file(file)
        else:
            return dict()


class CHIMEFileInfo(file_info_base):
    """Abstract base info class for a CHIME file

    Subclasses must re-implement `_parse_filename` to implement
    type detection, and optionally data gathering.

    Subclasses should add additional fields.  They should also
    either reimplement `_set_info` or else implement the method
    `_info_from_file` which will be passed an open, read-only
    file object.
    """

    @classmethod
    def _parse_filename(cls, name: str) -> dict | None:
        """Parse the filename `name`.

        Called from is_type() to check whether this is an
        appropriate file.  If it isn't, raise ValueError.

        Also called by the default `_set_info()`: if it returns
        a dict, the contents of that dict will be added to the info
        data.

        Parameters
        ----------
        name : str
            the name of the file being imported.

        Raises
        ------
        ValueError
            the name was not valid for this type
        """
        raise NotImplementedError("must be re-implemented in subclass")

    @classmethod
    def is_type(
        cls,
        path: pathlib.Path,
        node: StorageNode,
        acqtype: AcqType,
        acqname: str,
    ) -> bool:
        """Returns true if this is a corr file."""
        try:
            cls._parse_filename(path.name)
        except ValueError:
            return False

        return True

    def _set_info(
        self, node: StorageNode, path: pathlib.Path, item: ArchiveFile
    ) -> dict:
        """Set info for new ArchiveFile `item` from file `path`.

        If defined in the class, calls `_info_from_file` to
        populate the info record.

        Additionally, if `_parse_filename` returns a dict, that
        will also be merged into the info data.

        Parameters
        ----------
        node : StorageNode
            the node we're importing on
        path : pathlib.Path
            the path relative to `node.root` of the file being imported
        item : alpenhorn.archive.ArchiveFile
            the newly-created file record

        Returns
        -------
        info : dict
            A dict containing data from `_info_from_file` and/or
            `_parse_filename`.
        """
        if hasattr(self, "_info_from_file"):
            # Open the file
            with node.io.open(path) as file:
                # Get keywords from file
                info = self._info_from_file(file)
        else:
            info = dict()

        # Try _parse_filename, too
        result = self._parse_filename(path.name)

        if isinstance(result, dict):
            info |= result

        return info
