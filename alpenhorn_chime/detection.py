"""alpenhorn import detection logic for CHIME

CHIME's import detection logic is fairly simple and
is performed by pattern matching on the acqusition
directory and filename.
"""
from __future__ import annotations
from typing import TYPE_CHECKING

import re
import datetime
import peewee as pw
from functools import partial

from alpenhorn.acquisition import ArchiveAcq as AlpenAcq
from alpenhorn.acquisition import ArchiveFile as AlpenFile

from .types import AcqType, FileType
from .inst import ArchiveInst

if TYPE_CHECKING:
    import pathlib
    from collections.abc import Callable

    from alpenhorn.archive import ArchiveFileCopy
    from alpenhorn.update import UpdateableNode


# Extended CHIME versions of the tables
class ArchiveAcq(AlpenAcq):
    """Extend ArchiveAcq record.

    Adds "type" and "inst" to hold a refernce to the AcqType and ArchiveInst.
    """

    type = pw.ForeignKeyField(AcqType, backref="acqs", null=True)
    inst = pw.ForeignKeyField(ArchiveInst, backref="acqs", null=True)


class ArchiveFile(AlpenFile):
    """Extend ArchiveFile record.

    Adds a "type" attribute to hold a refernce to the FileType.

    Also re-implements "acq" to reference the CHIME ArchiveAcq and
    not the base alpenhorn one.
    """

    acq = pw.ForeignKeyField(ArchiveAcq, backref="files")
    type = pw.ForeignKeyField(FileType, backref="files", null=True)


def set_info(
    info_name: str | None,
    node: UpdateableNode,
    path: pathlib.Path,
    item: ArchiveAcq | ArchiveFile,
    name_data: dict = dict(),
) -> None:
    """Populate Acq or File Info class, if necessary.

    If `info_name` is None, this function does nothing.

    Parameters
    ----------
    info_name : str or None
        The name of the Info class in `alpenhorn_chime.info`.
    node : UpdateableNode
        The node on which the import has happened
    path : pathlib.Path
        Path, relative to node root, to the imported file
    item : ArchiveAcq or ArchiveFile
        The archive item (acq or file) that we're adding info for
    name_data : dict, optional
        A dictionary containing all the named subgroups of the pathname
        pattern match.  May be empty.
    """
    from . import info

    # Do nothing if there's no info class
    if info_name is None:
        return

    class_ = getattr(info, info_name)

    # If class_ is _not_ a class, then we it's a function that will
    # return us the class when passed the archive item.
    if not isinstance(class_, type):
        class_ = class_(item)

    # Generate the new row by passing a bunch of stuff to the
    # class constructor, which will replace it with normal
    # table data before the peewee Model is instantiated
    info = class_(item_=item, node_=node, path_=path, name_data_=name_data)
    info.save()


def store_info(
    copy: ArchiveFileCopy,
    file: AlpenFile | None,
    acq: AlpenAcq | None,
    node: UpdateableNode,
    /,
    *,
    import_data: dict,
) -> None:
    """The post-import callback.

    Called by alpenhornd after importing a file.

    Parameters
    ----------
    copy : alpenhorn.archive.ArchiveFileCopy
        The newly-imported file copy
    file : alpenhorn.acquisition.ArchiveFile or None
        If this import created the ArchiveFile, this is it.  Otherwise None.
    acq : alpenhorn.acquisition.ArchiveAcq or None
        If this import created the ArchiveAcq, this is it.  Otherwise None.
    node : alpenhorn.update.UpdateableNode
        The node on which the import happened
    import_data : dict
        The import data collected by `import_detect`.  Filled in by that
        function via a `functools.partial` before this function is given to
        alpenhornd.
    """

    # Add extra acq data, if necessary
    if acq is not None:
        acq = ArchiveAcq.get(id=acq.id)
        acq.type = import_data["acqtype"]
        acq.inst = import_data["acqinst"]
        acq.save()

        # If there's an AcqInfo class, create a new record
        set_info(
            import_data["acqtype"].info_class,
            node=node,
            path=copy.file.path,
            item=acq,
            name_data={"acqtime": import_data["acqtime"]},
        )

    # Add extra file data, if necessary
    if file is not None:
        file = ArchiveFile.get(id=file.id)
        file.type = import_data["filetype"]
        file.save()

        set_info(
            import_data["filetype"].info_class,
            node=node,
            path=file.path,
            item=file,
            name_data=import_data["name_data"],
        )


def parse_acq(
    name: str,
) -> (datetime.datetime | None, ArchiveInst | None, AcqType | None):
    """Parse a CHIME acquisition name

    A standard CHIME acqusition name has the form:

        <ISO-8601-datetime>_<inst-name>_<type-name>

    where the datetime has the form: YYYMMDDTHHMMSSZ

    Parameters
    ----------
    name : str
        the acqusition path to check

    Returns
    -------
    acq_data : dict or None
        If parsing succeeds, returns a dict with three keys:
          * acqtime: the parsed acquisition datetime
          * acqinst: the ArchiveInst of this acquisition
          * acqtype: the AcqType of this acqusition
        On failure, None is returned
    """

    # Split the path on underscores
    parts = str(name).split("_")

    # If we don't have three parts, the path is invalid
    if len(parts) != 3:
        return None

    # Check that the third part is a known AcqType
    try:
        acqtype = AcqType.get(name=parts[2])
    except pw.DoesNotExist:
        return None

    import logging

    logging.getLogger(__name__).info(f"AT: {acqtype!r}")

    # Check that the second part is a known ArchiveInst
    try:
        inst = ArchiveInst.get(name=parts[1])
    except pw.DoesNotExist:
        return None

    # Check that the first part is a valid date
    try:
        time = datetime.datetime.strptime(parts[0], "%Y%m%dT%H%M%SZ")
    except ValueError:
        return None

    # Otherwise type detection succeeds
    return {"acqtime": time, "acqinst": inst, "acqtype": acqtype}


def import_detect(
    path: pathlib.Path, node: UpdateableNode
) -> (str | None, Callable | None):
    """Import detection routine for CHIME data.

    Attempts to decompose `path` to determine acquision
    and file types.

    Parameters
    ----------
    path : pathlib.Path
        The path to the file, relative to `node.db.root`
    node : alpenhorn.udpate.UpdateableNode
        The node on which the file import is taking place

    Returns
    -------
    acq_name : str or None
        When detection succeeds, this is the name of the
        acquisition.  On failure, this is None.
    callback : callable or None
       When detection succeeds, this is a `functools.partial`
       wrapping `store_info`.  On failure, this is None.
    """

    # Split the filename from the directory
    acq_name = str(path.parent)
    file_name = str(path.name)

    # Try to match the acq_name
    import_data = parse_acq(acq_name)

    # Did acq detection fail?
    if import_data is None:
        return None, None

    # Try to determine the file type
    for filetype in import_data["acqtype"].file_types:
        if filetype.pattern is not None:
            m = re.match(filetype.pattern, file_name)
            if m is not None:
                break
    else:
        # file detection failed
        return None, None

    # Remember filetype
    import_data["filetype"] = filetype

    # Regex group matches, if any.  This a tuple which may be empty.
    import_data["name_data"] = m.groupdict()

    # Create partial for callback
    callback = partial(store_info, import_data=import_data)

    # Success
    return acq_name, callback
