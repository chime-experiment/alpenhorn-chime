"""CHIME rawadc info tables."""
import re
import h5py
import peewee as pw

from .base import CHIMEAcqInfo, CHIMEFileInfo


class RawadcAcqInfo(CHIMEAcqInfo):
    """Information about a rawadc acquisition.

    Attributes
    ----------
    acq : foreign key to ArchiveAcq
        The acquisition that the information is for.
    inst : foreign key to ArchiveInst
        The instrument that took the acquisition.
    start_time : double
        When the raw ADC acquisition was performed, in UNIX time.
    """

    start_time = pw.DoubleField(null=True)

    def _set_info(self, node, path, item):
        """Generate acq info.

        Parameters
        ----------
        node : StorageNode
            the node we're importing on
        path : pathlib.Path
            the path relative to `node.root` of the file
            being imported in this acquistion
        item : alpenhorn.archive.ArchiveAcq
            the newly-created acq record
        """

        info = super()._set_info(node, path, item)
        info["start_time"] = self.timestamp_from_name(item.name)
        return info


class RawadcFileInfo(CHIMEFileInfo):
    """Information about a rawadc data file.

    Attributes
    ----------
    file : foreign key to ArchiveFile
        The file this information is about.
    start_time : float
        Start of acquisition in UNIX time.
    finish_time : float
        End of acquisition in UNIX time.
    """

    start_time = pw.DoubleField(null=True)
    finish_time = pw.DoubleField(null=True)

    @classmethod
    def _parse_filename(cls, name: str) -> None:
        """Is this a valid RAW adc filename?

        Parameters
        ----------
        name : str
            Rawadc filename.

        Raises
        ------
        ValueError
            `name` didn't have the right form
        """

        match = re.match(r"[0-9]{6}\.h5", name)
        if not match:
            raise ValueError(f"bad rawadc file name: {name}")

    def _info_from_file(self, file):
        """Get rawadc file info.

        Parameters
        ----------
        file : open, read-only file
            the file being imported.
        """
        f = h5py.File(file, "r")
        times = f["timestamp"]["ctime"]
        start_time = times.min()
        finish_time = times.max()
        f.close()

        return {"start_time": start_time, "finish_time": finish_time}
