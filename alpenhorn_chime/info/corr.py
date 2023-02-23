"""CHIME correlator info tables."""
from typing import BinaryIO

import re
import h5py
import numpy as np
import peewee as pw

from .base import CHIMEAcqInfo, CHIMEFileInfo


class CorrAcqInfo(CHIMEAcqInfo):
    """Information about a correlation acquisition.

    Attributes
    ----------
    acq : foreign key to ArchiveAcq
        The acquisition that the information is for.
    inst : foreign key to ArchiveInst
        The instrument that took the acquisition.
    integration : float
        Integration time in seconds.
    nfreq : integer
        Number of frequency channels.
    nprod : integer
        Number of correlation products in acquisition.
    """

    integration = pw.DoubleField(null=True)
    nfreq = pw.IntegerField(null=True)
    nprod = pw.IntegerField(null=True)

    def _info_from_file(self, file: BinaryIO) -> dict:
        """Return corr acq info from a file in the acq.

        Copied from `auto_import.get_acqcorrinfo_keywords_from_h5`
        in alpenhorn-1.

        Parameters
        ----------
        file : open, read-only file being imported.
        """

        # Find the integration time from the median difference between timestamps.
        f = h5py.File(file, "r")
        dt = np.array([])
        t = f["/index_map/time"]
        for i in range(1, len(t)):
            dt = np.append(dt, float(t[i][1]) - float(t[i - 1][1]))
        integration = np.median(dt)
        n_freq = len(f["/index_map/freq"])
        n_prod = len(f["/index_map/prod"])

        f.close()
        return {"integration": integration, "nfreq": n_freq, "nprod": n_prod}


class CorrFileInfo(CHIMEFileInfo):
    """Information about a correlation data file.

    Attributes
    ----------
    file : foreign key to ArchiveFile
        The file this information is about.
    chunk_number : integer
        Label for where in the acquisition this file is.
    freq_number : integer
        Which frequency slice this file is.
    start_time : float
        Start of acquisition in UNIX time.
    finish_time : float
        End of acquisition in UNIX time.
    """

    start_time = pw.DoubleField(null=True)
    finish_time = pw.DoubleField(null=True)
    chunk_number = pw.IntegerField(null=True)
    freq_number = pw.IntegerField(null=True)

    @classmethod
    def _parse_filename(cls, name: str) -> dict:
        """Return chunk and frequency based on `name`.

        Copied from `chimedb.data_index.util.parse_corrfile_name`.

        Parameters
        ----------
        name : str
            Correlator filename.

        Returns
        -------
        dict with keys:

        chunk_number : int
            chunk number
        freq_number : int
            frequency number

        Raises
        ------
        ValueError
            `name` didn't have the right form
        """

        match = re.match(r"([0-9]{8})_([0-9]{4})\.h5", name)
        if not match:
            raise ValueError(f"bad correlator file name: {name}")

        return {"chunk_number": int(match.group(1)), "freq_number": int(match.group(2))}

    def _info_from_file(self, file: BinaryIO) -> dict:
        """Get corr file info.

        Parameters
        ----------
        file : open, read-only file
            the file being imported.
        """
        f = h5py.File(file, "r")
        start_time = f["/index_map/time"][0][1]
        finish_time = f["/index_map/time"][-1][1]
        f.close()

        return {
            "start_time": start_time,
            "finish_time": finish_time,
        }
