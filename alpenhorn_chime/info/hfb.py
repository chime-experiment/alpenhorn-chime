"""CHIME HFB info tables."""
from typing import BinaryIO

import h5py
import numpy as np
import peewee as pw

from .base import CHIMEAcqInfo, CHIMEFileInfo


class HFBAcqInfo(CHIMEAcqInfo):
    """Information about a HFB acquisition.

    Attributes
    ----------
    acq : foreign key
        Reference to the acquisition that the information is for.
    integration : float
        Integration time in seconds.
    nfreq : integer
        Number of frequency channels.
    nsubfreq : integer
        Number of sub-frequencies in acquisition.
    nbeam : integer
        Number of beams in acquisition.

    """

    integration = pw.DoubleField(null=True)
    nfreq = pw.IntegerField(null=True)
    nsubfreq = pw.IntegerField(null=True)
    nbeam = pw.IntegerField(null=True)

    def _info_from_file(self, file: BinaryIO) -> dict:
        """Return HFB corr acq info from a file in the acq.

        Copied from `auto_import.get_acqhfbinfo_keywords_from_h5`
        in alpenhorn-1.

        Parameters
        ----------
        file : open, read-only file being imported.
        """

        # Find the integration time from the median difference between timestamps.
        with h5py.File(file, "r") as f:
            dt = np.array([])
            t = f["/index_map/time"]
            for i in range(1, len(t)):
                dt = np.append(dt, float(t[i][1]) - float(t[i - 1][1]))
            integration = np.median(dt)
            n_freq = len(f["/index_map/freq"])
            n_sub_freq = len(f["/index_map/subfreq"])
            n_beam = len(f["/index_map/beam"])

        return {
            "integration": integration,
            "nfreq": n_freq,
            "nsubfreq": n_sub_freq,
            "nbeam": n_beam,
        }


class HFBFileInfo(CHIMEFileInfo):
    """Information about a HFB data file.

    Attributes
    ----------
    file : foreign key
        Reference to the file this information is about.
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

    def _info_from_file(self, file: BinaryIO) -> dict:
        """Get HFB file info.

        Parameters
        ----------
        file : open, read-only file
            the file being imported.
        """
        with h5py.File(file, "r") as f:
            start_time = f["/index_map/time"][0][1]
            finish_time = f["/index_map/time"][-1][1]

        return {
            "start_time": start_time,
            "finish_time": finish_time,
        }
