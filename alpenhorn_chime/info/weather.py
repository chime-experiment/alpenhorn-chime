"""Weather data info classes."""
from __future__ import annotations
from typing import TYPE_CHECKING, BinaryIO

import re
import calendar
import datetime
import peewee as pw

from .base import CHIMEAcqDetect, CHIMEFileInfo

if TYPE_CHECKING:
    import pathlib
    from alpenhorn.acquisition import ArchiveAcq
    from alpenhorn.storage import StorageNode


# No-model acq class
class WeatherAcqDetect(CHIMEAcqDetect):
    pass


class WeatherFileInfo(CHIMEFileInfo):
    """CHIME weather file info.

    Attributes
    ----------
    file : foreign key
        Reference to the file this information is about.
    start_time : float
        Start of acquisition in UNIX time.
    finish_time : float
        End of acquisition in UNIX time.
    date : string
        The date of the weather data, in the form YYYYMMDD.
    """

    start_time = pw.DoubleField(null=True)
    finish_time = pw.DoubleField(null=True)
    date = pw.CharField(null=True, max_length=8)

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
        if not re.match(r"(20[1-9][0-9][01][0-9][0-3][0-9])\.h5", name):
            raise ValueError(f"bad weather file name: {name}")

    def _set_info(
        self, node: StorageNode, path: pathlib.Path, item: ArchiveAcq
    ) -> dict:
        """Generate weather file info."""

        date = datetime.datetime.strptime(str(path)[0:8], "%Y%m%d")
        start_time = calendar.timegm(date.utctimetuple())
        finish_time = calendar.timegm(
            (
                date + datetime.timedelta(days=1) - datetime.timedelta(seconds=1)
            ).utctimetuple()
        )

        return {"start_time": start_time, "finish_time": finish_time, "date": date}
