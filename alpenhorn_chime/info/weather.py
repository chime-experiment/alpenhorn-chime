"""Weather data info classes."""
from __future__ import annotations
from typing import TYPE_CHECKING

import calendar
import datetime
import peewee as pw

from .base import CHIMEFileInfo

if TYPE_CHECKING:
    import pathlib
    from alpenhorn.update import UpdateableNode


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

    def _set_info(self, node: UpdateableNode, path: pathlib.Path, name_data: dict) -> dict:
        """Generate weather file info."""

        date = name_data["date"]
        dt = datetime.datetime.strptime(date, "%Y%m%d")
        start_time = calendar.timegm(dt.utctimetuple())
        finish_time = calendar.timegm(
            (
                dt + datetime.timedelta(days=1) - datetime.timedelta(seconds=1)
            ).utctimetuple()
        )

        return {"start_time": start_time, "finish_time": finish_time, "date": date}
