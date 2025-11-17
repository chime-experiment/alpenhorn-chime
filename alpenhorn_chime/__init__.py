"""Alpenhorn extensions for CHIME."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("alpenhorn_chime")
except PackageNotFoundError:
    # package is not installed
    pass
del version, PackageNotFoundError


def register_extensions() -> dict:
    """Provide the extension to alpenhorn.

    This function is called by alpenhorn when loading this extension module
    to get the list of Extensions which we provide.
    """
    global __version__
    from alpenhorn.extensions import ImportDetectExtension, IOClassExtension
    from .detection import import_detect
    from .hpss.node import SciNetHPSSNodeIO

    return [
        ImportDetectExtension("CHIMEDetect", __version__, detect=import_detect),
        IOClassExtension(
            "SciNetHPSSIO", __version__, "SciNetHPSS", node_class=SciNetHPSSNodeIO
        ),
    ]
