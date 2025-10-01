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

    This function is called by alpenhorn when loading this extension to
    get the list of Extensions which we provide.
    """
    global __version__
    from alpenhorn.extensions import ImportDetectExtension
    from .detection import import_detect

    return [ImportDetectExtension("Detect", __version__, detect=import_detect)]
