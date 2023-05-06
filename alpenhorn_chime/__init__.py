"""Alpenhorn extensions for CHIME."""

from .detection import import_detect


def register_extension() -> dict:
    """Return alpenhorn extension data.

    This module provides the CHIME import-detect routine.
    """
    return {"import-detect": import_detect}
