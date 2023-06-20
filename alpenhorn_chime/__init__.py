"""Alpenhorn extensions for CHIME."""

from . import reserving
from .detection import import_detect


def register_extension() -> dict:
    """Return alpenhorn extension data.

    This module provides:
     * the CHIME import-detect routine
     * the Reserving I/O module
    """
    return {"import-detect": import_detect, "io-modules": {"reserving": reserving}}
