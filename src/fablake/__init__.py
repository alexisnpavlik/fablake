"""Public package interface for fablake."""
from __future__ import annotations

from importlib import metadata

from . import spark
from .spark import *  # noqa: F401,F403
from .utils import create_spark_session

try:  # pragma: no cover - during development the package is not installed
    __version__ = metadata.version("fablake")
except metadata.PackageNotFoundError:  # pragma: no cover
    __version__ = "0.0.0"

__all__ = ["create_spark_session", "spark", "__version__", *spark.__all__]
