"""Ergonomic helpers around :mod:`pyspark` session management."""
from __future__ import annotations

from typing import Mapping

from pyspark.sql import SparkSession

__all__ = ["create_spark_session"]


def create_spark_session(
    app_name: str = "Fablake",
    *,
    configs: Mapping[str, str] | None = None,
    log_level: str | None = "WARN",
    enable_hive: bool = False,
) -> SparkSession:
    """Create (or reuse) a configured :class:`~pyspark.sql.SparkSession`.

    Parameters
    ----------
    app_name:
        Friendly application name.
    configs:
        Optional Spark configuration key/value pairs.
    log_level:
        Log level applied to the underlying SparkContext when provided.
    enable_hive:
        When ``True`` the session is created with Hive support.
    """

    builder = SparkSession.builder.appName(app_name)
    for key, value in (configs or {}).items():
        builder = builder.config(key, value)

    if enable_hive:
        builder = builder.enableHiveSupport()

    session = builder.getOrCreate()

    if log_level:
        session.sparkContext.setLogLevel(log_level)

    return session
