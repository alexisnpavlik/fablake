"""Helpers to coerce Spark DataFrame columns into specific data types."""
from __future__ import annotations

from typing import Iterable

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_unixtime, regexp_replace

__all__ = [
    "convert_to_float",
    "convert_to_int",
    "convert_to_money",
    "convert_to_string",
    "convert_date_unix_columns",
]


def convert_to_int(df: DataFrame, column: str) -> DataFrame:
    """Cast *column* to ``int`` after replacing nulls with ``0``."""
    df = df.fillna({column: 0})
    return df.withColumn(column, col(column).cast("int"))


def convert_to_float(df: DataFrame, column: str) -> DataFrame:
    """Cast *column* to ``float`` after replacing nulls with ``0.0``."""
    df = df.fillna({column: 0.0})
    return df.withColumn(column, col(column).cast("float"))


def convert_to_money(df: DataFrame, column: str) -> DataFrame:
    """Normalise monetary strings and cast the *column* to ``double``.

    The helper assumes European formatted numbers (``.`` as thousand separator
    and ``,`` as decimal separator). Any whitespace is removed prior to the
    conversion.
    """
    df = df.fillna({column: 0})
    df = df.withColumn(column, col(column).cast("string"))
    df = df.withColumn(column, regexp_replace(col(column), r"\\s", ""))
    df = df.withColumn(column, regexp_replace(col(column), r"\\.", ""))
    df = df.withColumn(column, regexp_replace(col(column), ",", "."))
    return df.withColumn(column, col(column).cast("double"))


def convert_to_string(df: DataFrame, column: str) -> DataFrame:
    """Cast *column* to ``string`` and replace nulls with ``""``."""
    df = df.withColumn(column, col(column).cast("string"))
    return df.fillna({column: ""})


def convert_date_unix_columns(df: DataFrame, date_columns: Iterable[str]) -> DataFrame:
    """Convert Unix timestamp columns expressed in milliseconds to timestamps."""
    for column in date_columns:
        df = df.withColumn(column, from_unixtime(col(column) / 1000).cast("timestamp"))
    return df
