"""Utilities to work with null-heavy Spark columns."""
from __future__ import annotations

from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when
from pyspark.sql.types import NullType, StringType

__all__ = [
    "drop_rows_if_column_null",
    "check_null_columns",
    "convert_null_columns_to_string",
]


def drop_rows_if_column_null(df: DataFrame, column_name: str) -> DataFrame:
    """Return ``df`` without rows where *column_name* is ``NULL``."""
    return df.na.drop(subset=[column_name])


def check_null_columns(df: DataFrame) -> List[str]:
    """Return columns whose values are **all** null."""
    non_null_counts = df.select(
        [count(when(col(c).isNotNull(), c)).alias(c) for c in df.columns]
    ).collect()[0]
    return [name for name, value in non_null_counts.asDict().items() if value == 0]


def convert_null_columns_to_string(df: DataFrame) -> DataFrame:
    """Cast columns with ``NullType`` to ``StringType`` to make them usable."""
    for field in df.schema.fields:
        if isinstance(field.dataType, NullType):
            df = df.withColumn(field.name, col(field.name).cast(StringType()))
    return df
