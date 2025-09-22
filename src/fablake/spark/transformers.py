"""DataFrame transformation helpers built on top of PySpark."""
from __future__ import annotations

from typing import Iterable, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

__all__ = [
    "remove_nulls",
    "remove_duplicates",
    "clean_text_columns",
]

_ALLOWED_OPERATIONS = {
    "trim": F.trim,
    "lower": F.lower,
    "upper": F.upper,
    "normalize_whitespace": lambda column: F.regexp_replace(column, r"\\s+", " "),
}


def remove_nulls(
    df: DataFrame,
    *,
    how: str = "any",
    subset: Sequence[str] | None = None,
) -> DataFrame:
    """Drop rows containing nulls according to the provided strategy."""
    return df.dropna(how=how, subset=list(subset) if subset else None)


def remove_duplicates(df: DataFrame, subset: Sequence[str] | None = None) -> DataFrame:
    """Drop duplicate rows using ``subset`` when provided."""
    return df.dropDuplicates(list(subset) if subset else None)


def clean_text_columns(
    df: DataFrame,
    columns: Iterable[str],
    operations: Sequence[str] | None = None,
) -> DataFrame:
    """Apply a set of canonical text-cleaning operations to columns."""
    ops = operations or ("trim", "normalize_whitespace")
    for column in columns:
        spark_column = F.col(column)
        for op in ops:
            try:
                transformer = _ALLOWED_OPERATIONS[op]
            except KeyError as exc:  # pragma: no cover - defensive branch
                raise ValueError(f"Unsupported text operation: {op}") from exc
            spark_column = transformer(spark_column)
        df = df.withColumn(column, spark_column)
    return df
