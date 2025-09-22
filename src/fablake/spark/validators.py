"""Validation helpers for PySpark DataFrames."""
from __future__ import annotations

from typing import Mapping, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

__all__ = [
    "validate_nulls",
    "validate_duplicates",
]


def validate_nulls(
    df: DataFrame,
    *,
    threshold: float = 0.0,
    subset: Sequence[str] | None = None,
) -> Mapping[str, float | bool]:
    """Return metrics about null density and whether it passes *threshold*.

    The percentage represents the share of null cells within the inspected
    columns. A dataset with 10% null density will pass when ``threshold`` is
    ``>= 10``.
    """
    columns = list(subset) if subset else df.columns
    row_count = df.count()
    inspected_cells = row_count * len(columns)

    if inspected_cells == 0:
        return {
            "passed": True,
            "null_percentage": 0.0,
            "null_cells": 0,
            "inspected_cells": 0,
        }

    null_aggregations = [
        F.count(F.when(F.col(column).isNull(), column)).alias(column) for column in columns
    ]
    null_counts_row = df.select(null_aggregations).collect()[0]
    null_cells = sum(null_counts_row.asDict().values())
    null_percentage = (null_cells / inspected_cells) * 100

    return {
        "passed": null_percentage <= threshold,
        "null_percentage": round(null_percentage, 2),
        "null_cells": int(null_cells),
        "inspected_cells": int(inspected_cells),
    }


def validate_duplicates(
    df: DataFrame,
    *,
    threshold: float = 0.0,
    subset: Sequence[str] | None = None,
) -> Mapping[str, float | bool | int]:
    """Return duplicate ratio metrics and whether they pass *threshold*."""
    total_rows = df.count()
    if total_rows == 0:
        return {
            "passed": True,
            "duplicate_percentage": 0.0,
            "duplicate_rows": 0,
            "total_rows": 0,
        }

    unique_rows = df.dropDuplicates(list(subset) if subset else None).count()
    duplicate_rows = total_rows - unique_rows
    duplicate_percentage = (duplicate_rows / total_rows) * 100

    return {
        "passed": duplicate_percentage <= threshold,
        "duplicate_percentage": round(duplicate_percentage, 2),
        "duplicate_rows": int(duplicate_rows),
        "total_rows": int(total_rows),
    }
