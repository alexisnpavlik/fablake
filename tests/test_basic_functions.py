"""Smoke tests for the fablake PySpark helpers."""

import pytest

pytest.importorskip("pyspark")

from fablake import (
    clean_text_columns,
    create_spark_session,
    remove_duplicates,
    remove_nulls,
    validate_duplicates,
    validate_nulls,
)
from fablake.spark.data_quality import convert_to_money, normalize_columns


@pytest.fixture(scope="session")
def spark():
    """Provide a shared Spark session for the test run."""
    session = create_spark_session(app_name="FablakeTests", log_level="ERROR")
    yield session
    session.stop()


@pytest.fixture
def sample_data(spark):
    """Sample dataset used across tests."""
    data = [
        ("Juan", "Pérez", 25, "juan@email.com"),
        ("María", None, 30, "maria@email.com"),
        (" Pedro ", "López", None, "pedro@email.com"),
        ("Juan", "Pérez", 25, "juan@email.com"),
        (None, "García", 28, None),
    ]
    columns = ["nombre", "apellido", "edad", "email"]
    return spark.createDataFrame(data, columns)


def test_remove_nulls(sample_data):
    cleaned = remove_nulls(sample_data, how="any")
    assert cleaned.count() == 2

    cleaned_all = remove_nulls(sample_data, how="all")
    assert cleaned_all.count() == sample_data.count()


def test_remove_duplicates(sample_data):
    deduped = remove_duplicates(sample_data)
    assert deduped.count() == sample_data.count() - 1


def test_clean_text_columns(sample_data):
    cleaned = clean_text_columns(
        sample_data,
        columns=["nombre"],
        operations=["trim", "lower"],
    )
    pedro_row = cleaned.filter(cleaned.nombre == "pedro").collect()
    assert len(pedro_row) == 1


def test_validate_nulls(sample_data):
    # ~20% of the inspected cells are null
    result = validate_nulls(sample_data, threshold=10.0)
    assert result["passed"] is False
    assert result["null_percentage"] == 20.0

    result_high = validate_nulls(sample_data, threshold=50.0)
    assert result_high["passed"] is True


def test_validate_duplicates(sample_data):
    result = validate_duplicates(sample_data, threshold=10.0)
    assert result["passed"] is False
    assert result["duplicate_percentage"] == 20.0


def test_convert_to_money(spark):
    data = [("1.234,50",), (None,), ("987",)]
    df = spark.createDataFrame(data, ["monto"])
    converted = convert_to_money(df, "monto")
    collected = [row.monto for row in converted.collect()]
    assert collected == [1234.5, 0.0, 987.0]


def test_normalize_columns(spark):
    df = spark.createDataFrame([(1, 2)], [" Producto ", "Precio ($)"])
    normalized = normalize_columns(df)
    assert normalized.columns == ["producto", "precio_"]
