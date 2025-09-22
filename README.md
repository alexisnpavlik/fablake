# fablake

Fablake is a small toolbox that groups together convenience helpers for PySpark
workloads. It focuses on three areas that typically reappear in any data
project:

- quick data quality fixes (normalising column names, shaping types);
- reusable transformations for text cleaning and deduplication;
- lightweight validation utilities to surface null density or duplicate ratios.

The project follows the standard `src/` layout and ships with a `pyproject.toml`
file, meaning it can be built and published to PyPI with modern packaging tools.

## Installation

```bash
pip install fablake
```

When developing locally, install the project in editable mode:

```bash
pip install -e .[dev]
```

## Quick start

```python
from fablake import create_spark_session, remove_duplicates

spark = create_spark_session(app_name="FablakeDemo")

raw_df = spark.createDataFrame(
    [("Alice", 10), ("Bob", 5), ("Alice", 10)], ["name", "purchases"]
)

deduped_df = remove_duplicates(raw_df)
print(deduped_df.count())  # -> 2
```

All Spark-facing helpers live under `fablake.spark`. Functions are documented
with docstrings and type annotations for quick discovery in your editor.

## Testing

The repository ships with pytest-based suites. You can run them with:

```bash
pytest
```

The tests are skipped automatically when PySpark is not available in the
environment.
