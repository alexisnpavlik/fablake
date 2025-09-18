import os
import re
import pandas as pd

from pyspark.sql.functions import col, sum, trim, regexp_replace, count, when, from_unixtime
from pyspark.sql.types import StringType, NullType


def drop_rows_if_column_null(df, column_name):
    """
    Drops rows from the DataFrame where the specified column is null.

    Args:
        df (pyspark.sql.DataFrame): Input DataFrame.
        column_name (str): Name of the column to check for nulls.

    Returns:
        pyspark.sql.DataFrame: DataFrame with rows removed where the column is null.
    """
    return df.na.drop(subset=[column_name])


def check_null_columns(df):
    """
    This function checks for null columns in a DataFrame.

    Args:
        df (pyspark.sql.DataFrame): The DataFrame to check.

    Returns:
        null_columns (list): A list of columns with null values.
    """
    non_null_counts = df.select([
        count(when(col(c).isNotNull(), c)).alias(c) for c in df.columns]).collect()[0].asDict()

    null_columns = [col_name for col_name,
                    count_val in non_null_counts.items() if count_val == 0]

    return null_columns


def convert_null_columns_to_string(df):
    """
    This function converts null columns in the DataFrame to the appropriate data types.

    Args:
        df (DataFrame): The DataFrame to convert null columns.

    Returns:
        DataFrame: The DataFrame with null columns converted
    """

    for field in df.schema.fields:
        if isinstance(field.dataType, NullType):
            df = df.withColumn(field.name, col(field.name).cast(StringType()))
            print(f"Column {field.name} casted to StringType")

    return df
