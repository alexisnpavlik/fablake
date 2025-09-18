from pyspark.sql.functions import col, sum, trim, regexp_replace, count, when, from_unixtime


def convert_to_int(df, column):
    """
    This function converts a column to integer type.

    Args:
        df (DataFrame): The DataFrame to convert the column.
        column (str): The column to convert to integer.

    Returns:
        DataFrame: The DataFrame with the column converted to integer type.
    """

    df = df.fillna({column: 0})
    df = df.withColumn(column, col(column).cast("int"))

    return df


def convert_to_float(df, column):
    """
    This function converts a column to float type.

    Args:
        df (DataFrame): The DataFrame to convert the column.
        column (str): The column to convert to float.

    Returns:
        DataFrame: The DataFrame with the column converted to float type.
    """

    df = df.fillna({column: 0})
    df = df.withColumn(column, col(column).cast("float"))

    return df


def convert_to_money(df, column):
    """
    This function converts a column to float type.

    Args:
        df (DataFrame): The DataFrame to convert the column.
        column (str): The column to convert to float.

    Returns:
        DataFrame: The DataFrame with the column converted to float type.

    """

    df = df.fillna({column: 0})
    df = df.withColumn(
        column, regexp_replace(col(column), ".", ""))
    df = df.withColumn(
        column, regexp_replace(col(column), ",", "."))
    df = df.withColumn(column, col(column).cast("float"))

    return df


def convert_to_string(df, column):
    """
    This function converts a column to string type.

    Args:
        df (DataFrame): The DataFrame to convert the column.
        column (str): The column to convert to string.

    Returns:
        DataFrame: The DataFrame with the column converted to string type.
    """

    df = df.withColumn(column, col(column).cast("string"))
    df = df.fillna({column: ""})

    return df


def convert_date_unix_columns(df, date_columns):
    """
    Converts specified date columns in a DataFrame from Unix timestamp in milliseconds 
    to datetime format.

    Args:
        df (DataFrame): The input DataFrame containing date columns.
        date_columns (list): List of column names to be converted.

    Returns:
        DataFrame: DataFrame with specified date columns converted to datetime format.
    """
    for column in date_columns:
        df = df.withColumn(
            column,
            from_unixtime(col(column) / 1000).cast("timestamp")
        )
    return df
