import json


def replace_special_characters(field_name, special_characters_dict):
    """
    Replaces special characters in the given field_name using the given special_characters_dict.

    Args:
        field_name (str): The string in which special characters need to be replaced.
        special_characters_dict (dict): A dictionary with special characters as keys and their replacements as values.

    Returns:
        str: The modified string with special characters replaced.
    """
    for special_char, replacement in special_characters_dict.items():
        field_name = field_name.replace(special_char, replacement)
    return field_name


def handle_duplicates(names_list):
    """
    Handles duplicates in the given list of names by appending a numeric suffix.

    Args:
        names_list (list): The list of names to check for duplicates.

    Returns:
        list: The modified list with duplicates handled.
    """
    seen = {}
    result = []
    for name in names_list:
        if name in seen:
            seen[name] += 1
            result.append(f'{name}{seen[name]}')
        else:
            seen[name] = 0
            result.append(name)

    return result


def process_names_list(names_list):
    """
    Processes the given list of names by removing special characters, replacing special characters, and handling duplicates.

    Args:
        names_list (list): The list of names to process.

    Returns:
        list: The processed list of names.
    """
    with open("fablake/fablake/spark/data_quality/dict_mapping.json", "r") as f:
        dict_mapping = json.load(f)
    new_names_list = []

    for name in names_list:
        name = name.strip().lower()
        name = replace_special_characters(name, dict_mapping)
        if name == '':
            name = 'unnamed'
        new_names_list.append(name)

    new_names_list = handle_duplicates(new_names_list)
    return new_names_list


def normalize_columns(df):
    """
    Normalizes the columns of the given DataFrame by processing the existing column names.

    Args:
        df (DataFrame): The DataFrame whose columns need to be renamed.

    Returns:
        DataFrame: The DataFrame with columns renamed.
    """

    original_columns = df.columns
    new_columns = process_names_list(original_columns)
    new_columns = process_names_list(new_columns)

    map_columns = dict(zip(original_columns, new_columns))

    for old_col, new_col in map_columns.items():
        df = df.withColumnRenamed(old_col, new_col)

    return df
