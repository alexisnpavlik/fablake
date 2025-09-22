"""Helpers to normalise column names by removing special characters."""
from __future__ import annotations

import json
from importlib import resources
from typing import Dict, List, Mapping, Sequence

from pyspark.sql import DataFrame

__all__ = [
    "replace_special_characters",
    "handle_duplicates",
    "process_names_list",
    "normalize_columns",
    "load_special_characters_dict",
]

_MAPPING_RESOURCE = "dict_mapping.json"


def load_special_characters_dict(resource: str = _MAPPING_RESOURCE) -> Dict[str, str]:
    """Load the default mapping of special characters bundled with the package."""
    with resources.files(__package__).joinpath(resource).open("r", encoding="utf-8") as handle:
        return json.load(handle)


def replace_special_characters(field_name: str, special_characters_dict: Mapping[str, str]) -> str:
    """Replace characters in *field_name* using the provided mapping."""
    for special_char, replacement in special_characters_dict.items():
        field_name = field_name.replace(special_char, replacement)
    return field_name


def handle_duplicates(names_list: Sequence[str]) -> List[str]:
    """Append numeric suffixes to duplicate names while preserving order."""
    seen: Dict[str, int] = {}
    result: List[str] = []
    for name in names_list:
        counter = seen.get(name, 0)
        if counter == 0:
            result.append(name)
        else:
            result.append(f"{name}{counter}")
        seen[name] = counter + 1
    return result


def process_names_list(
    names_list: Sequence[str],
    special_characters_dict: Mapping[str, str] | None = None,
) -> List[str]:
    """Normalise raw column names and guarantee uniqueness."""
    mapping = special_characters_dict or load_special_characters_dict()
    new_names: List[str] = []
    for name in names_list:
        candidate = replace_special_characters(name.strip().lower(), mapping)
        candidate = candidate or "unnamed"
        new_names.append(candidate)
    return handle_duplicates(new_names)


def normalize_columns(df: DataFrame) -> DataFrame:
    """Return a new DataFrame with normalised column names."""
    original_columns = df.columns
    new_columns = process_names_list(original_columns)
    rename_map = dict(zip(original_columns, new_columns))
    for old_name, new_name in rename_map.items():
        if old_name != new_name:
            df = df.withColumnRenamed(old_name, new_name)
    return df
