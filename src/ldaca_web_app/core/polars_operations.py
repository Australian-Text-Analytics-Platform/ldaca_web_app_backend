"""Static registry of no-arg Polars methods, categorised by dtype family.

Each entry is a mapping from *namespace* (``""`` for top-level methods,
``"str"`` for ``.str.*``, etc.) to a list of ``(method_name, label)``
tuples.  The frontend uses this to populate the operation-picker popup
in the Create tab, filtered by column dtype.
"""

from __future__ import annotations

from typing import TypedDict

import polars as pl


class OperationInfo(TypedDict):
    method: str
    label: str


OperationsByNamespace = dict[str, list[OperationInfo]]

_op = OperationInfo


def _ops(*pairs: tuple[str, str]) -> list[OperationInfo]:
    return [_op(method=m, label=l) for m, l in pairs]


_COMMON_TOP = _ops(
    ("is_null", "Is null"),
    ("is_not_null", "Is not null"),
    ("count", "Count"),
    ("null_count", "Null count"),
    ("n_unique", "Unique count"),
)

OPERATIONS_BY_DTYPE: dict[str, OperationsByNamespace] = {
    "string": {
        "": [
            *_COMMON_TOP,
            *_ops(("len", "Length"), ("unique", "Unique values")),
        ],
        "str": _ops(
            ("to_lowercase", "Lowercase"),
            ("to_uppercase", "Uppercase"),
            ("to_titlecase", "Title case"),
            ("strip_chars", "Strip whitespace"),
            ("strip_chars_start", "Strip leading whitespace"),
            ("strip_chars_end", "Strip trailing whitespace"),
            ("len_chars", "Char length"),
            ("len_bytes", "Byte length"),
            ("reverse", "Reverse"),
            ("escape_regex", "Escape regex chars"),
        ),
    },
    "numeric": {
        "": [
            *_COMMON_TOP,
            *_ops(
                ("sum", "Sum"),
                ("mean", "Mean"),
                ("median", "Median"),
                ("min", "Min"),
                ("max", "Max"),
                ("std", "Std deviation"),
                ("var", "Variance"),
                ("abs", "Absolute value"),
                ("sqrt", "Square root"),
                ("exp", "Exponential"),
                ("log", "Natural log"),
                ("floor", "Floor"),
                ("ceil", "Ceil"),
                ("round", "Round"),
                ("is_nan", "Is NaN"),
                ("is_not_nan", "Is not NaN"),
            ),
        ],
    },
    "datetime": {
        "": _COMMON_TOP,
        "dt": _ops(
            ("year", "Year"),
            ("month", "Month"),
            ("day", "Day"),
            ("hour", "Hour"),
            ("minute", "Minute"),
            ("second", "Second"),
            ("millisecond", "Millisecond"),
            ("microsecond", "Microsecond"),
            ("nanosecond", "Nanosecond"),
            ("date", "Date part"),
            ("time", "Time part"),
            ("ordinal_day", "Day of year"),
            ("quarter", "Quarter"),
            ("week", "ISO week"),
            ("weekday", "Weekday (Mon=1)"),
            ("iso_year", "ISO year"),
        ),
    },
    "boolean": {
        "": [
            *_COMMON_TOP,
            *_ops(("sum", "Sum (true count)"), ("mean", "Mean (true ratio)")),
        ],
    },
    "list": {
        "": _COMMON_TOP,
        "list": _ops(
            ("len", "List length"),
            ("first", "First element"),
            ("last", "Last element"),
            ("reverse", "Reverse list"),
            ("unique", "Unique elements"),
            ("sort", "Sort list"),
        ),
    },
}

_DTYPE_FAMILY_MAP: dict[type, str] = {
    pl.Utf8: "string",
    pl.String: "string",
    pl.Boolean: "boolean",
    pl.Date: "datetime",
    pl.Datetime: "datetime",
    pl.Time: "datetime",
    pl.Duration: "datetime",
    pl.List: "list",
    pl.Array: "list",
}


def dtype_to_family(dtype: pl.DataType) -> str:
    """Map a concrete Polars dtype to a family key used by *OPERATIONS_BY_DTYPE*."""
    base = type(dtype)
    if base in _DTYPE_FAMILY_MAP:
        return _DTYPE_FAMILY_MAP[base]
    if dtype.is_numeric():
        return "numeric"
    return "string"


def get_operations_for_dtype(dtype: pl.DataType) -> OperationsByNamespace:
    """Return the operation registry for the given Polars dtype."""
    family = dtype_to_family(dtype)
    return OPERATIONS_BY_DTYPE.get(family, OPERATIONS_BY_DTYPE["string"])
