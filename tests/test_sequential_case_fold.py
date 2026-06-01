"""Multi-group case-folding for Trends (sequential analysis).

Regression test for: when case-insensitive grouping was requested and a
group-by column was Categorical/Enum (common for low-cardinality fields), the
old `== pl.String` guard skipped folding it. A multi-group key like
"party - stance" then kept the Categorical part's original case and series that
differed only by case ("lnp - Yes" vs "lnp - yes") never merged.
"""

from __future__ import annotations

from datetime import datetime

import polars as pl
from ldaca_wordflow.api.workspaces.analyses.sequential_analysis import (
    VisibleGroupSelection,
    _build_group_filter_expression,
    _run_sequential_analysis,
)


def _frame() -> pl.DataFrame:
    # party = plain String (folded by the old code), stance = Categorical
    # (skipped by the old code) — mixed case in both.
    return pl.DataFrame(
        {
            "ts": [
                datetime(2020, 1, 1),
                datetime(2020, 1, 2),
                datetime(2020, 1, 3),
                datetime(2020, 1, 4),
            ],
            "party": ["LNP", "lnp", "LNP", "lnp"],
            "stance": ["Yes", "yes", "Yes", "yes"],
        }
    ).with_columns(pl.col("stance").cast(pl.Categorical))


def test_multigroup_case_insensitive_folds_categorical_column():
    res = _run_sequential_analysis(
        _frame().lazy(),
        time_column="ts",
        group_by_columns=["party", "stance"],
        frequency="yearly",
        case_sensitive=False,
    )
    groups = res.select(["party", "stance"]).unique()
    # Both columns folded → a single merged series, all lowercase.
    assert groups.height == 1
    row = groups.row(0, named=True)
    assert row["party"] == "lnp"
    assert row["stance"] == "yes"
    assert int(res.select(pl.col("sequential_count").sum()).item()) == 4


def test_multigroup_case_sensitive_preserves_distinct_groups():
    res = _run_sequential_analysis(
        _frame().lazy(),
        time_column="ts",
        group_by_columns=["party", "stance"],
        frequency="yearly",
        case_sensitive=True,
    )
    # Case-sensitive: "LNP - Yes" and "lnp - yes" stay distinct (contrast the
    # case-insensitive test above, which merges these two into one).
    assert res.select(["party", "stance"]).unique().height == 2


def test_numeric_group_column_does_not_break_case_insensitive():
    df = pl.DataFrame(
        {
            "ts": [datetime(2020, 1, 1), datetime(2020, 1, 2)],
            "party": ["LNP", "lnp"],
            "bucket": [1, 1],  # numeric group column — no case, must not error
        }
    )
    res = _run_sequential_analysis(
        df.lazy(),
        time_column="ts",
        group_by_columns=["party", "bucket"],
        frequency="yearly",
        case_sensitive=False,
    )
    # party folds to "lnp", numeric bucket stringifies to "1" → one series.
    assert res.select(["party", "bucket"]).unique().height == 1


def test_visible_group_filter_folds_categorical_column():
    df = _frame()
    expr = _build_group_filter_expression(
        visible_groups=[VisibleGroupSelection(values={"party": "lnp", "stance": "yes"})],
        schema=df.schema,
        case_sensitive=False,
    )
    assert expr is not None
    # The folded filter must match all four rows (both LNP/lnp and Yes/yes),
    # not just the already-lowercase ones.
    assert df.filter(expr).height == 4
