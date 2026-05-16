"""Snapshot-view ``page_size: "all"`` override on POST /concordance/.../result.

Verifies that the API schema accepts the literal string ``"all"`` and
that ``_apply_result_query_overrides`` translates it to the server-side
hard cap before downstream code reads ``page_size`` as an ``int``.
"""

import pytest
from pydantic import ValidationError

from ldaca_wordflow.api.workspaces.analyses.concordance import (
    SNAPSHOT_ALL_PAGE_SIZE_CAP,
    ConcordanceResultQuery,
    _apply_result_query_overrides,
)


def test_page_size_accepts_literal_all() -> None:
    query = ConcordanceResultQuery(page_size="all")
    assert query.page_size == "all"


def test_page_size_still_accepts_int() -> None:
    query = ConcordanceResultQuery(page_size=25)
    assert query.page_size == 25


def test_page_size_rejects_other_strings() -> None:
    with pytest.raises(ValidationError):
        ConcordanceResultQuery(page_size="huge")


def test_apply_result_query_overrides_translates_all_to_cap() -> None:
    normalized: dict = {"page_size": 25}
    query = ConcordanceResultQuery(page_size="all")
    out = _apply_result_query_overrides(normalized, query)
    assert out["page_size"] == SNAPSHOT_ALL_PAGE_SIZE_CAP
    assert isinstance(out["page_size"], int), (
        "Downstream code reads page_size as int; the override path must "
        "translate the string sentinel before stuffing it into the request."
    )


def test_apply_result_query_overrides_passes_int_page_size_through() -> None:
    normalized: dict = {}
    query = ConcordanceResultQuery(page_size=25)
    out = _apply_result_query_overrides(normalized, query)
    assert out["page_size"] == 25


def test_apply_result_query_overrides_leaves_unset_page_size_alone() -> None:
    normalized: dict = {"page_size": 10}
    query = ConcordanceResultQuery()
    out = _apply_result_query_overrides(normalized, query)
    # No override given → existing normalized value preserved.
    assert out["page_size"] == 10


def test_snapshot_cap_matches_documented_value() -> None:
    # The frontend ``SNAPSHOT_CAPS.demo.maxResultRows`` is also 500_000.
    # If this constant changes, update both sides together to keep the
    # capture-side eligibility check aligned with the server-side cap.
    assert SNAPSHOT_ALL_PAGE_SIZE_CAP == 500_000
