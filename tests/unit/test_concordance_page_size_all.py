"""Page-size overrides on POST /concordance/.../result."""

from typing import Any, cast

import pytest
from ldaca_wordflow.api.workspaces.analyses.concordance import (
    ConcordanceResultQuery,
    _apply_result_query_overrides,
)
from pydantic import ValidationError


def test_page_size_still_accepts_int() -> None:
    query = ConcordanceResultQuery(page_size=25)
    assert query.page_size == 25


def test_page_size_rejects_other_strings() -> None:
    with pytest.raises(ValidationError):
        ConcordanceResultQuery(page_size=cast(Any, "huge"))


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
