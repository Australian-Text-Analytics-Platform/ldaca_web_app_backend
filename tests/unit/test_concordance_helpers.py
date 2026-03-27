import polars as pl
import pytest
from ldaca_web_app_backend.api.workspaces.analyses.concordance_core import (
    CORE_CONCORDANCE_COLUMNS,
    collect_interleaved_combined,
    compute_concordance_page,
    concordance_non_empty_expr,
    normalize_saved_request,
    sanitize_request_for_storage,
)


@pytest.mark.parametrize(
    "raw_request",
    [
        {
            "node_ids": ["node-1"],
            "node_columns": {"node-1": "text"},
            "search_word": "example",
            "page": 3,
            "page_size": 25,
            "descending": False,
            "pagination": {"page": 3},
            "regex": False,
            "case_sensitive": None,
        }
    ],
)
def test_sanitize_request_excludes_pagination_keys(raw_request):
    sanitized = sanitize_request_for_storage(raw_request)

    assert sanitized == {
        "node_ids": ["node-1"],
        "node_columns": {"node-1": "text"},
        "search_word": "example",
        "regex": False,
    }
    for excluded in (
        "page",
        "page_size",
        "sort_by",
        "descending",
        "pagination",
    ):
        assert excluded not in sanitized


def test_normalize_saved_request_coerces_legacy_shape():
    # We no longer support legacy request formats. Requests must include
    # `node_ids` and `node_columns`.
    raw_request = {
        "node_id": "node-legacy",
        "column": "text",
        "search_word": "alpha",
    }

    assert normalize_saved_request(raw_request) is None


def test_filter_concordance_rows_removes_blank_entries():
    df = pl.DataFrame(
        {
            "CONC_matched_text": ["alpha", None, "   ", ""],
            "CONC_left_context": ["", "", "", ""],
            "CONC_right_context": ["", "context", "\t", None],
        }
    )

    filtered = df.filter(concordance_non_empty_expr())

    assert filtered.height == 2


def test_core_concordance_columns_use_prefixed_names():
    assert CORE_CONCORDANCE_COLUMNS == (
        "CONC_left_context",
        "CONC_matched_text",
        "CONC_right_context",
        "CONC_start_idx",
        "CONC_end_idx",
        "CONC_l1",
        "CONC_r1",
    )


def test_compute_concordance_page_groups_matches_by_source_row():
    request = {
        "search_word": "alpha",
        "num_left_tokens": 2,
        "num_right_tokens": 2,
        "regex": False,
        "case_sensitive": False,
    }
    source = pl.DataFrame(
        {
            "text": ["alpha beta alpha", "gamma alpha"],
            "speaker": ["A", "B"],
        }
    ).lazy()

    result = compute_concordance_page(
        source,
        "text",
        request,
        page=1,
        page_size=1,
        sort_by=None,
        descending=False,
        node_label="node-a",
    )

    assert result["pagination"]["page_size"] == 1
    assert len(result["data"]) == 1

    grouped_row = result["data"][0]
    assert isinstance(grouped_row, list)
    assert len(grouped_row) == 2
    assert all(hit["speaker"] == "A" for hit in grouped_row)
    assert all(hit["__source_node"] == "node-a" for hit in grouped_row)
    assert [hit["CONC_matched_text"] for hit in grouped_row] == ["alpha", "alpha"]


def test_collect_interleaved_combined_interleaves_grouped_rows():
    request = {
        "search_word": "alpha",
        "num_left_tokens": 2,
        "num_right_tokens": 2,
        "regex": False,
        "case_sensitive": False,
    }
    left_source = pl.DataFrame(
        {
            "text": ["alpha beta alpha", "beta alpha"],
            "speaker": ["L1", "L2"],
        }
    ).lazy()
    right_source = pl.DataFrame(
        {
            "text": ["alpha gamma", "alpha delta"],
            "speaker": ["R1", "R2"],
        }
    ).lazy()

    result = collect_interleaved_combined(
        left_source,
        "text",
        right_source,
        "text",
        request,
        page=1,
        page_size=2,
        sort_by=None,
        descending=False,
        left_label="left",
        right_label="right",
    )

    assert len(result["data"]) == 4
    assert all(isinstance(grouped_row, list) for grouped_row in result["data"])
    assert result["data"][0][0]["__source_node"] == "left"
    assert result["data"][1][0]["__source_node"] == "right"
    assert result["data"][2][0]["__source_node"] == "left"
    assert result["data"][3][0]["__source_node"] == "right"
