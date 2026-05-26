"""Unit tests for the per-user DuckDB-backed token cache."""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import duckdb
import polars as pl
import pytest
from ldaca_wordflow.api.workspaces.analyses.generated_columns import (
    TOKENS_FORM,
    derived_column_name,
)
from ldaca_wordflow.core import tokens_cache as tc
from ldaca_wordflow.core.derived_columns import tokenise_column

from docworkspace import Node

TEST_USER = "test_user"


@pytest.fixture(autouse=True)
def isolated_cache_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    db_path = tmp_path / "tokens.duckdb"
    monkeypatch.setattr(tc, "tokens_cache_path", lambda _user_id: db_path)
    return db_path


def test_cache_schema_has_six_columns(isolated_cache_db: Path) -> None:
    tc._connect(TEST_USER).close()
    assert isolated_cache_db.exists()

    with duckdb.connect(str(isolated_cache_db), read_only=True) as conn:
        rows = conn.execute("DESCRIBE token_cache").fetchall()

    assert [row[0] for row in rows] == [
        "model",
        "params_hash",
        "content_hash",
        "tokens",
        "start_offsets",
        "end_offsets",
    ]


def test_tokenise_column_registers_metadata_without_mutating_node_data() -> None:
    node = Node(
        data=pl.DataFrame({"text": ["hello world"]}).lazy(),
        name="probe",
    )

    derived_name = tokenise_column(
        node,
        source_column="text",
        model="bert-base-uncased",
        language="en",
        user_id="ignored",
        workspace_id="workspace",
    )

    assert derived_name == derived_column_name(TOKENS_FORM, "text", "bert-base-uncased")
    assert derived_name in node.derived
    assert derived_name not in node.data.collect_schema().names()
    derived_meta = cast(dict[str, Any], node.derived[derived_name])
    assert derived_meta["cache_backend"] == "duckdb"
    assert "cache_schema_version" not in derived_meta


def test_hydrate_derived_tokens_adds_tokens_column() -> None:
    node = Node(
        data=pl.DataFrame({"text": ["hello world", "hello again"]}).lazy(),
        name="probe",
    )
    derived_name = tokenise_column(
        node,
        source_column="text",
        model="bert-base-uncased",
        language="en",
        user_id="ignored",
    )

    hydrated = tc.hydrate_derived_tokens_lazyframe(
        node.data,
        node=node,
        source_column="text",
        derived_name=derived_name,
        user_id=TEST_USER,
    )
    hydrated_df = cast(pl.DataFrame, hydrated.collect())

    assert derived_name in hydrated_df.columns
    assert derived_name not in node.data.collect_schema().names()
    first_tokens = hydrated_df.to_dicts()[0][derived_name]
    assert isinstance(first_tokens, list) and first_tokens
    assert first_tokens[0]["token"]


def test_warm_cache_does_not_retokenize(monkeypatch: pytest.MonkeyPatch) -> None:
    base = pl.DataFrame({"text": ["hello world", "hello world"]}).lazy()
    expr = tc.cached_tokens_expr(
        pl.col("text"),
        user_id=TEST_USER,
        model="bert-base-uncased",
        lowercase=True,
        remove_punct=True,
    )
    base.with_columns(expr.alias("tokens")).collect()

    def fail(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("warm-cache run should not tokenize misses")

    monkeypatch.setattr(tc, "_tokenize_misses", fail)

    warm = cast(
        pl.DataFrame,
        base.with_columns(expr.alias("tokens")).collect(),
    )
    assert warm.height == 2
    assert warm.to_dicts()[0]["tokens"][0]["token"]


def test_filter_pushdown_only_tokenizes_surviving_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = pl.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "text": ["alpha", "beta", "gamma", "delta", "epsilon"],
        }
    ).lazy()

    seen: list[list[str]] = []
    real = tc._tokenize_misses

    def spy(texts: list[str], **kwargs: Any) -> list[list[dict[str, Any]]]:
        seen.append(list(texts))
        return real(texts, **kwargs)

    monkeypatch.setattr(tc, "_tokenize_misses", spy)

    expr = tc.cached_tokens_expr(
        pl.col("text"),
        user_id=TEST_USER,
        model="bert-base-uncased",
        lowercase=True,
        remove_punct=True,
    )
    filtered = (
        base.with_columns(expr.alias("tokens")).filter(pl.col("id") == 3).collect()
    )

    assert cast(pl.DataFrame, filtered).height == 1
    flat = sorted(t for batch in seen for t in batch)
    assert flat == ["gamma"]


def test_repeated_texts_in_chunk_are_deduplicated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = pl.DataFrame({"text": ["same", "same", "same"]}).lazy()

    seen: list[list[str]] = []
    real = tc._tokenize_misses

    def spy(texts: list[str], **kwargs: Any) -> list[list[dict[str, Any]]]:
        seen.append(list(texts))
        return real(texts, **kwargs)

    monkeypatch.setattr(tc, "_tokenize_misses", spy)

    expr = tc.cached_tokens_expr(
        pl.col("text"),
        user_id=TEST_USER,
        model="bert-base-uncased",
        lowercase=True,
        remove_punct=True,
    )
    out = cast(pl.DataFrame, base.with_columns(expr.alias("tokens")).collect())

    assert out.height == 3
    flat = [t for batch in seen for t in batch]
    assert flat == ["same"]
