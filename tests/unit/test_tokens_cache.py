"""Unit tests for the DuckDB-backed token cache."""

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
def isolated_cache_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(tc.CACHE_ROOT_ENV, str(tmp_path / "tokens-cache"))
    return tmp_path / "tokens-cache"


def test_tokens_duckdb_path_is_user_specific(isolated_cache_root: Path) -> None:
    alice_path = tc.tokens_cache_path("alice")
    bob_path = tc.tokens_cache_path("bob")

    assert alice_path == isolated_cache_root / "alice" / "tokens.duckdb"
    assert bob_path == isolated_cache_root / "bob" / "tokens.duckdb"
    assert alice_path != bob_path


def test_open_tokens_cache_creates_missing_duckdb_file_with_schema() -> None:
    path = tc.tokens_cache_path(TEST_USER)
    assert not path.exists()

    tc.ensure_tokens_cache(TEST_USER)

    assert path.exists()
    with duckdb.connect(str(path), read_only=True) as conn:
        columns = conn.execute("DESCRIBE token_cache").fetchall()

    column_names = [row[0] for row in columns]
    assert column_names[:5] == [
        "model",
        "params_hash",
        "content_hash",
        "tokens",
        "input_ids",
    ]
    assert "start_offsets" in column_names
    assert "end_offsets" in column_names
    assert "pos_tags" in column_names


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
        user_id=TEST_USER,
        workspace_id="workspace",
    )

    assert derived_name == derived_column_name(TOKENS_FORM, "text", "bert-base-uncased")
    assert derived_name in node.derived
    assert derived_name not in node.data.collect_schema().names()
    derived_meta = cast(dict[str, Any], node.derived[derived_name])
    assert derived_meta["cache_backend"] == "duckdb"


def test_hydrate_tokens_lazyframe_temporarily_adds_tokens_column() -> None:
    node = Node(
        data=pl.DataFrame({"text": ["hello world", "hello again"]}).lazy(),
        name="probe",
    )
    derived_name = tokenise_column(
        node,
        source_column="text",
        model="bert-base-uncased",
        language="en",
        user_id=TEST_USER,
    )

    hydrated = tc.hydrate_tokens_lazyframe(
        node.data,
        source_column="text",
        model="bert-base-uncased",
        params=cast(dict[str, Any], node.derived[derived_name])["params"],
        user_id=TEST_USER,
        derived_name=derived_name,
    )
    hydrated_df = cast(pl.DataFrame, hydrated.collect())

    assert derived_name in hydrated_df.columns
    assert derived_name not in node.data.collect_schema().names()
    first_tokens = hydrated_df.to_dicts()[0][derived_name]
    assert isinstance(first_tokens, list)
    assert first_tokens[0]["token"]


def test_hydrate_tokens_uses_cache_on_warm_run(monkeypatch: pytest.MonkeyPatch) -> None:
    node_data = pl.DataFrame({"text": ["hello world"]}).lazy()
    params = {"lowercase": True, "remove_punct": True}

    tc.hydrate_tokens_lazyframe(
        node_data,
        source_column="text",
        model="bert-base-uncased",
        params=params,
        user_id=TEST_USER,
        derived_name="tokens",
    ).collect()

    def fail_tokenize(*args, **kwargs):
        raise AssertionError("warm cache run should not tokenize missing rows")

    monkeypatch.setattr(tc, "_tokenize_source_rows", fail_tokenize)

    warm_df = cast(
        pl.DataFrame,
        tc.hydrate_tokens_lazyframe(
            node_data,
            source_column="text",
            model="bert-base-uncased",
            params=params,
            user_id=TEST_USER,
            derived_name="tokens",
        ).collect(),
    )

    assert warm_df.height == 1
    assert warm_df.to_dicts()[0]["tokens"][0]["token"]
