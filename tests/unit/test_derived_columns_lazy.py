"""Phase 2 — `tokenise_column` with `LDACA_LAZY_TOKENISE=1` skips the
eager upsert and stamps the node's plan with a
`polars_text.tokenize_with_cache_lookup` expression instead.

The integration contract this exercises:

* No parquet is written at tokenise time (it's lazy — cache fills on
  first collect).
* The first `.collect()` populates the cache; the same plan re-collected
  produces identical output and adds no new delta files.
* The serialised lazy plan (.plbin equivalent) carries no absolute
  paths — that's the central portability promise of the lazy refactor.
* The flag-off (default) path still works exactly the same as before.

Lower-level invariants of the Rust expression itself (per-row miss
delta layout, dedup ordering, flock) are covered by
`polars-text/tests/test_tokenize_with_cache_lookup.py` and the inline
Rust unit tests in `src/tokens_cache_io.rs::tests`.

See: backend/docs/developer-guide/lazy-tokenisation-refactor.md.
"""

from __future__ import annotations

import os
from pathlib import Path

import polars as pl
import pytest
from docworkspace import Node

from ldaca_wordflow.api.workspaces.analyses.generated_columns import (
    TOKENS_FORM,
    derived_column_name,
)
from ldaca_wordflow.core import tokens_cache
from ldaca_wordflow.core.derived_columns import LAZY_TOKENISE_ENV, tokenise_column


def _make_node(name: str = "root") -> Node:
    df = pl.DataFrame(
        {
            "text": ["Hello world", "Goodbye world", "Hello again"],
            "value": [1, 2, 3],
        }
    ).lazy()
    return Node(data=df, name=name)


@pytest.fixture
def lazy_flag_on(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(LAZY_TOKENISE_ENV, "1")


def _bucket_files(user_id: str, model: str, params: dict) -> list[Path]:
    """Files belonging to the (user, model, params) bucket — used by tests
    to assert what's been written without going through the Rust API."""
    bucket = tokens_cache.cache_filename(model, params)
    dir_ = tokens_cache.tokens_cache_dir(user_id)
    stem = bucket.removesuffix(".parquet")
    files: list[Path] = []
    legacy = dir_ / f"{stem}.parquet"
    if legacy.exists():
        files.append(legacy)
    files.extend(dir_.glob(f"{stem}__delta__*.parquet"))
    return sorted(files)


def test_lazy_flag_off_uses_eager_path() -> None:
    # Sanity check — no flag set, behaviour matches the pre-Phase-2 path.
    node = _make_node()
    name = tokenise_column(
        node,
        source_column="text",
        model="bert-base-uncased",
        language="en",
        user_id="flag_off_user",
    )
    assert name in node.derived

    # Eager path writes a parquet immediately.
    params = {"lowercase": True, "remove_punct": True}
    files = _bucket_files("flag_off_user", "bert-base-uncased", params)
    assert len(files) >= 1, f"eager path should have written a parquet, got {files}"


def test_lazy_flag_on_writes_no_parquet_at_tokenise_time(
    lazy_flag_on: None,
) -> None:
    node = _make_node()
    tokenise_column(
        node,
        source_column="text",
        model="bert-base-uncased",
        language="en",
        user_id="lazy_user_a",
    )
    params = {"lowercase": True, "remove_punct": True}
    files = _bucket_files("lazy_user_a", "bert-base-uncased", params)
    assert files == [], (
        f"lazy path should write NO parquet at tokenise time, got {files}"
    )


def test_lazy_first_collect_populates_cache(lazy_flag_on: None) -> None:
    node = _make_node()
    derived_name = tokenise_column(
        node,
        source_column="text",
        model="bert-base-uncased",
        language="en",
        user_id="lazy_user_b",
    )
    params = {"lowercase": True, "remove_punct": True}

    # No cache yet — but a .collect() should fill it
    assert _bucket_files("lazy_user_b", "bert-base-uncased", params) == []
    result = node.data.collect()
    files_after = _bucket_files("lazy_user_b", "bert-base-uncased", params)
    assert len(files_after) == 1, (
        f"first collect should have written one delta file, got {files_after}"
    )

    # Output schema matches the contract — `List<Struct<token, start, end>>`
    dtype = result.schema[derived_name]
    assert isinstance(dtype, pl.List)
    inner = dtype.inner
    assert isinstance(inner, pl.Struct)
    assert {f.name for f in inner.fields} == {"token", "start", "end"}


def test_lazy_second_collect_is_full_cache_hit(lazy_flag_on: None) -> None:
    node = _make_node()
    tokenise_column(
        node,
        source_column="text",
        model="bert-base-uncased",
        language="en",
        user_id="lazy_user_c",
    )
    params = {"lowercase": True, "remove_punct": True}

    out1 = node.data.collect()
    files1 = _bucket_files("lazy_user_c", "bert-base-uncased", params)
    assert len(files1) == 1

    out2 = node.data.collect()
    files2 = _bucket_files("lazy_user_c", "bert-base-uncased", params)
    assert files2 == files1, "no new delta on full-hit collect"
    assert out1.equals(out2), "second collect must reproduce the first"


def test_lazy_plan_serialises_without_absolute_paths(lazy_flag_on: None) -> None:
    # The central portability promise: the serialised plan must not carry
    # any absolute parquet path. Verify by introspecting the serialised
    # binary for any reference to the cache dir base.
    node = _make_node()
    tokenise_column(
        node,
        source_column="text",
        model="bert-base-uncased",
        language="en",
        user_id="lazy_user_d",
    )
    blob = node.data.serialize(format="binary")
    # The Phase 1 wire-up uses `LDACA_TOKENS_CACHE_DIR` for runtime
    # resolution — that path must NOT appear in the serialised plan.
    cache_base = os.environ.get("LDACA_TOKENS_CACHE_DIR", "")
    assert cache_base, "test fixture must set LDACA_TOKENS_CACHE_DIR"
    assert cache_base.encode() not in blob, (
        "serialised plan should not bake the cache base path — that's "
        "what eager hash-join plans did and what makes them non-portable"
    )
    # `bucket_filename` (the stable hash-of-(model, params) name) SHOULD
    # appear since it's a kwarg of the expression — that's fine, it's
    # the same on every machine.
    bucket_filename = tokens_cache.cache_filename(
        "bert-base-uncased", {"lowercase": True, "remove_punct": True}
    )
    assert bucket_filename.encode() in blob, (
        "bucket filename should be in the serialised plan as an expression kwarg"
    )


def test_lazy_per_user_isolation(lazy_flag_on: None) -> None:
    # Two users, same model+params, same source text — must produce two
    # separate cache files under their respective subdirs.
    n1 = _make_node()
    n2 = _make_node()
    tokenise_column(
        n1,
        source_column="text",
        model="bert-base-uncased",
        language="en",
        user_id="user_one_iso",
    )
    tokenise_column(
        n2,
        source_column="text",
        model="bert-base-uncased",
        language="en",
        user_id="user_two_iso",
    )
    n1.data.collect()
    n2.data.collect()
    params = {"lowercase": True, "remove_punct": True}
    f1 = _bucket_files("user_one_iso", "bert-base-uncased", params)
    f2 = _bucket_files("user_two_iso", "bert-base-uncased", params)
    assert len(f1) == 1 and len(f2) == 1
    assert f1[0].parent != f2[0].parent, "per-user subdirs should not collide"
