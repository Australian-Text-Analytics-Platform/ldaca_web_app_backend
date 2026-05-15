"""Unit tests for the tokens-cache primitives.

The cache is per-user — every fn takes a ``user_id`` first. The
autouse fixture redirects ``LDACA_TOKENS_CACHE_DIR`` at a tmpdir so
each test gets a fresh base and the per-user subdir layout is still
exercised under the override (``{base}/{user_id}/tokens/...``).
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl
import pytest

from ldaca_wordflow.core import tokens_cache as tc

# Single canonical test user; switching to a different one would exercise
# multi-user isolation and is done explicitly in
# ``test_sweep_walks_all_users``.
TEST_USER = "test_user"


@pytest.fixture(autouse=True)
def isolated_cache_root(tmp_path, monkeypatch):
    """Point the env var at a per-test tmpdir so tests don't leak between
    each other and never touch the developer's real ``user_cache``."""
    monkeypatch.setenv(tc.CACHE_ROOT_ENV, str(tmp_path / "tokens-cache"))
    yield tmp_path / "tokens-cache"


def _toy_rows(hash_to_tokens: dict[int, list[dict]]) -> pl.DataFrame:
    """Build a (content-hash, tokens) DataFrame matching the cache schema."""
    return pl.DataFrame(
        {
            tc.CONTENT_HASH_COLUMN: list(hash_to_tokens.keys()),
            "tokens": list(hash_to_tokens.values()),
        },
        schema={
            tc.CONTENT_HASH_COLUMN: pl.UInt64,
            "tokens": pl.List(
                pl.Struct(
                    [
                        pl.Field("token", pl.String),
                        pl.Field("start", pl.Int64),
                        pl.Field("end", pl.Int64),
                    ]
                )
            ),
        },
    )


def _ref(workspace: str, node: str) -> tc.CacheReference:
    return tc.CacheReference(workspace_id=workspace, node_id=node)


# --------------------------------------------------------------------------- #
# Path / filename derivation                                                  #
# --------------------------------------------------------------------------- #


def test_cache_dir_honours_env_with_per_user_subdir(isolated_cache_root):
    """The env var supplies the base; the per-user subdir layout is still
    applied so production multi-user behaviour is exercised under the
    override."""
    got = tc.tokens_cache_dir(TEST_USER)
    expected = isolated_cache_root / TEST_USER / tc.TOKENS_CACHE_SUBDIR
    assert got == expected
    assert got.exists()


def test_cache_dir_isolates_users(isolated_cache_root):
    """Two different users get two different cache directories — the
    privacy contract this refactor was about."""
    a = tc.tokens_cache_dir("alice")
    b = tc.tokens_cache_dir("bob")
    assert a != b
    assert a.parent.parent == b.parent.parent  # same base


def test_cache_filename_stable_across_param_orderings():
    a = tc.cache_filename("jieba", {"lowercase": False, "remove_punct": True})
    b = tc.cache_filename("jieba", {"remove_punct": True, "lowercase": False})
    assert a == b, "param hash must be order-insensitive"


def test_cache_filename_sanitises_model_id():
    name = tc.cache_filename("../../etc/passwd", {})
    assert "/" not in name and "\\" not in name
    assert name.endswith(".parquet")


def test_cache_filename_distinguishes_params():
    a = tc.cache_filename("jieba", {"lowercase": True})
    b = tc.cache_filename("jieba", {"lowercase": False})
    assert a != b


def test_cache_filename_distinguishes_models():
    params = {"lowercase": False}
    assert tc.cache_filename("jieba", params) != tc.cache_filename(
        "lindera-ja-ipadic", params
    )


# --------------------------------------------------------------------------- #
# write / read round-trip                                                      #
# --------------------------------------------------------------------------- #


def test_write_creates_parquet_and_round_trips():
    rows = _toy_rows(
        {
            1: [{"token": "hello", "start": 0, "end": 5}],
            2: [{"token": "world", "start": 0, "end": 5}],
        }
    )
    path = tc.write_or_append_cache(
        TEST_USER, "jieba", {"lowercase": False}, rows
    )
    assert path.exists()

    lf = tc.tokens_cache_lazyframe(TEST_USER, "jieba", {"lowercase": False})
    assert lf is not None
    got = lf.collect().sort(tc.CONTENT_HASH_COLUMN)
    assert got.get_column(tc.CONTENT_HASH_COLUMN).to_list() == [1, 2]


def test_write_isolates_users():
    """Same model/params/hash on two users → two different files; neither
    sees the other's tokens."""
    params = {"lowercase": False}
    rows = _toy_rows({1: [{"token": "alice-tok", "start": 0, "end": 1}]})
    tc.write_or_append_cache("alice", "jieba", params, rows)

    bob_hashes = tc.read_cached_hashes("bob", "jieba", params)
    assert bob_hashes == set()


def test_append_dedups_on_content_hash():
    params = {"lowercase": False}
    first = _toy_rows({1: [{"token": "hello", "start": 0, "end": 5}]})
    tc.write_or_append_cache(TEST_USER, "jieba", params, first)

    # Re-write the same hash with different tokens — first write wins.
    second = _toy_rows({1: [{"token": "OVERWRITTEN", "start": 0, "end": 9}]})
    tc.write_or_append_cache(TEST_USER, "jieba", params, second)

    got = tc.tokens_cache_lazyframe(TEST_USER, "jieba", params).collect()
    assert got.height == 1
    only_tokens = got.get_column("tokens").to_list()[0]
    assert only_tokens[0]["token"] == "hello"


def test_append_extends_existing_cache():
    params = {"lowercase": False}
    tc.write_or_append_cache(
        TEST_USER,
        "jieba",
        params,
        _toy_rows({1: [{"token": "a", "start": 0, "end": 1}]}),
    )
    tc.write_or_append_cache(
        TEST_USER,
        "jieba",
        params,
        _toy_rows({2: [{"token": "b", "start": 0, "end": 1}]}),
    )
    hashes = tc.read_cached_hashes(TEST_USER, "jieba", params)
    assert hashes == {1, 2}


def test_write_or_append_rejects_missing_columns():
    bad = pl.DataFrame({"foo": [1, 2]})
    with pytest.raises(ValueError, match="missing columns"):
        tc.write_or_append_cache(TEST_USER, "jieba", {}, bad)


def test_tokens_cache_lazyframe_returns_none_when_absent():
    assert (
        tc.tokens_cache_lazyframe(TEST_USER, "jieba", {"lowercase": False})
        is None
    )


def test_read_cached_hashes_empty_when_absent():
    assert (
        tc.read_cached_hashes(TEST_USER, "jieba", {"lowercase": False}) == set()
    )


# --------------------------------------------------------------------------- #
# Manifest / references                                                       #
# --------------------------------------------------------------------------- #


def _manifest(user: str) -> dict:
    return json.loads(
        (tc.tokens_cache_dir(user) / tc.MANIFEST_FILENAME).read_text()
    )


def test_add_reference_creates_entry_and_dedups():
    params = {"lowercase": False}
    fname = tc.cache_filename("jieba", params)
    ref = _ref("ws1", "n1")
    tc.add_reference(TEST_USER, fname, ref)
    tc.add_reference(TEST_USER, fname, ref)  # dedup

    refs = _manifest(TEST_USER)["entries"][fname]["references"]
    assert refs == [ref.to_dict()]


def test_drop_reference_removes_one_claim():
    params = {"lowercase": False}
    fname = tc.cache_filename("jieba", params)
    ref_a = _ref("ws1", "n1")
    ref_b = _ref("ws1", "n2")
    tc.add_reference(TEST_USER, fname, ref_a)
    tc.add_reference(TEST_USER, fname, ref_b)

    tc.drop_reference(TEST_USER, fname, ref_a)
    refs = _manifest(TEST_USER)["entries"][fname]["references"]
    assert refs == [ref_b.to_dict()]


def test_drop_node_references_drops_only_that_node():
    fname = tc.cache_filename("jieba", {})
    tc.add_reference(TEST_USER, fname, _ref("ws1", "nA"))
    tc.add_reference(TEST_USER, fname, _ref("ws1", "nB"))
    tc.add_reference(TEST_USER, fname, _ref("ws2", "nA"))

    tc.drop_node_references(TEST_USER, "ws1", "nA")

    refs = _manifest(TEST_USER)["entries"][fname]["references"]
    # ws1/nA gone; ws1/nB and ws2/nA survive.
    assert {(r["workspace_id"], r["node_id"]) for r in refs} == {
        ("ws1", "nB"),
        ("ws2", "nA"),
    }


def test_drop_workspace_references_drops_only_that_workspace():
    fname = tc.cache_filename("jieba", {})
    tc.add_reference(TEST_USER, fname, _ref("wsA", "n1"))
    tc.add_reference(TEST_USER, fname, _ref("wsB", "n2"))
    tc.add_reference(TEST_USER, fname, _ref("wsA", "n3"))

    tc.drop_workspace_references(TEST_USER, "wsA")

    refs = _manifest(TEST_USER)["entries"][fname]["references"]
    # wsA refs gone; wsB survives.
    assert {(r["workspace_id"], r["node_id"]) for r in refs} == {
        ("wsB", "n2"),
    }


def test_drop_reference_is_idempotent_for_unknown_file():
    # Must not raise — node-delete path may run against a cache that
    # was already swept.
    tc.drop_reference(TEST_USER, "nonexistent.parquet", _ref("w", "n"))


# --------------------------------------------------------------------------- #
# Sweep                                                                       #
# --------------------------------------------------------------------------- #


def _write_minimal_cache(
    user: str, model: str, params: dict, hash_value: int
) -> Path:
    return tc.write_or_append_cache(
        user,
        model,
        params,
        _toy_rows({hash_value: [{"token": "x", "start": 0, "end": 1}]}),
    )


def test_sweep_keeps_referenced_files():
    params = {"lowercase": False}
    path = _write_minimal_cache(TEST_USER, "jieba", params, 1)
    tc.add_reference(TEST_USER, path.name, _ref("w", "n"))

    removed = tc.sweep_unreferenced(
        TEST_USER, now=datetime.now(timezone.utc) + timedelta(days=30)
    )
    assert removed == {TEST_USER: []}
    assert path.exists()


def test_sweep_keeps_unreferenced_files_inside_grace_period():
    params = {"lowercase": False}
    path = _write_minimal_cache(TEST_USER, "jieba", params, 1)
    removed = tc.sweep_unreferenced(TEST_USER)
    assert removed == {TEST_USER: []}
    assert path.exists()


def test_sweep_removes_unreferenced_files_past_grace_period():
    params = {"lowercase": False}
    path = _write_minimal_cache(TEST_USER, "jieba", params, 1)
    far_future = datetime.now(timezone.utc) + timedelta(days=30)
    removed = tc.sweep_unreferenced(
        TEST_USER, grace_period_days=7, now=far_future
    )
    assert removed == {TEST_USER: [path.name]}
    assert not path.exists()


def test_sweep_reaps_orphan_parquets_past_grace():
    # File on disk without any manifest entry — possible if a tokenise
    # write succeeded but the manifest update crashed.
    cache_dir = tc.tokens_cache_dir(TEST_USER)
    orphan = cache_dir / "ghost__abc.parquet"
    pl.DataFrame({"x": [1]}).write_parquet(orphan)
    old = (datetime.now(timezone.utc) - timedelta(days=30)).timestamp()
    import os as _os

    _os.utime(orphan, (old, old))

    removed = tc.sweep_unreferenced(TEST_USER, grace_period_days=7)
    assert orphan.name in removed[TEST_USER]
    assert not orphan.exists()


def test_sweep_cleans_manifest_entries_for_vanished_files():
    fname = tc.cache_filename("jieba", {})
    tc.add_reference(TEST_USER, fname, _ref("w", "n"))  # creates entry
    tc.drop_reference(TEST_USER, fname, _ref("w", "n"))  # vanished file

    tc.sweep_unreferenced(TEST_USER)
    assert fname not in _manifest(TEST_USER)["entries"]


def test_touch_access_resets_grace_window():
    params = {"lowercase": False}
    path = _write_minimal_cache(TEST_USER, "jieba", params, 1)
    # Manually backdate, then touch.
    manifest_path = tc.tokens_cache_dir(TEST_USER) / tc.MANIFEST_FILENAME
    manifest = json.loads(manifest_path.read_text())
    manifest["entries"][path.name]["last_accessed_at"] = (
        (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
    )
    manifest_path.write_text(json.dumps(manifest))

    tc.touch_access(TEST_USER, path.name)

    removed = tc.sweep_unreferenced(TEST_USER, grace_period_days=7)
    assert path.exists()
    assert removed == {TEST_USER: []}


def test_sweep_walks_all_users_when_user_id_omitted():
    """The startup-hook path: ``sweep_unreferenced()`` without a user
    must iterate every user that has a cache directory on disk."""
    params = {"lowercase": False}
    # Two users, both with unreferenced caches eligible for eviction.
    pa = _write_minimal_cache("alice", "jieba", params, 1)
    pb = _write_minimal_cache("bob", "jieba", params, 1)

    far_future = datetime.now(timezone.utc) + timedelta(days=30)
    removed = tc.sweep_unreferenced(grace_period_days=7, now=far_future)

    assert set(removed.keys()) >= {"alice", "bob"}
    assert pa.name in removed["alice"]
    assert pb.name in removed["bob"]
    assert not pa.exists()
    assert not pb.exists()


# --------------------------------------------------------------------------- #
# tokenise_column integration                                                 #
# --------------------------------------------------------------------------- #


def test_tokenise_column_slice_collect_returns_correct_tokens(
    isolated_cache_root,
):
    """Pins the slice-correctness invariant from Fix #6 of the PLAN.

    Polars 1.40 doesn't push the slice past the LEFT JOIN to the cache
    parquet (the join still reads the full file), but the hash match
    keeps row identity correct so a sliced collect must return tokens
    matching the corresponding prefix of the full collect.
    """
    from docworkspace import Node

    from ldaca_wordflow.api.workspaces.analyses.generated_columns import (
        TOKENS_FORM,
        derived_column_name,
    )
    from ldaca_wordflow.core.derived_columns import tokenise_column

    df = pl.DataFrame(
        {
            "text": [f"document number {i}" for i in range(20)],
            "value": list(range(20)),
        }
    ).lazy()
    node = Node(data=df, name="probe")

    tokenise_column(
        node,
        source_column="text",
        model="bert-base-uncased",
        language="en",
        user_id=TEST_USER,
    )
    derived_name = derived_column_name(
        TOKENS_FORM, "text", "bert-base-uncased"
    )

    full = node.data.collect()
    page = node.data.slice(0, 5).collect()

    assert page.height == 5
    assert derived_name in page.columns
    # The first 5 rows of the slice must equal the first 5 rows of the
    # full collect — tokens included.
    full_first_five = full.head(5)
    assert page.equals(full_first_five)
