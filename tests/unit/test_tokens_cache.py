"""Unit tests for the tokens-cache primitives.

The Python module owns the manifest (reference tracking, sweep,
compaction) and the bucket-filename derivation; the Rust
``polars_text.tokenize_with_cache_lookup`` expression owns the
per-row parquet writes during lazy collect.

For tests that need cache files on disk *without* going through a full
lazy-expression collect (e.g. sweep / compaction tests), the
``_write_delta_directly`` helper writes a `<bucket>__delta__<uuid>.parquet`
file in the same shape the Rust expression produces. This keeps the
test surface independent of the Rust build and lets us pin the
Python-side invariants (manifest, sweep, compaction) in isolation.

The autouse fixture points ``LDACA_TOKENS_CACHE_DIR`` at a tmpdir so
each test gets a fresh base and the per-user subdir layout is still
exercised under the override (``{base}/{user_id}/tokens/...``).
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl
import pytest

from ldaca_wordflow.core import tokens_cache as tc

# Single canonical test user; switching to a different one would exercise
# multi-user isolation and is done explicitly in
# ``test_sweep_walks_all_users_when_user_id_omitted``.
TEST_USER = "test_user"


@pytest.fixture(autouse=True)
def isolated_cache_root(tmp_path, monkeypatch):
    """Point the env var at a per-test tmpdir so tests don't leak between
    each other and never touch the developer's real cache."""
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


def _write_delta_directly(user_id: str, model: str, params: dict, rows: pl.DataFrame) -> Path:
    """Drop a `<bucket>__delta__<uuid>.parquet` file in the bucket dir
    exactly as the Rust lazy expression would. Used by tests that need
    files on disk for sweep / compaction checks without invoking the
    full lazy collect."""
    cache_dir = tc.tokens_cache_dir(user_id)
    bucket = tc.cache_filename(model, params).removesuffix(".parquet")
    delta_path = cache_dir / f"{bucket}{tc.DELTA_INFIX}{uuid.uuid4().hex}.parquet"
    rows.write_parquet(delta_path)
    return delta_path


def _bucket_files_count(user_id: str, model: str, params: dict) -> int:
    bucket = tc.cache_filename(model, params).removesuffix(".parquet")
    return len(tc._bucket_files(user_id, bucket))


def _ref(workspace: str, node: str) -> tc.CacheReference:
    return tc.CacheReference(workspace_id=workspace, node_id=node)


# --------------------------------------------------------------------------- #
# Path / filename derivation                                                  #
# --------------------------------------------------------------------------- #


def test_cache_dir_honours_env_with_per_user_subdir(isolated_cache_root):
    got = tc.tokens_cache_dir(TEST_USER)
    assert got == isolated_cache_root / TEST_USER / tc.TOKENS_CACHE_SUBDIR
    assert got.exists()


def test_cache_dir_isolates_users(isolated_cache_root):
    a = tc.tokens_cache_dir("alice")
    b = tc.tokens_cache_dir("bob")
    assert a != b
    # Layout: {env}/{user_id}/{TOKENS_CACHE_SUBDIR} — so the COMMON
    # ancestor is the env root, two levels up.
    assert a.parent.parent == b.parent.parent
    assert a.parent.parent == isolated_cache_root


def test_cache_dir_raises_when_env_missing(monkeypatch):
    """tokens_cache_dir requires the env var to be set; the backend
    lifespan sets a default before any request reaches the module."""
    monkeypatch.delenv(tc.CACHE_ROOT_ENV, raising=False)
    with pytest.raises(RuntimeError, match=tc.CACHE_ROOT_ENV):
        tc.tokens_cache_dir("anyone")


def test_cache_filename_stable_across_param_orderings():
    assert tc.cache_filename("jieba", {"a": 1, "b": 2}) == tc.cache_filename(
        "jieba", {"b": 2, "a": 1}
    )


def test_cache_filename_sanitises_model_id():
    # `../etc/passwd` must not produce a filename that contains a
    # path separator — the model id is sanitised to a flat string. The
    # dots in `..` are allowed in the safe-char whitelist (so versioned
    # model ids like "bert-base-uncased.v2" round-trip), so we only
    # check the slash here.
    fname = tc.cache_filename("../etc/passwd", {})
    assert "/" not in fname


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
# Manifest / references                                                       #
# --------------------------------------------------------------------------- #


def _manifest(user: str) -> dict:
    return json.loads(
        (tc.tokens_cache_dir(user) / tc.MANIFEST_FILENAME).read_text()
    )


def _bucket_for(model: str, params: dict) -> str:
    return tc.cache_filename(model, params).removesuffix(".parquet")


def test_add_reference_creates_entry_and_dedups():
    params = {"lowercase": False}
    fname = tc.cache_filename("jieba", params)
    bucket = _bucket_for("jieba", params)
    ref = _ref("ws1", "n1")
    tc.add_reference(TEST_USER, fname, ref)
    tc.add_reference(TEST_USER, fname, ref)  # dedup

    refs = _manifest(TEST_USER)["entries"][bucket]["references"]
    assert refs == [ref.to_dict()]


def test_drop_reference_removes_one_claim():
    params = {"lowercase": False}
    fname = tc.cache_filename("jieba", params)
    bucket = _bucket_for("jieba", params)
    ref_a = _ref("ws1", "n1")
    ref_b = _ref("ws1", "n2")
    tc.add_reference(TEST_USER, fname, ref_a)
    tc.add_reference(TEST_USER, fname, ref_b)

    tc.drop_reference(TEST_USER, fname, ref_a)
    refs = _manifest(TEST_USER)["entries"][bucket]["references"]
    assert refs == [ref_b.to_dict()]


def test_drop_node_references_drops_only_that_node():
    fname = tc.cache_filename("jieba", {})
    bucket = _bucket_for("jieba", {})
    tc.add_reference(TEST_USER, fname, _ref("ws1", "nA"))
    tc.add_reference(TEST_USER, fname, _ref("ws1", "nB"))
    tc.add_reference(TEST_USER, fname, _ref("ws2", "nA"))

    tc.drop_node_references(TEST_USER, "ws1", "nA")

    refs = _manifest(TEST_USER)["entries"][bucket]["references"]
    assert {(r["workspace_id"], r["node_id"]) for r in refs} == {
        ("ws1", "nB"),
        ("ws2", "nA"),
    }


def test_drop_workspace_references_drops_only_that_workspace():
    fname = tc.cache_filename("jieba", {})
    bucket = _bucket_for("jieba", {})
    tc.add_reference(TEST_USER, fname, _ref("wsA", "n1"))
    tc.add_reference(TEST_USER, fname, _ref("wsB", "n2"))
    tc.add_reference(TEST_USER, fname, _ref("wsA", "n3"))

    tc.drop_workspace_references(TEST_USER, "wsA")

    refs = _manifest(TEST_USER)["entries"][bucket]["references"]
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


def _seed_bucket(user: str, model: str, params: dict, hash_value: int) -> Path:
    """Write one delta + register a manifest entry — the minimal state
    the sweep tests need."""
    delta = _write_delta_directly(
        user, model, params,
        _toy_rows({hash_value: [{"token": "x", "start": 0, "end": 1}]}),
    )
    # `add_reference` materialises the manifest entry (with a fresh
    # last_accessed_at) before we add the reference; this exercises the
    # same path tokenise_column takes.
    return delta


def test_sweep_keeps_referenced_files():
    params = {"lowercase": False}
    _seed_bucket(TEST_USER, "jieba", params, 1)
    fname = tc.cache_filename("jieba", params)
    tc.add_reference(TEST_USER, fname, _ref("w", "n"))

    removed = tc.sweep_unreferenced(
        TEST_USER, now=datetime.now(timezone.utc) + timedelta(days=30)
    )
    assert removed == {TEST_USER: []}
    assert _bucket_files_count(TEST_USER, "jieba", params) > 0


def test_sweep_keeps_unreferenced_files_inside_grace_period():
    params = {"lowercase": False}
    _seed_bucket(TEST_USER, "jieba", params, 1)
    # Register so the manifest carries a last_accessed_at; drop right
    # away so the bucket has 0 refs but is "fresh".
    fname = tc.cache_filename("jieba", params)
    tc.add_reference(TEST_USER, fname, _ref("w", "n"))
    tc.drop_reference(TEST_USER, fname, _ref("w", "n"))

    removed = tc.sweep_unreferenced(TEST_USER)
    assert removed == {TEST_USER: []}
    assert _bucket_files_count(TEST_USER, "jieba", params) > 0


def test_sweep_removes_unreferenced_files_past_grace_period():
    params = {"lowercase": False}
    _seed_bucket(TEST_USER, "jieba", params, 1)
    fname = tc.cache_filename("jieba", params)
    tc.add_reference(TEST_USER, fname, _ref("w", "n"))
    tc.drop_reference(TEST_USER, fname, _ref("w", "n"))

    far_future = datetime.now(timezone.utc) + timedelta(days=30)
    removed = tc.sweep_unreferenced(
        TEST_USER, grace_period_days=7, now=far_future
    )
    assert removed[TEST_USER], "sweep should have removed at least one file"
    assert _bucket_files_count(TEST_USER, "jieba", params) == 0


def test_sweep_reaps_orphan_parquets_past_grace():
    # File on disk without any manifest entry — possible if a write
    # crashed between the parquet write and the manifest update.
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
    bucket = _bucket_for("jieba", {})
    tc.add_reference(TEST_USER, fname, _ref("w", "n"))  # creates entry
    tc.drop_reference(TEST_USER, fname, _ref("w", "n"))  # vanished file

    tc.sweep_unreferenced(TEST_USER)
    assert bucket not in _manifest(TEST_USER)["entries"]


def test_sweep_walks_all_users_when_user_id_omitted():
    """The startup-hook path: ``sweep_unreferenced()`` without a user
    must iterate every user that has a cache directory on disk."""
    params = {"lowercase": False}
    # Two users, both with unreferenced caches eligible for eviction.
    _seed_bucket("alice", "jieba", params, 1)
    _seed_bucket("bob", "jieba", params, 1)

    far_future = datetime.now(timezone.utc) + timedelta(days=30)
    removed = tc.sweep_unreferenced(grace_period_days=7, now=far_future)

    assert set(removed.keys()) >= {"alice", "bob"}
    assert removed["alice"], "alice's cache should have been swept"
    assert removed["bob"], "bob's cache should have been swept"
    assert _bucket_files_count("alice", "jieba", params) == 0
    assert _bucket_files_count("bob", "jieba", params) == 0


# --------------------------------------------------------------------------- #
# Compaction                                                                  #
# --------------------------------------------------------------------------- #


def test_compaction_is_noop_below_threshold():
    """compact_bucket_if_needed reports False and leaves files alone
    when the delta count is at-or-below the threshold."""
    params = {"lowercase": False}
    threshold = tc.DEFAULT_COMPACTION_THRESHOLD
    for i in range(threshold):
        _write_delta_directly(
            TEST_USER, "jieba", params,
            _toy_rows({i: [{"token": f"t{i}", "start": 0, "end": 1}]}),
        )
    assert _bucket_files_count(TEST_USER, "jieba", params) == threshold
    merged = tc.compact_bucket_if_needed(TEST_USER, "jieba", params)
    assert merged is False
    # Files unchanged.
    assert _bucket_files_count(TEST_USER, "jieba", params) == threshold


def test_compaction_collapses_many_deltas_into_one():
    """Above threshold, compaction merges every file in the bucket into
    one fresh delta and deletes the originals."""
    params = {"lowercase": False}
    threshold = tc.DEFAULT_COMPACTION_THRESHOLD
    # Write threshold + 5 individual deltas
    for i in range(threshold + 5):
        _write_delta_directly(
            TEST_USER, "jieba", params,
            _toy_rows({i: [{"token": f"t{i}", "start": 0, "end": 1}]}),
        )
    assert _bucket_files_count(TEST_USER, "jieba", params) == threshold + 5

    merged = tc.compact_bucket_if_needed(TEST_USER, "jieba", params)
    assert merged is True
    # One merged delta remains.
    assert _bucket_files_count(TEST_USER, "jieba", params) == 1
    # All original hashes still present.
    bucket = _bucket_for("jieba", params)
    file = tc._bucket_files(TEST_USER, bucket)[0]
    rows = pl.read_parquet(file).sort(tc.CONTENT_HASH_COLUMN)
    assert rows.get_column(tc.CONTENT_HASH_COLUMN).to_list() == list(
        range(threshold + 5)
    )


def test_compact_all_buckets_walks_all_users_and_buckets():
    """compact_all_buckets() with no user_id is the startup-hook entry
    point — it must visit every user dir and every bucket within."""
    params_jieba = {"lowercase": False}
    params_lindera = {"lowercase": False, "remove_punct": True}
    threshold = tc.DEFAULT_COMPACTION_THRESHOLD

    # User A: two buckets, only one over threshold.
    for i in range(threshold + 3):
        _write_delta_directly(
            "alice", "jieba", params_jieba,
            _toy_rows({i: [{"token": str(i), "start": 0, "end": 1}]}),
        )
    for i in range(threshold - 1):
        _write_delta_directly(
            "alice", "lindera-ja-ipadic", params_lindera,
            _toy_rows({i: [{"token": str(i), "start": 0, "end": 1}]}),
        )
    # User B: one over-threshold bucket.
    for i in range(threshold + 1):
        _write_delta_directly(
            "bob", "jieba", params_jieba,
            _toy_rows({i: [{"token": str(i), "start": 0, "end": 1}]}),
        )

    counts = tc.compact_all_buckets()
    # Exactly the two over-threshold buckets get compacted.
    assert counts["alice"] == 1
    assert counts["bob"] == 1
    # alice/jieba and bob/jieba collapsed; alice/lindera left alone.
    assert _bucket_files_count("alice", "jieba", params_jieba) == 1
    assert (
        _bucket_files_count("alice", "lindera-ja-ipadic", params_lindera)
        == threshold - 1
    )
    assert _bucket_files_count("bob", "jieba", params_jieba) == 1


def test_compact_all_buckets_scoped_to_one_user():
    """compact_all_buckets(user_id) only touches that user's buckets."""
    params = {"lowercase": False}
    threshold = tc.DEFAULT_COMPACTION_THRESHOLD
    for u in ("alice", "bob"):
        for i in range(threshold + 2):
            _write_delta_directly(
                u, "jieba", params,
                _toy_rows({i: [{"token": str(i), "start": 0, "end": 1}]}),
            )

    counts = tc.compact_all_buckets("alice")
    assert counts == {"alice": 1}
    assert _bucket_files_count("alice", "jieba", params) == 1
    # bob untouched
    assert _bucket_files_count("bob", "jieba", params) == threshold + 2


# --------------------------------------------------------------------------- #
# tokenise_column integration                                                 #
# --------------------------------------------------------------------------- #


def test_tokenise_column_slice_collect_returns_correct_tokens(
    isolated_cache_root,
):
    """End-to-end: the lazy `tokenize_with_cache_lookup` expression
    keeps row identity under a slice — slice(0, 5).collect() returns
    the same first five rows as full().collect()."""
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
    full_first_five = full.head(5)
    assert page.equals(full_first_five)
