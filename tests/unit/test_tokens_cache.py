"""Unit tests for the tokens cache primitives.

These tests stub out the cache root via the ``LDACA_TOKENS_CACHE_DIR``
env variable so they touch only tmp directories — never the user's
real ``~/.ldaca/tokens-cache`` even when run locally.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl
import pytest

from ldaca_wordflow.core import tokens_cache as tc


@pytest.fixture(autouse=True)
def isolated_cache_dir(tmp_path, monkeypatch):
    """Each test gets a clean cache dir under ``tmp_path``."""
    cache_dir = tmp_path / "tokens-cache"
    monkeypatch.setenv(tc.CACHE_ROOT_ENV, str(cache_dir))
    yield cache_dir


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


# --------------------------------------------------------------------------- #
# Path / filename derivation                                                  #
# --------------------------------------------------------------------------- #


def test_cache_dir_honours_env(isolated_cache_dir):
    assert tc.tokens_cache_dir() == isolated_cache_dir
    assert isolated_cache_dir.exists()


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
    path = tc.write_or_append_cache("jieba", {"lowercase": False}, rows)
    assert path.exists()

    lf = tc.tokens_cache_lazyframe("jieba", {"lowercase": False})
    assert lf is not None
    got = lf.collect().sort(tc.CONTENT_HASH_COLUMN)
    assert got.get_column(tc.CONTENT_HASH_COLUMN).to_list() == [1, 2]


def test_append_dedups_on_content_hash():
    params = {"lowercase": False}
    first = _toy_rows({1: [{"token": "hello", "start": 0, "end": 5}]})
    tc.write_or_append_cache("jieba", params, first)

    # Re-write the same hash with different tokens — first write should
    # win. (Both writes correspond to the same source content, so the
    # tokens *should* be identical; we test the dedup contract rather
    # than the equivalence of those tokens.)
    second = _toy_rows({1: [{"token": "OVERWRITTEN", "start": 0, "end": 9}]})
    tc.write_or_append_cache("jieba", params, second)

    got = tc.tokens_cache_lazyframe("jieba", params).collect()
    assert got.height == 1
    only_tokens = got.get_column("tokens").to_list()[0]
    assert only_tokens[0]["token"] == "hello"


def test_append_extends_existing_cache():
    params = {"lowercase": False}
    tc.write_or_append_cache(
        "jieba", params, _toy_rows({1: [{"token": "a", "start": 0, "end": 1}]})
    )
    tc.write_or_append_cache(
        "jieba", params, _toy_rows({2: [{"token": "b", "start": 0, "end": 1}]})
    )
    hashes = tc.read_cached_hashes("jieba", params)
    assert hashes == {1, 2}


def test_write_or_append_rejects_missing_columns():
    bad = pl.DataFrame({"foo": [1, 2]})
    with pytest.raises(ValueError, match="missing columns"):
        tc.write_or_append_cache("jieba", {}, bad)


def test_tokens_cache_lazyframe_returns_none_when_absent():
    assert tc.tokens_cache_lazyframe("jieba", {"lowercase": False}) is None


def test_read_cached_hashes_empty_when_absent():
    assert tc.read_cached_hashes("jieba", {"lowercase": False}) == set()


# --------------------------------------------------------------------------- #
# Manifest / references                                                       #
# --------------------------------------------------------------------------- #


def test_add_reference_creates_entry_and_dedups():
    params = {"lowercase": False}
    fname = tc.cache_filename("jieba", params)
    # The manifest entry is created on the first add even without a
    # cache file — write paths set the size, the reference path only
    # needs the entry's references list.
    ref = tc.CacheReference(user_id="u1", workspace_id="ws1", node_id="n1")
    tc.add_reference(fname, ref)
    tc.add_reference(fname, ref)  # dedup

    manifest = json.loads(
        (tc.tokens_cache_dir() / tc.MANIFEST_FILENAME).read_text()
    )
    refs = manifest["entries"][fname]["references"]
    assert refs == [ref.to_dict()]


def test_drop_reference_removes_one_claim():
    params = {"lowercase": False}
    fname = tc.cache_filename("jieba", params)
    ref_a = tc.CacheReference("u1", "ws1", "n1")
    ref_b = tc.CacheReference("u1", "ws1", "n2")
    tc.add_reference(fname, ref_a)
    tc.add_reference(fname, ref_b)

    tc.drop_reference(fname, ref_a)
    manifest = json.loads(
        (tc.tokens_cache_dir() / tc.MANIFEST_FILENAME).read_text()
    )
    refs = manifest["entries"][fname]["references"]
    assert refs == [ref_b.to_dict()]


def test_drop_workspace_references_drops_only_that_workspace():
    fname = tc.cache_filename("jieba", {})
    tc.add_reference(fname, tc.CacheReference("u1", "wsA", "n1"))
    tc.add_reference(fname, tc.CacheReference("u1", "wsB", "n2"))
    tc.add_reference(fname, tc.CacheReference("u2", "wsA", "n3"))

    tc.drop_workspace_references("u1", "wsA")

    manifest = json.loads(
        (tc.tokens_cache_dir() / tc.MANIFEST_FILENAME).read_text()
    )
    refs = manifest["entries"][fname]["references"]
    # u1/wsA gone; u1/wsB and u2/wsA survive.
    assert {(r["user_id"], r["workspace_id"]) for r in refs} == {
        ("u1", "wsB"),
        ("u2", "wsA"),
    }


def test_drop_reference_is_idempotent_for_unknown_file():
    # Must not raise — node-delete path may run against a cache that
    # was already swept.
    tc.drop_reference("nonexistent.parquet", tc.CacheReference("u", "w", "n"))


# --------------------------------------------------------------------------- #
# Sweep                                                                       #
# --------------------------------------------------------------------------- #


def _write_minimal_cache(model: str, params: dict, hash_value: int) -> Path:
    return tc.write_or_append_cache(
        model,
        params,
        _toy_rows({hash_value: [{"token": "x", "start": 0, "end": 1}]}),
    )


def test_sweep_keeps_referenced_files():
    params = {"lowercase": False}
    path = _write_minimal_cache("jieba", params, 1)
    tc.add_reference(path.name, tc.CacheReference("u", "w", "n"))

    removed = tc.sweep_unreferenced(now=datetime.now(timezone.utc) + timedelta(days=30))
    assert removed == []
    assert path.exists()


def test_sweep_keeps_unreferenced_files_inside_grace_period():
    params = {"lowercase": False}
    path = _write_minimal_cache("jieba", params, 1)
    # No add_reference; just-written so within the default 7-day grace.
    removed = tc.sweep_unreferenced()
    assert removed == []
    assert path.exists()


def test_sweep_removes_unreferenced_files_past_grace_period():
    params = {"lowercase": False}
    path = _write_minimal_cache("jieba", params, 1)
    # Simulate elapsed time by jumping ``now`` forward past the grace
    # period; ``last_accessed_at`` in the manifest was set at write
    # time, so this triggers eviction.
    far_future = datetime.now(timezone.utc) + timedelta(days=30)
    removed = tc.sweep_unreferenced(grace_period_days=7, now=far_future)
    assert removed == [path.name]
    assert not path.exists()


def test_sweep_reaps_orphan_parquets_past_grace():
    # File on disk without any manifest entry — possible if a tokenise
    # write succeeded but the manifest update crashed.
    cache_dir = tc.tokens_cache_dir()
    orphan = cache_dir / "ghost__abc.parquet"
    pl.DataFrame({"x": [1]}).write_parquet(orphan)
    # Backdate mtime past the grace period.
    old = (datetime.now(timezone.utc) - timedelta(days=30)).timestamp()
    import os as _os

    _os.utime(orphan, (old, old))

    removed = tc.sweep_unreferenced(grace_period_days=7)
    assert orphan.name in removed
    assert not orphan.exists()


def test_sweep_cleans_manifest_entries_for_vanished_files():
    fname = tc.cache_filename("jieba", {})
    tc.add_reference(fname, tc.CacheReference("u", "w", "n"))  # creates entry
    # Drop the reference so the entry has no claims; but never wrote
    # the parquet → vanished file.
    tc.drop_reference(fname, tc.CacheReference("u", "w", "n"))

    tc.sweep_unreferenced()
    manifest = json.loads(
        (tc.tokens_cache_dir() / tc.MANIFEST_FILENAME).read_text()
    )
    assert fname not in manifest["entries"]


def test_touch_access_resets_grace_window():
    params = {"lowercase": False}
    path = _write_minimal_cache("jieba", params, 1)
    # Simulate the file aging out by manually backdating its
    # last_accessed_at, then touch it.
    manifest_path = tc.tokens_cache_dir() / tc.MANIFEST_FILENAME
    manifest = json.loads(manifest_path.read_text())
    manifest["entries"][path.name]["last_accessed_at"] = (
        (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
    )
    manifest_path.write_text(json.dumps(manifest))

    tc.touch_access(path.name)

    removed = tc.sweep_unreferenced(grace_period_days=7)
    assert path.exists()
    assert removed == []
