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
    tc.write_or_append_cache(
        TEST_USER, "jieba", {"lowercase": False}, rows
    )
    # After the delta refactor the canonical ``<bucket>.parquet`` is
    # *not* created — writes land in ``<bucket>__delta__*.parquet``
    # siblings. Use ``cache_exists`` to assert the bucket holds at
    # least one file.
    assert tc.cache_exists(TEST_USER, "jieba", {"lowercase": False})

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
    """Earliest-write-wins on content-hash collisions.

    In the delta refactor the writer always writes a fresh delta; the
    *reader* drops duplicates via ``.unique(keep="first")`` against
    files ordered by mtime ascending. So the chronologically earliest
    token for any given hash survives — same semantic as the old
    read-merge-replace path that explicitly filtered new rows whose
    hashes were already present.
    """
    import time as _time

    params = {"lowercase": False}
    first = _toy_rows({1: [{"token": "hello", "start": 0, "end": 5}]})
    tc.write_or_append_cache(TEST_USER, "jieba", params, first)

    # Ensure the second delta file lands strictly *later* than the first
    # on the filesystem clock. APFS gives sub-millisecond mtime
    # resolution; the sleep is overkill but documents the intent.
    _time.sleep(0.01)

    # Re-write the same hash with different tokens — earliest still wins.
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


def _bucket_for(model: str, params: dict) -> str:
    """Manifest entries are keyed by bucket (no ``.parquet`` suffix)
    post-refactor — these tests pass legacy filenames into the API
    (which still works thanks to the in-helper normalisation) but read
    back from the manifest by bucket key."""
    return tc.cache_filename(model, params)[: -len(".parquet")]


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
    # ws1/nA gone; ws1/nB and ws2/nA survive.
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
    _write_minimal_cache(TEST_USER, "jieba", params, 1)
    fname = tc.cache_filename("jieba", params)
    tc.add_reference(TEST_USER, fname, _ref("w", "n"))

    removed = tc.sweep_unreferenced(
        TEST_USER, now=datetime.now(timezone.utc) + timedelta(days=30)
    )
    assert removed == {TEST_USER: []}
    assert tc.cache_exists(TEST_USER, "jieba", params)


def test_sweep_keeps_unreferenced_files_inside_grace_period():
    params = {"lowercase": False}
    _write_minimal_cache(TEST_USER, "jieba", params, 1)
    removed = tc.sweep_unreferenced(TEST_USER)
    assert removed == {TEST_USER: []}
    assert tc.cache_exists(TEST_USER, "jieba", params)


def test_sweep_removes_unreferenced_files_past_grace_period():
    params = {"lowercase": False}
    _write_minimal_cache(TEST_USER, "jieba", params, 1)
    far_future = datetime.now(timezone.utc) + timedelta(days=30)
    removed = tc.sweep_unreferenced(
        TEST_USER, grace_period_days=7, now=far_future
    )
    # The actual file name is ``<bucket>__delta__<uuid>.parquet`` —
    # sweep returns the file names it removed, so we just assert at
    # least one file was reaped for this user.
    assert removed[TEST_USER], "sweep should have removed at least one file"
    assert not tc.cache_exists(TEST_USER, "jieba", params)


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
    bucket = _bucket_for("jieba", {})
    tc.add_reference(TEST_USER, fname, _ref("w", "n"))  # creates entry
    tc.drop_reference(TEST_USER, fname, _ref("w", "n"))  # vanished file

    tc.sweep_unreferenced(TEST_USER)
    assert bucket not in _manifest(TEST_USER)["entries"]


def test_touch_access_resets_grace_window():
    params = {"lowercase": False}
    _write_minimal_cache(TEST_USER, "jieba", params, 1)
    fname = tc.cache_filename("jieba", params)
    bucket = _bucket_for("jieba", params)
    # Manually backdate, then touch.
    manifest_path = tc.tokens_cache_dir(TEST_USER) / tc.MANIFEST_FILENAME
    manifest = json.loads(manifest_path.read_text())
    manifest["entries"][bucket]["last_accessed_at"] = (
        (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
    )
    manifest_path.write_text(json.dumps(manifest))

    tc.touch_access(TEST_USER, fname)

    removed = tc.sweep_unreferenced(TEST_USER, grace_period_days=7)
    assert tc.cache_exists(TEST_USER, "jieba", params)
    assert removed == {TEST_USER: []}


def test_sweep_walks_all_users_when_user_id_omitted():
    """The startup-hook path: ``sweep_unreferenced()`` without a user
    must iterate every user that has a cache directory on disk."""
    params = {"lowercase": False}
    # Two users, both with unreferenced caches eligible for eviction.
    _write_minimal_cache("alice", "jieba", params, 1)
    _write_minimal_cache("bob", "jieba", params, 1)

    far_future = datetime.now(timezone.utc) + timedelta(days=30)
    removed = tc.sweep_unreferenced(grace_period_days=7, now=far_future)

    assert set(removed.keys()) >= {"alice", "bob"}
    assert removed["alice"], "alice's cache should have been swept"
    assert removed["bob"], "bob's cache should have been swept"
    assert not tc.cache_exists("alice", "jieba", params)
    assert not tc.cache_exists("bob", "jieba", params)


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


# --------------------------------------------------------------------------- #
# Delta-files behaviour                                                       #
# --------------------------------------------------------------------------- #


def test_writes_create_separate_delta_files():
    """Each ``write_or_append_cache`` call must produce a fresh delta
    rather than rewrite the same file — that's the core property the
    refactor depends on for race-free concurrent writers."""
    params = {"lowercase": False}
    tc.write_or_append_cache(
        TEST_USER, "jieba", params,
        _toy_rows({1: [{"token": "a", "start": 0, "end": 1}]}),
    )
    tc.write_or_append_cache(
        TEST_USER, "jieba", params,
        _toy_rows({2: [{"token": "b", "start": 0, "end": 1}]}),
    )
    bucket = _bucket_for("jieba", params)
    files = tc._bucket_files(TEST_USER, bucket)
    assert len(files) == 2
    for p in files:
        assert tc.DELTA_INFIX in p.name
        assert p.name.startswith(bucket)


def test_concurrent_duplicate_hash_collapses_to_one_row_on_read():
    """Two writers concurrently writing the same hash to different
    deltas — the read-side ``.unique()`` makes them indistinguishable
    from one row, so downstream joins never produce duplicate output."""
    params = {"lowercase": False}
    tc.write_or_append_cache(
        TEST_USER, "jieba", params,
        _toy_rows({42: [{"token": "first", "start": 0, "end": 5}]}),
    )
    # No mtime sleep — the two delta files may even land in the same
    # microsecond, exercising the lexicographic tiebreak in
    # ``_bucket_files``.
    tc.write_or_append_cache(
        TEST_USER, "jieba", params,
        _toy_rows({42: [{"token": "second", "start": 0, "end": 6}]}),
    )
    got = tc.tokens_cache_lazyframe(TEST_USER, "jieba", params).collect()
    assert got.height == 1


def test_compaction_collapses_many_deltas_into_one():
    """After enough deltas accumulate the writer trips opportunistic
    compaction, replacing N small files with one merged file."""
    params = {"lowercase": False}
    threshold = tc.DEFAULT_COMPACTION_THRESHOLD
    # Write threshold+2 rows, one per delta — first ``threshold`` land
    # as individual deltas; subsequent writes trigger compaction.
    for i in range(threshold + 2):
        tc.write_or_append_cache(
            TEST_USER, "jieba", params,
            _toy_rows({i: [{"token": f"t{i}", "start": 0, "end": 1}]}),
        )
    bucket = _bucket_for("jieba", params)
    files = tc._bucket_files(TEST_USER, bucket)
    # After compaction we should be back to a small number of files —
    # the merged delta plus at most one not-yet-compacted write.
    assert len(files) < threshold + 2
    # All original hashes must still be readable.
    hashes = tc.read_cached_hashes(TEST_USER, "jieba", params)
    assert hashes == set(range(threshold + 2))


def test_legacy_single_file_still_readable_alongside_deltas():
    """A pre-refactor bucket may have a ``<bucket>.parquet`` plus
    new ``<bucket>__delta__*.parquet`` siblings. Both must contribute
    rows to the cache view."""
    params = {"lowercase": False}
    bucket = _bucket_for("jieba", params)
    cache_dir = tc.tokens_cache_dir(TEST_USER)
    # Hand-craft a legacy single-file cache with hash 100.
    legacy = cache_dir / f"{bucket}.parquet"
    _toy_rows({100: [{"token": "legacy", "start": 0, "end": 6}]}).write_parquet(
        legacy
    )
    # Then add a delta with hash 200 via the public API.
    tc.write_or_append_cache(
        TEST_USER, "jieba", params,
        _toy_rows({200: [{"token": "delta", "start": 0, "end": 5}]}),
    )
    hashes = tc.read_cached_hashes(TEST_USER, "jieba", params)
    assert hashes == {100, 200}


def test_sweep_removes_legacy_and_delta_files_together():
    """When a bucket's references drop, sweep must reap the legacy
    single-file AND every delta — leaving one behind would leak disk."""
    params = {"lowercase": False}
    bucket = _bucket_for("jieba", params)
    cache_dir = tc.tokens_cache_dir(TEST_USER)
    # Create one legacy + one delta the public way + manifest entry.
    legacy = cache_dir / f"{bucket}.parquet"
    _toy_rows({1: [{"token": "x", "start": 0, "end": 1}]}).write_parquet(legacy)
    tc.write_or_append_cache(
        TEST_USER, "jieba", params,
        _toy_rows({2: [{"token": "y", "start": 0, "end": 1}]}),
    )
    assert len(tc._bucket_files(TEST_USER, bucket)) == 2

    far_future = datetime.now(timezone.utc) + timedelta(days=30)
    tc.sweep_unreferenced(TEST_USER, grace_period_days=7, now=far_future)

    assert tc._bucket_files(TEST_USER, bucket) == []
    assert not legacy.exists()
