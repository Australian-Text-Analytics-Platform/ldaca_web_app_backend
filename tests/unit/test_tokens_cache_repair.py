"""Tests for ldaca_wordflow.core.tokens_cache_repair.

Pins four scenarios that ``repair_tokens_cache_paths`` has to handle:

  - **Case A — Relocate:** the donor's absolute path is missing on the
    receiver, but a same-basename file exists in the receiver's local cache
    dir. The plbin must be rewritten to point at the local file; no stub.
  - **Case B — Stub:** the donor's path is missing AND no same-basename file
    exists locally. The repair must fabricate a 0-row parquet with the
    canonical cache schema at the local equivalent, and rewrite the plbin.
  - **No-op:** the donor's path still exists (same machine reload). Repair
    is a no-op.
  - **Conservative scope:** a non-tokens-cache missing path (e.g. somewhere
    outside the ``tokens/`` directory) is left alone for upstream code to
    surface — this module's contract is "fix tokens cache only".
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from ldaca_wordflow.core.tokens_cache import (
    CONTENT_HASH_COLUMN,
    TOKENS_CACHE_SCHEMA,
    TOKENS_CACHE_SUBDIR,
    tokens_cache_dir,
)
from ldaca_wordflow.core.tokens_cache_repair import (
    TokensCacheRepairReport,
    repair_tokens_cache_paths,
)


USER_ID = "test_user"


@pytest.fixture
def cache_root(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Override the per-user tokens-cache root to a tmp dir for the test."""
    root = tmp_path / "receiver_cache"
    root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("LDACA_TOKENS_CACHE_DIR", str(root))
    return tokens_cache_dir(USER_ID)


def _write_tokens_parquet(path: Path) -> None:
    """Write a real (but tiny) parquet matching the cache schema at ``path``.

    Used to build the donor's serialised lazy plan and to stage a
    same-basename receiver file for Case A.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(schema=TOKENS_CACHE_SCHEMA).write_parquet(path)


def _make_plbin_referencing(donor_parquet: Path, plbin_path: Path) -> None:
    """Build a serialised LazyFrame whose only source is ``donor_parquet``."""
    plbin_path.parent.mkdir(parents=True, exist_ok=True)
    lf = pl.scan_parquet(donor_parquet)
    lf.serialize(plbin_path, format="binary")


def _make_workspace(
    workspace_dir: Path,
    plbin_relative_path: str,
) -> None:
    """Write a minimal metadata.json + node entry pointing at ``plbin_relative_path``."""
    workspace_dir.mkdir(parents=True, exist_ok=True)
    metadata = {
        "workspace_metadata": {
            "id": "ws-test",
            "name": "ws-test",
            "version": 2,
        },
        "nodes": [
            {
                "id": "node-1",
                "data_path": plbin_relative_path,
            }
        ],
    }
    (workspace_dir / "metadata.json").write_text(json.dumps(metadata))


def _plbin_source_paths(plbin: Path) -> list[str]:
    from polars_text import list_source_paths

    return list_source_paths(plbin)


# --------------------------------------------------------------------------- #
# Case A — Relocate                                                            #
# --------------------------------------------------------------------------- #


def test_case_a_relocates_to_receiver_cache_when_basename_exists(
    cache_root: Path,
    tmp_path: Path,
) -> None:
    # Donor wrote a tokens cache parquet on their machine.
    donor_path = tmp_path / "donor" / "user_cache" / TOKENS_CACHE_SUBDIR / "bert_a1b2c3d4e5f6.parquet"
    _write_tokens_parquet(donor_path)

    # Workspace plbin references that donor path.
    workspace_dir = tmp_path / "workspace"
    plbin = workspace_dir / "data" / "node-1.plbin"
    _make_plbin_referencing(donor_path, plbin)
    _make_workspace(workspace_dir, "data/node-1.plbin")

    # Receiver staged a same-basename cache file locally (e.g. they had
    # previously tokenised with the same model+params).
    local_path = cache_root / donor_path.name
    _write_tokens_parquet(local_path)

    # Simulate transfer: the donor's path is gone on the receiver.
    donor_path.unlink()

    report = repair_tokens_cache_paths(workspace_dir, USER_ID)

    assert len(report.relocated) == 1
    old, new = report.relocated[0]
    assert new == local_path.resolve()
    assert old.name == donor_path.name
    assert report.stubbed == []
    assert report.plbins_modified == [plbin]

    # The plbin must now reference the receiver's local copy, not the donor's path.
    sources = _plbin_source_paths(plbin)
    assert sources == [str(local_path.resolve())]


# --------------------------------------------------------------------------- #
# Case B — Stub                                                                #
# --------------------------------------------------------------------------- #


def test_case_b_stubs_local_parquet_when_no_match_anywhere(
    cache_root: Path,
    tmp_path: Path,
) -> None:
    donor_path = tmp_path / "donor" / "user_cache" / TOKENS_CACHE_SUBDIR / "xlm_999888777666.parquet"
    _write_tokens_parquet(donor_path)

    workspace_dir = tmp_path / "workspace"
    plbin = workspace_dir / "data" / "node-1.plbin"
    _make_plbin_referencing(donor_path, plbin)
    _make_workspace(workspace_dir, "data/node-1.plbin")

    # Receiver has NO same-basename cache file — first time seeing this bucket.
    donor_path.unlink()
    expected_stub = cache_root / donor_path.name
    assert not expected_stub.exists()

    report = repair_tokens_cache_paths(workspace_dir, USER_ID)

    assert report.relocated == []
    assert len(report.stubbed) == 1
    assert report.stubbed[0] == expected_stub.resolve()
    assert report.plbins_modified == [plbin]

    # The stub must exist, be readable, and have the canonical cache schema
    # (so the join downstream can run without crashing).
    assert expected_stub.exists()
    stub_df = pl.read_parquet(expected_stub)
    assert stub_df.height == 0
    assert stub_df.schema == TOKENS_CACHE_SCHEMA

    # The plbin must reference the stub.
    sources = _plbin_source_paths(plbin)
    assert sources == [str(expected_stub.resolve())]


def test_case_b_lazy_collect_against_stub_yields_empty(
    cache_root: Path,
    tmp_path: Path,
) -> None:
    """The repair pass promises that loading no longer crashes — verify by
    actually deserialising and collecting the post-repair plbin."""
    donor_path = (
        tmp_path / "donor" / "user_cache" / TOKENS_CACHE_SUBDIR / "model_x_1234567890ab.parquet"
    )
    _write_tokens_parquet(donor_path)

    workspace_dir = tmp_path / "workspace"
    plbin = workspace_dir / "data" / "node-1.plbin"
    _make_plbin_referencing(donor_path, plbin)
    _make_workspace(workspace_dir, "data/node-1.plbin")

    donor_path.unlink()
    repair_tokens_cache_paths(workspace_dir, USER_ID)

    # Without repair, this collect() would raise FileNotFoundError on the
    # donor's absolute path. With repair → the stub means it returns empty.
    lf = pl.LazyFrame.deserialize(plbin, format="binary")
    result = lf.collect()
    assert result.height == 0
    assert set(result.columns) == {CONTENT_HASH_COLUMN, "tokens"}


# --------------------------------------------------------------------------- #
# No-op                                                                        #
# --------------------------------------------------------------------------- #


def test_no_op_when_source_paths_still_exist(
    cache_root: Path,
    tmp_path: Path,
) -> None:
    donor_path = tmp_path / "donor" / "user_cache" / TOKENS_CACHE_SUBDIR / "still_here_aabbccddeeff.parquet"
    _write_tokens_parquet(donor_path)

    workspace_dir = tmp_path / "workspace"
    plbin = workspace_dir / "data" / "node-1.plbin"
    _make_plbin_referencing(donor_path, plbin)
    _make_workspace(workspace_dir, "data/node-1.plbin")

    # Path still exists — repair must be a no-op.
    report = repair_tokens_cache_paths(workspace_dir, USER_ID)

    assert report.relocated == []
    assert report.stubbed == []
    assert report.plbins_modified == []
    assert report.needed_repair is False
    sources = _plbin_source_paths(plbin)
    assert sources == [str(donor_path.resolve())]


# --------------------------------------------------------------------------- #
# Conservative scope                                                           #
# --------------------------------------------------------------------------- #


def test_leaves_non_tokens_cache_missing_paths_alone(
    cache_root: Path,
    tmp_path: Path,
) -> None:
    # A missing parquet that is NOT under a `tokens/` directory — could be a
    # raw user-data file, an analysis cache, anything. Outside this module's
    # scope; leave it for the existing rebaser / regular error paths.
    missing_path = tmp_path / "donor" / "user_cache" / "embeddings" / "some_other.parquet"
    _write_tokens_parquet(missing_path)

    workspace_dir = tmp_path / "workspace"
    plbin = workspace_dir / "data" / "node-1.plbin"
    _make_plbin_referencing(missing_path, plbin)
    _make_workspace(workspace_dir, "data/node-1.plbin")

    missing_path.unlink()

    report = repair_tokens_cache_paths(workspace_dir, USER_ID)

    assert report.relocated == []
    assert report.stubbed == []
    assert report.plbins_modified == []
    # The plbin still references the donor path verbatim.
    sources = _plbin_source_paths(plbin)
    assert sources == [str(missing_path.resolve())]


# --------------------------------------------------------------------------- #
# Idempotence                                                                  #
# --------------------------------------------------------------------------- #


def test_repair_is_idempotent(
    cache_root: Path,
    tmp_path: Path,
) -> None:
    donor_path = tmp_path / "donor" / "user_cache" / TOKENS_CACHE_SUBDIR / "bucket_idem_ffffffffffff.parquet"
    _write_tokens_parquet(donor_path)

    workspace_dir = tmp_path / "workspace"
    plbin = workspace_dir / "data" / "node-1.plbin"
    _make_plbin_referencing(donor_path, plbin)
    _make_workspace(workspace_dir, "data/node-1.plbin")
    donor_path.unlink()

    first = repair_tokens_cache_paths(workspace_dir, USER_ID)
    assert first.stubbed != []
    sources_after_first = _plbin_source_paths(plbin)

    second = repair_tokens_cache_paths(workspace_dir, USER_ID)
    # Second pass sees the now-local stub and treats it as Case A
    # (relocate-no-op): the path already resolves locally, so no rewrite
    # is needed at all.
    assert second.relocated == []
    assert second.stubbed == []
    assert second.plbins_modified == []
    sources_after_second = _plbin_source_paths(plbin)
    assert sources_after_first == sources_after_second


# --------------------------------------------------------------------------- #
# Report dataclass                                                             #
# --------------------------------------------------------------------------- #


def test_report_needed_repair_property() -> None:
    assert TokensCacheRepairReport().needed_repair is False
    assert TokensCacheRepairReport(relocated=[(Path("/a"), Path("/b"))]).needed_repair is True
    assert TokensCacheRepairReport(stubbed=[Path("/c")]).needed_repair is True
