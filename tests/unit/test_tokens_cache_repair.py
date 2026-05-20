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
    REPAIR_SIDECAR_FILENAME,
    TokensCacheRepairReport,
    clear_node_from_sidecar,
    detect_invalid_token_cache_node_ids,
    read_repair_sidecar,
    repair_tokens_cache_paths,
    write_repair_sidecar,
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


# --------------------------------------------------------------------------- #
# Node-ID derivation                                                           #
# --------------------------------------------------------------------------- #


def test_stubbed_node_ids_derived_from_plbin_stem(
    cache_root: Path,
    tmp_path: Path,
) -> None:
    """Docworkspace writes each node's plan as ``data/<node_id>.plbin``,
    so the repair report can recover the node id by taking the plbin stem."""
    donor_path = (
        tmp_path / "donor" / "user_cache" / TOKENS_CACHE_SUBDIR / "model_a_111111111111.parquet"
    )
    _write_tokens_parquet(donor_path)

    workspace_dir = tmp_path / "workspace"
    plbin = workspace_dir / "data" / "node-xyz-7.plbin"
    _make_plbin_referencing(donor_path, plbin)
    _make_workspace(workspace_dir, "data/node-xyz-7.plbin")
    donor_path.unlink()

    report = repair_tokens_cache_paths(workspace_dir, USER_ID)

    assert report.stubbed_node_ids == ["node-xyz-7"]


def test_stubbed_node_ids_relocate_case_does_not_appear(
    cache_root: Path,
    tmp_path: Path,
) -> None:
    """Case A (relocate) is not a 'stubbed' event — the receiver has real
    tokens for the bucket, the user doesn't need to do anything. So the
    node id must NOT appear in stubbed_node_ids."""
    donor_path = (
        tmp_path / "donor" / "user_cache" / TOKENS_CACHE_SUBDIR / "model_b_222222222222.parquet"
    )
    _write_tokens_parquet(donor_path)

    workspace_dir = tmp_path / "workspace"
    plbin = workspace_dir / "data" / "node-relocated.plbin"
    _make_plbin_referencing(donor_path, plbin)
    _make_workspace(workspace_dir, "data/node-relocated.plbin")

    local_path = cache_root / donor_path.name
    _write_tokens_parquet(local_path)
    donor_path.unlink()

    report = repair_tokens_cache_paths(workspace_dir, USER_ID)

    assert report.stubbed_node_ids == []
    assert len(report.relocated) == 1


# --------------------------------------------------------------------------- #
# Sidecar persistence                                                          #
# --------------------------------------------------------------------------- #


def test_sidecar_round_trip(tmp_path: Path) -> None:
    workspace_dir = tmp_path / "ws"
    workspace_dir.mkdir()

    write_repair_sidecar(workspace_dir, ["node-1", "node-2", "node-1"])

    sidecar_file = workspace_dir / REPAIR_SIDECAR_FILENAME
    assert sidecar_file.exists()

    state = read_repair_sidecar(workspace_dir)
    # Dedup + sort, both done by write.
    assert state == {"stubbed_node_ids": ["node-1", "node-2"]}


def test_sidecar_empty_write_removes_existing(tmp_path: Path) -> None:
    workspace_dir = tmp_path / "ws"
    workspace_dir.mkdir()

    write_repair_sidecar(workspace_dir, ["node-1"])
    sidecar_file = workspace_dir / REPAIR_SIDECAR_FILENAME
    assert sidecar_file.exists()

    # An empty-id write means "we just successfully reloaded with no stubs"
    # — the file should be removed so the banner clears.
    write_repair_sidecar(workspace_dir, [])
    assert not sidecar_file.exists()
    assert read_repair_sidecar(workspace_dir) == {}


def test_sidecar_clear_node_removes_one_entry(tmp_path: Path) -> None:
    workspace_dir = tmp_path / "ws"
    workspace_dir.mkdir()
    write_repair_sidecar(workspace_dir, ["a", "b", "c"])

    clear_node_from_sidecar(workspace_dir, "b")
    assert read_repair_sidecar(workspace_dir) == {"stubbed_node_ids": ["a", "c"]}

    clear_node_from_sidecar(workspace_dir, "a")
    clear_node_from_sidecar(workspace_dir, "c")
    # All cleared → file deleted.
    assert not (workspace_dir / REPAIR_SIDECAR_FILENAME).exists()


def test_sidecar_clear_node_is_noop_when_absent(tmp_path: Path) -> None:
    workspace_dir = tmp_path / "ws"
    workspace_dir.mkdir()

    # No sidecar yet — calling clear must not crash and must not create one.
    clear_node_from_sidecar(workspace_dir, "node-1")
    assert not (workspace_dir / REPAIR_SIDECAR_FILENAME).exists()


def test_sidecar_read_tolerates_corrupt_json(tmp_path: Path) -> None:
    workspace_dir = tmp_path / "ws"
    workspace_dir.mkdir()
    (workspace_dir / REPAIR_SIDECAR_FILENAME).write_text("{not valid json")

    # Returns empty dict rather than raising — sidecar is a UX hint, not a
    # load-blocking invariant.
    assert read_repair_sidecar(workspace_dir) == {}


# --------------------------------------------------------------------------- #
# Runtime path-validity detection                                              #
# --------------------------------------------------------------------------- #


class TestDetectInvalidTokenCacheNodeIds:
    """The runtime check that drives the workspace banner. Walks each
    node's plan to flag nodes whose tokens-cache parquets are missing or
    0-row stubs *right now* (vs. the sidecar's snapshot from the last load).
    """

    def test_returns_empty_when_workspace_dir_is_none(self) -> None:
        assert detect_invalid_token_cache_node_ids(None, ["any-node"]) == []

    def test_returns_empty_when_no_plbins(self, tmp_path: Path) -> None:
        workspace_dir = tmp_path / "ws"
        (workspace_dir / "data").mkdir(parents=True)
        assert detect_invalid_token_cache_node_ids(workspace_dir, ["a", "b"]) == []

    def test_flags_node_with_missing_cache_path(
        self, cache_root: Path, tmp_path: Path
    ) -> None:
        donor_path = (
            tmp_path
            / "donor"
            / "user_cache"
            / TOKENS_CACHE_SUBDIR
            / "model_a_aaaaaaaaaaaa.parquet"
        )
        _write_tokens_parquet(donor_path)
        workspace_dir = tmp_path / "workspace"
        plbin = workspace_dir / "data" / "node-missing.plbin"
        _make_plbin_referencing(donor_path, plbin)
        donor_path.unlink()  # path now points to nothing

        result = detect_invalid_token_cache_node_ids(workspace_dir, ["node-missing"])
        assert result == ["node-missing"]

    def test_flags_node_with_zero_row_stub(
        self, cache_root: Path, tmp_path: Path
    ) -> None:
        stub_path = (
            tmp_path / "donor" / "user_cache" / TOKENS_CACHE_SUBDIR / "stub.parquet"
        )
        _write_tokens_parquet(stub_path)  # zero rows by construction
        workspace_dir = tmp_path / "workspace"
        plbin = workspace_dir / "data" / "node-stubbed.plbin"
        _make_plbin_referencing(stub_path, plbin)

        result = detect_invalid_token_cache_node_ids(workspace_dir, ["node-stubbed"])
        assert result == ["node-stubbed"]

    def test_does_not_flag_node_with_populated_cache(
        self, cache_root: Path, tmp_path: Path
    ) -> None:
        real_path = (
            tmp_path / "donor" / "user_cache" / TOKENS_CACHE_SUBDIR / "real.parquet"
        )
        # Populated parquet — one row matching the cache schema. Distinct
        # from the stub which is 0 rows.
        real_path.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(
            {CONTENT_HASH_COLUMN: [1234], "tokens": [[]]},
            schema=TOKENS_CACHE_SCHEMA,
        ).write_parquet(real_path)

        workspace_dir = tmp_path / "workspace"
        plbin = workspace_dir / "data" / "node-healthy.plbin"
        _make_plbin_referencing(real_path, plbin)

        result = detect_invalid_token_cache_node_ids(workspace_dir, ["node-healthy"])
        assert result == []

    def test_does_not_flag_when_stub_coexists_with_real_delta_in_same_bucket(
        self, cache_root: Path, tmp_path: Path
    ) -> None:
        """Reproduces the post-re-tokenise scenario. After a re-tokenise,
        ``tokens_cache_lazyframe`` rebuilds the plan against every file in
        the bucket — so the plbin now references both the original 0-row
        stub AND the freshly-written delta with real tokens. The plan
        materialises real tokens (scan_parquet + unique-by-hash drops the
        stub's 0 rows), so the node is NOT broken. A naive "flag any 0-row
        file" detector would still mark the node and keep the banner
        showing. Bucket-aware detection groups by bucket key and clears
        the flag once any sibling has rows.
        """
        bucket = "model_z_cafebabe1234"
        stub_path = (
            tmp_path
            / "donor"
            / "user_cache"
            / TOKENS_CACHE_SUBDIR
            / f"{bucket}.parquet"
        )
        _write_tokens_parquet(stub_path)  # 0-row stub

        # Sibling delta with one real row, matching the bucket key.
        delta_path = (
            stub_path.parent
            / f"{bucket}__delta__deadbeef.parquet"
        )
        pl.DataFrame(
            {CONTENT_HASH_COLUMN: [9876], "tokens": [[]]},
            schema=TOKENS_CACHE_SCHEMA,
        ).write_parquet(delta_path)

        # The plbin still only references the stub (that's what gets
        # baked into ``scan_parquet([all files])`` at the time the new
        # plan was built; in practice ``tokens_cache_lazyframe`` lists
        # all bucket files but the test stresses the worst case where
        # the plbin path is the empty one).
        workspace_dir = tmp_path / "workspace"
        plbin = workspace_dir / "data" / "node-recovered.plbin"
        _make_plbin_referencing(stub_path, plbin)

        result = detect_invalid_token_cache_node_ids(
            workspace_dir, ["node-recovered"]
        )
        assert result == []

    def test_ignores_non_tokens_cache_paths(
        self, cache_root: Path, tmp_path: Path
    ) -> None:
        # Path outside of TOKENS_CACHE_SUBDIR — the detector deliberately
        # leaves these alone so it doesn't claim ownership over other
        # workspace failure modes (raw data parquets, etc.).
        other_path = tmp_path / "elsewhere" / "not_tokens" / "thing.parquet"
        _write_tokens_parquet(other_path)
        workspace_dir = tmp_path / "workspace"
        plbin = workspace_dir / "data" / "node-other.plbin"
        _make_plbin_referencing(other_path, plbin)
        other_path.unlink()  # missing — but still NOT flagged

        result = detect_invalid_token_cache_node_ids(workspace_dir, ["node-other"])
        assert result == []
