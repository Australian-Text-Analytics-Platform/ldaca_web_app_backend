"""Workspace-load repair pass for stale tokens-cache scan paths.

Background
----------
`tokens_cache.tokens_cache_lazyframe()` builds its result with
`pl.scan_parquet([abs_path, ...])`. Those absolute paths are baked into the
node's serialised lazy plan (``.plbin``). When a workspace is shared across
machines or operating systems, the receiver's filesystem has no such paths,
and the first ``collect()`` on a tokenised node fails.

`docworkspace.workspace.rebase_workspace_sources` only fixes paths whose
basenames travelled inside the workspace's own ``data/`` directory. Tokens
cache files live under ``user_cache/tokens/`` (outside the workspace
bundle), so the existing rebaser ignores them.

This module adds a peer pass that runs after `rebase_workspace_sources`:

  - **Case A** — the basename exists in the current user's tokens cache:
    rewrite the plan to point at the local copy.
  - **Case B** — it doesn't: write a 0-row stub parquet with the canonical
    cache schema at the local equivalent path, then rewrite. The stub means
    the lazy plan deserialises and collects without crashing — joins against
    it simply yield empty token lists. Re-tokenising on the receiver writes
    real delta files into the same bucket and supersedes the stub, because
    the reader does ``pl.scan_parquet([all files in bucket])`` + dedupe.

Non-goals
---------
- Does not carry cache files inside the workspace bundle. Cache portability
  is explicitly out of scope (cache can be gigabytes; it's not part of the
  workspace by design).
- Does not attempt to re-derive tokens at load time. Until the user
  re-tokenises, tokenised analyses run against empty token lists.
- Does not touch any non-tokens-cache missing paths — those are left for the
  existing rebaser / regular error paths to surface.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

import polars as pl

from .tokens_cache import (
    TOKENS_CACHE_SCHEMA,
    TOKENS_CACHE_SUBDIR,
    tokens_cache_dir,
)

logger = logging.getLogger(__name__)


@dataclass
class TokensCacheRepairReport:
    """Per-call summary of what the repair pass did.

    The fields are deliberately path-typed (not just counts) so callers /
    callers' callers can later surface a "these nodes need re-tokenising"
    banner without re-walking the workspace.
    """

    relocated: list[tuple[Path, Path]] = field(default_factory=list)
    """Pairs of (old_path, new_path) where a same-basename cache file was
    found on the receiver and the plan was rewritten to point at it."""

    stubbed: list[Path] = field(default_factory=list)
    """Local stub-parquet paths that the pass had to fabricate because no
    matching cache file existed on the receiver. Nodes whose plans contain
    any of these are functionally "tokens missing — re-tokenise to restore".
    """

    plbins_modified: list[Path] = field(default_factory=list)
    """The set of plbin files whose serialised source paths were rewritten.
    Useful for logging and for tests that want to assert at-most-once IO."""

    @property
    def needed_repair(self) -> bool:
        return bool(self.relocated or self.stubbed)


def _looks_like_tokens_cache_path(p: Path) -> bool:
    """True iff ``p`` looks like a tokens-cache reference.

    The conservative test: one of the path components is ``TOKENS_CACHE_SUBDIR``
    (``"tokens"``). That covers both the production layout
    (``…/user_cache/tokens/<bucket>.parquet``) and the test override layout
    (``$LDACA_TOKENS_CACHE_DIR/<user_id>/tokens/<bucket>.parquet``) without
    misidentifying user-data parquets, which sit under the workspace's own
    ``data/`` directory.
    """
    return TOKENS_CACHE_SUBDIR in p.parts


def _write_stub_parquet(dest: Path) -> None:
    """Write a 0-row parquet at ``dest`` matching ``TOKENS_CACHE_SCHEMA``.

    Joining an input against an empty cache yields NULL/empty token lists for
    every row — i.e. tokenised analyses degrade to empty results rather than
    crashing, which is the contract this repair pass promises callers.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(schema=TOKENS_CACHE_SCHEMA).write_parquet(dest)


def _iter_plbin_paths(workspace_dir: Path) -> Iterable[Path]:
    """Yield every ``.plbin`` referenced by the workspace's ``metadata.json``.

    Pulled here (rather than re-using docworkspace's helpers) so this module
    has zero coupling to docworkspace internals beyond the public
    ``read_workspace_metadata`` and the FFI ``list_source_paths`` /
    ``replace_source_paths`` from polars_text.
    """
    from docworkspace.workspace.io import read_workspace_metadata
    from docworkspace.node.io import NODE_DATA_DIR

    metadata = read_workspace_metadata(workspace_dir)
    data_dir = workspace_dir / NODE_DATA_DIR
    for node_entry in metadata.get("nodes", []):
        data_path = node_entry.get("data_path")
        if not data_path:
            continue
        plbin = (workspace_dir / Path(str(data_path))).resolve()
        # Only feed plbins that actually exist; a missing one means the node
        # is broken at a deeper level and shouldn't be papered over here.
        if plbin.exists():
            yield plbin


def repair_tokens_cache_paths(
    workspace_dir: Path,
    user_id: str,
) -> TokensCacheRepairReport:
    """Rewrite stale tokens-cache scan paths in every node's plbin.

    Idempotent — running it twice produces the same final state. Called once
    on workspace load, immediately after ``rebase_workspace_sources`` and
    before any node plan deserialisation triggers a Polars scan.
    """
    from polars_text import list_source_paths, replace_source_paths

    report = TokensCacheRepairReport()
    target_cache_dir = tokens_cache_dir(user_id)

    for plbin in _iter_plbin_paths(workspace_dir):
        try:
            sources = list_source_paths(plbin)
        except Exception as exc:
            # A corrupt plbin is a separate failure mode — log loudly but
            # don't abort the whole repair (other nodes may still be fine).
            logger.warning(
                "tokens_cache_repair: failed to read sources from %s: %s",
                plbin,
                exc,
            )
            continue

        mapping: dict[str, str] = {}
        for raw in sources:
            old_path = Path(raw)
            if old_path.exists():
                continue
            if not _looks_like_tokens_cache_path(old_path):
                continue

            local_path = (target_cache_dir / old_path.name).resolve()

            if local_path.exists():
                report.relocated.append((old_path, local_path))
            else:
                _write_stub_parquet(local_path)
                report.stubbed.append(local_path)

            new_str = str(local_path)
            if raw != new_str:
                mapping[raw] = new_str

        if mapping:
            try:
                replace_source_paths(plbin, mapping)
                report.plbins_modified.append(plbin)
            except Exception as exc:  # pragma: no cover - defensive
                logger.error(
                    "tokens_cache_repair: failed to rewrite %s: %s", plbin, exc
                )

    if report.needed_repair:
        logger.info(
            "tokens_cache_repair: relocated=%d stubbed=%d plbins_modified=%d",
            len(report.relocated),
            len(report.stubbed),
            len(report.plbins_modified),
        )

    return report


__all__ = [
    "TokensCacheRepairReport",
    "repair_tokens_cache_paths",
]
