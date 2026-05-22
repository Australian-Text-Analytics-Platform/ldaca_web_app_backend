"""Lifecycle management for analysis side-effect parquet caches.

Background workers write per-task cache parquets to speed up subsequent
operations on the same hits/quotes (e.g. dispersion bin endpoints, fast-path
detach, fast-path re-materialise). The files live at:

    <workspace_dir>/data/artifacts/materialized_<feature>_<task_id>_<node_id>.parquet

For ``concordance_materialize`` and ``quotation_materialize`` tasks the
embedded ``task_id`` is the **child worker task ID** (the materialize task
itself). Side-effect materializations follow the same rule: the embedded id
is the worker task that produced the file.

**Why ``data/artifacts/`` and not ``data/``**: the docworkspace garbage
collector at ``workspace.save()`` time iterates the top of ``data/`` with
``iterdir()`` (non-recursive) and deletes any ``.parquet`` not referenced by
a registered node plan. Files inside ``data/artifacts/`` are a directory
entry, so the GC skips them entirely.

This module is the single source of truth for cache naming and cache-file
matching:

  * the canonical cache filename (``materialized_cache_path``) — workers MUST
    use this helper so cleanup globs find their files.
    * task-lifecycle cleanup (``cleanup_task_caches``,
        ``cleanup_workspace_caches``).

Multi-user safety: every cleanup is scoped by ``(user_id, workspace_id)``.
The workspace path is resolved through the trusted
``workspace_manager.get_workspace_dir`` which never escapes the user's folder.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)

# Canonical filename: materialized_<feature>_<task_id>_<node_id>.parquet
# - feature: lowercase tool key (concordance, quotation, ...)
# - task_id: UUID4 string with dashes (no underscores)
# - node_id: opaque identifier; may contain dashes or letters but is not
#   constrained to UUID4. The regex is correspondingly permissive on the tail.
_CACHE_FILENAME_RE = re.compile(
    r"^materialized_"
    r"(?P<feature>[a-z][a-z_]*)_"
    r"(?P<task_id>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})_"
    r"(?P<node_id>.+)"
    r"\.parquet$"
)


def materialized_cache_path(
    workspace_dir: str | Path,
    feature: str,
    task_id: str,
    node_id: str,
) -> Path:
    """Return the canonical cache file path. Does not create dirs or files."""
    return (
        Path(workspace_dir)
        / "data"
        / "artifacts"
        / f"materialized_{feature}_{task_id}_{node_id}.parquet"
    )


def _cache_dir(user_id: str, workspace_id: str) -> Path | None:
    """Resolve ``<workspace_dir>/data/artifacts`` for ``(user_id, workspace_id)``.

    Returns ``None`` when the workspace can't be located, which makes every
    public cleanup function a safe no-op for unloaded or deleted workspaces.
    """
    from .workspace import workspace_manager

    workspace_dir = workspace_manager.get_workspace_dir(user_id, workspace_id)
    if workspace_dir is None:
        return None
    cache_dir = workspace_dir / "data" / "artifacts"
    if not cache_dir.exists() or not cache_dir.is_dir():
        return None
    return cache_dir


def _unlink_quiet(path: Path) -> bool:
    try:
        path.unlink(missing_ok=True)
        return True
    except OSError as exc:
        logger.warning("Failed to unlink analysis cache %s: %s", path, exc)
        return False


def cleanup_task_caches(user_id: str, workspace_id: str, task_id: str) -> int:
    """Delete every cache parquet owned by ``task_id`` in the given workspace.

    Idempotent. Returns the number of files unlinked. Filename matching uses
    the canonical regex (not a raw glob), so a node_id that happens to embed
    a UUID-shaped substring can't cause a false positive.
    """
    if not task_id:
        return 0
    cache_dir = _cache_dir(user_id, workspace_id)
    if cache_dir is None:
        return 0

    count = 0
    for path in cache_dir.glob("materialized_*.parquet"):
        m = _CACHE_FILENAME_RE.match(path.name)
        if m is None:
            continue
        if m.group("task_id") != task_id:
            continue
        if _unlink_quiet(path):
            count += 1
    return count


def cleanup_workspace_caches(user_id: str, workspace_id: str) -> int:
    """Delete every analysis cache parquet in a workspace's data dir.

    Used on workspace unload. Returns number of files unlinked.
    """
    cache_dir = _cache_dir(user_id, workspace_id)
    if cache_dir is None:
        return 0

    count = 0
    for path in cache_dir.glob("materialized_*.parquet"):
        if _CACHE_FILENAME_RE.match(path.name) is None:
            continue  # ignore unrelated files humans may have dropped here
        if _unlink_quiet(path):
            count += 1
    return count


__all__ = [
    "materialized_cache_path",
    "cleanup_task_caches",
    "cleanup_workspace_caches",
]
