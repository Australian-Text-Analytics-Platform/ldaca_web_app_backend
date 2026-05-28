"""Task-owned artifact cleanup.

Analysis and worker tasks may write transient files below a workspace's
``data/artifacts`` directory. Clearing the task should reclaim those files,
while files promoted into ``data`` by Add to Workspace remain node-owned and
are preserved by design.

Used by:
- Backend API routes, worker tasks, workspace services, and backend tests because they
  need a backend boundary that validates inputs before delegating to workspace or worker
  state.

Flow: derive owned artifact paths, walk serialized task payloads for references, remove
    only workspace-owned files, and tolerate missing paths during cleanup.
"""

from __future__ import annotations

import logging
import shutil
from pathlib import Path
from typing import Any, Iterable, Iterator

from .analysis_cache import cleanup_task_caches

logger = logging.getLogger(__name__)

_PATH_SUFFIXES = ("_path", "_parquet_path")
_PATH_COLLECTION_SUFFIXES = ("_paths", "_parquet_paths")


def _workspace_artifact_root(user_id: str, workspace_id: str) -> Path | None:
    """Support task artifact ownership cleanup with a workspace artifact root helper.

    Called by:
    - Local helpers, route handlers, or service methods in this module because they need a
      backend boundary that validates inputs before delegating to workspace or worker state.

    Flow: derive owned artifact paths, walk serialized task payloads for references, remove
        only workspace-owned files, and tolerate missing paths during cleanup.
    """

    from .workspace import workspace_manager

    workspace_dir = workspace_manager.get_workspace_dir(user_id, workspace_id)
    if workspace_dir is None:
        return None
    return workspace_dir / "data" / "artifacts"


def _iter_string_values(node: Any) -> Iterator[str]:
    """Support task artifact ownership cleanup with an iter string values helper.

    Called by:
    - Local helpers, route handlers, or service methods in this module because they need a
      backend boundary that validates inputs before delegating to workspace or worker state.

    Flow: derive owned artifact paths, walk serialized task payloads for references, remove
        only workspace-owned files, and tolerate missing paths during cleanup.
    """

    if isinstance(node, str):
        yield node
    elif isinstance(node, dict):
        for value in node.values():
            yield from _iter_string_values(value)
    elif isinstance(node, (list, tuple, set)):
        for item in node:
            yield from _iter_string_values(item)


def _iter_artifact_paths(node: Any) -> Iterator[str]:
    """Support task artifact ownership cleanup with an iter artifact paths helper.

    Called by:
    - Local helpers, route handlers, or service methods in this module because they need a
      backend boundary that validates inputs before delegating to workspace or worker state.

    Flow: derive owned artifact paths, walk serialized task payloads for references, remove
        only workspace-owned files, and tolerate missing paths during cleanup.
    """

    if isinstance(node, dict):
        for key, value in node.items():
            if not isinstance(key, str):
                yield from _iter_artifact_paths(value)
                continue

            if key.endswith(_PATH_SUFFIXES) and isinstance(value, str):
                yield value
            elif key.endswith(_PATH_COLLECTION_SUFFIXES):
                yield from _iter_string_values(value)
            else:
                yield from _iter_artifact_paths(value)
    elif isinstance(node, (list, tuple, set)):
        for item in node:
            yield from _iter_artifact_paths(item)


def _is_within(path: Path, root: Path) -> bool:
    """Check whether within applies for task artifact ownership cleanup.

    Called by:
    - Local helpers, route handlers, or service methods in this module because they need a
      backend boundary that validates inputs before delegating to workspace or worker state.

    Flow: derive owned artifact paths, walk serialized task payloads for references, remove
        only workspace-owned files, and tolerate missing paths during cleanup.
    """

    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _delete_owned_path(raw_path: str, artifact_root: Path) -> int:
    """Delete owned path resources used by task artifact ownership cleanup.

    Called by:
    - Local helpers, route handlers, or service methods in this module because they need a
      backend boundary that validates inputs before delegating to workspace or worker state.

    Flow: derive owned artifact paths, walk serialized task payloads for references, remove
        only workspace-owned files, and tolerate missing paths during cleanup.
    """

    if not raw_path:
        return 0

    path = Path(raw_path)
    if not path.is_absolute():
        return 0

    resolved_root = artifact_root.resolve(strict=False)
    resolved_path = path.resolve(strict=False)
    if resolved_path == resolved_root or not _is_within(resolved_path, resolved_root):
        return 0

    try:
        if path.is_dir() and not path.is_symlink():
            shutil.rmtree(path)
            return 1
        if path.exists() or path.is_symlink():
            path.unlink()
            return 1
    except OSError as exc:
        logger.warning("Failed to delete task artifact %s: %s", path, exc)
    return 0


def cleanup_task_artifacts(
    user_id: str,
    workspace_id: str,
    task_id: str,
    payloads: Iterable[Any] = (),
) -> int:
    """Delete transient artifacts owned by one task.

    Ownership is determined by two sources: canonical materialized cache names
    containing ``task_id`` and path-like values recorded in task request/result
    payloads. Path cleanup is restricted to ``data/artifacts`` so promoted
    workspace data remains durable.

    Used by:
    - core workspace and worker services because background jobs need one lifecycle owner
      for submission, progress, cancellation, and artifact cleanup.

    Flow: derive owned artifact paths, walk serialized task payloads for references, remove
        only workspace-owned files, and tolerate missing paths during cleanup.
    """
    count = cleanup_task_caches(user_id, workspace_id, task_id)
    artifact_root = _workspace_artifact_root(user_id, workspace_id)
    if artifact_root is None:
        return count

    paths: set[str] = set()
    for payload in payloads:
        paths.update(_iter_artifact_paths(payload))

    for raw_path in paths:
        count += _delete_owned_path(raw_path, artifact_root)
    return count


def _model_payload(value: Any) -> Any:
    """Support task artifact ownership cleanup with a model payload helper.

    Called by:
    - Local helpers, route handlers, or service methods in this module because they need a
      backend boundary that validates inputs before delegating to workspace or worker state.

    Flow: derive owned artifact paths, walk serialized task payloads for references, remove
        only workspace-owned files, and tolerate missing paths during cleanup.
    """

    if value is None:
        return None
    if hasattr(value, "model_dump"):
        try:
            return value.model_dump(mode="python")
        except Exception as exc:
            logger.debug("Could not dump task model for artifact cleanup: %s", exc)
            return None
    return value


def cleanup_analysis_task_artifacts(user_id: str, task: Any) -> int:
    """Delete artifacts referenced by an analysis task record.

    Used by:
    - analysis task helpers, core workspace and worker services because background jobs need
      one lifecycle owner for submission, progress, cancellation, and artifact cleanup.

    Flow: derive owned artifact paths, walk serialized task payloads for references, remove
        only workspace-owned files, and tolerate missing paths during cleanup.
    """
    task_id = str(getattr(task, "task_id", "") or "")
    workspace_id = str(getattr(task, "workspace_id", "") or "")
    if not task_id or not workspace_id:
        return 0

    payloads: list[Any] = []
    request_payload = _model_payload(getattr(task, "request", None))
    if request_payload is not None:
        payloads.append(request_payload)

    result = getattr(task, "result", None)
    if result is not None:
        if hasattr(result, "to_json"):
            try:
                payloads.append(result.to_json())
            except Exception as exc:
                logger.debug("Could not serialize task result for cleanup: %s", exc)
        else:
            payloads.append(result)

    return cleanup_task_artifacts(user_id, workspace_id, task_id, payloads)


def cleanup_worker_task_artifacts(task_info: Any) -> int:
    """Delete artifacts referenced by a worker task record.

    Used by:
    - core workspace and worker services because background jobs need one lifecycle owner
      for submission, progress, cancellation, and artifact cleanup.

    Flow: derive owned artifact paths, walk serialized task payloads for references, remove
        only workspace-owned files, and tolerate missing paths during cleanup.
    """
    task_id = str(getattr(task_info, "id", "") or "")
    user_id = str(getattr(task_info, "user_id", "") or "")
    workspace_id = str(getattr(task_info, "workspace_id", "") or "")
    if not task_id or not user_id or not workspace_id:
        return 0

    payloads: list[Any] = []
    result = getattr(task_info, "result", None)
    if result is not None:
        payloads.append(result)

    return cleanup_task_artifacts(user_id, workspace_id, task_id, payloads)


__all__ = [
    "cleanup_analysis_task_artifacts",
    "cleanup_task_artifacts",
    "cleanup_worker_task_artifacts",
]
