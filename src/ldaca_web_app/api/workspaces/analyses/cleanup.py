"""Helpers for cleaning up prior analysis tasks before submitting a new run.

When the user submits a new analysis (e.g. topic modeling) the previous
"current" task for that analysis type stops being useful: its result is
about to be replaced and any on-disk parquet artifacts referenced by the
result become orphaned.

`clear_previous_completed_analysis_task` removes that prior task from both
the analysis store and the worker store, and best-effort deletes any
artifact files referenced by the stored result. It deliberately skips
tasks that are still running so the existing per-analysis "already
running" short-circuit can do its job.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable, Iterator

from ....analysis.manager import get_task_manager
from ....analysis.models import AnalysisStatus, AnalysisTask
from ....core.workspace import workspace_manager

logger = logging.getLogger(__name__)


_TERMINAL_STATUSES = {
    AnalysisStatus.COMPLETED,
    AnalysisStatus.FAILED,
    AnalysisStatus.CANCELLED,
    "completed",
    "successful",
    "failed",
    "cancelled",
}


def _iter_artifact_paths(node: object) -> Iterator[str]:
    """Yield every string value whose key looks like an artifact path."""
    if isinstance(node, dict):
        for key, value in node.items():
            if isinstance(value, str) and isinstance(key, str) and key.endswith(
                ("_path", "_parquet_path")
            ):
                yield value
            else:
                yield from _iter_artifact_paths(value)
    elif isinstance(node, list):
        for item in node:
            yield from _iter_artifact_paths(item)


def _delete_task_artifacts(task: AnalysisTask) -> None:
    """Best-effort deletion of parquet artifacts referenced by a task result."""
    if task.result is None:
        return
    try:
        payload = task.result.to_json()
    except Exception as exc:
        logger.debug("Could not serialize task result for cleanup: %s", exc)
        return
    if not isinstance(payload, dict):
        return
    artifacts = payload.get("artifacts")
    if not isinstance(artifacts, (dict, list)):
        return
    for raw_path in _iter_artifact_paths(artifacts):
        try:
            path = Path(raw_path)
            if path.is_file():
                path.unlink()
        except Exception as exc:
            logger.debug("Failed to delete artifact %s: %s", raw_path, exc)


async def clear_previous_completed_analysis_task(
    user_id: str,
    workspace_id: str,
    analysis_keys: Iterable[str],
) -> None:
    """Remove any terminal "current" task for the given analysis keys.

    Used by analysis submit endpoints immediately after the
    "already-running" short-circuit and before saving the new task. This
    keeps the analysis store and worker store from accumulating stale
    completed/failed task records and reclaims their on-disk artifacts.

    Running and pending tasks are left untouched so that explicit
    de-duplication logic in each endpoint (e.g. `any_running`) keeps
    working.
    """
    analysis_tm = get_task_manager(user_id)
    worker_tm = workspace_manager.get_task_manager(user_id)

    seen: set[str] = set()
    for key in analysis_keys:
        for task_id in list(analysis_tm.get_current_task_ids(key)):
            if not task_id or task_id in seen:
                continue
            seen.add(task_id)

            task = analysis_tm.get_task(task_id)
            if task is None:
                # Already gone from analysis store; still try worker side.
                try:
                    await worker_tm.clear_task(task_id)
                except Exception as exc:
                    logger.debug(
                        "Worker clear failed for orphan task %s: %s",
                        task_id,
                        exc,
                    )
                continue

            if task.status not in _TERMINAL_STATUSES:
                continue
            if workspace_id and task.workspace_id and task.workspace_id != workspace_id:
                continue

            _delete_task_artifacts(task)
            analysis_tm.clear_task(task_id)
            try:
                await worker_tm.clear_task(task_id)
            except Exception as exc:
                logger.debug(
                    "Worker clear failed for task %s: %s", task_id, exc
                )
