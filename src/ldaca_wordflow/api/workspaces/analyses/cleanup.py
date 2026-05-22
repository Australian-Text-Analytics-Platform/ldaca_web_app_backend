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
from typing import Iterable

from ....analysis.manager import get_task_manager
from ....analysis.models import AnalysisStatus
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


def _dedupe_task_ids(task_ids: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for task_id in task_ids:
        if not task_id or task_id in seen:
            continue
        seen.add(task_id)
        deduped.append(task_id)
    return deduped


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
                    await worker_tm.clear_task_tree(task_id)
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

            task_ids_to_clear = _dedupe_task_ids(
                [
                    *analysis_tm.get_descendant_task_ids(task_id),
                    *await worker_tm.get_descendant_task_ids(task_id),
                    task_id,
                ]
            )

            for current_task_id in task_ids_to_clear:
                try:
                    await worker_tm.clear_task_tree(current_task_id)
                except Exception as exc:
                    logger.debug(
                        "Worker clear failed for task %s: %s", current_task_id, exc
                    )

            for current_task_id in task_ids_to_clear:
                analysis_tm.clear_task(current_task_id)
