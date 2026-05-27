"""Unified task endpoints.

Provides a single SSE stream and root task operations for Task Center.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

from fastapi import APIRouter, Depends, Header, Query
from fastapi.responses import StreamingResponse

from ..analysis.manager import get_task_manager as get_analysis_task_manager
from ..core.auth import get_current_user
from ..core.workspace import workspace_manager
from ..models import TaskCancelActionResponse, TaskClearActionResponse, TaskListResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/tasks", tags=["task_streaming"])


def _dedupe_task_ids(task_ids: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for task_id in task_ids:
        if not task_id or task_id in seen:
            continue
        seen.add(task_id)
        deduped.append(task_id)
    return deduped


@router.get("", response_model=TaskListResponse)
async def list_tasks(
    current_user: dict = Depends(get_current_user),
):
    """List tasks for current user."""
    user_id = current_user["id"]
    tm = workspace_manager.get_task_manager(user_id)
    tasks = await tm.list(user_id=user_id)
    return {
        "state": "successful",
        "data": tasks,
        "message": "Tasks listed successfully.",
    }


@router.post("/clear", response_model=TaskClearActionResponse)
async def clear_tasks(
    task_id: str = Query(...),
    current_user: dict = Depends(get_current_user),
):
    """Clear a task and all associated caches by task id.

    If the task is still running it is cancelled first.  Both the worker
    task manager record and the analysis task manager record (including
    the current-task-id mapping) are removed.
    """
    user_id = current_user["id"]
    tm = workspace_manager.get_task_manager(user_id)

    analysis_tm = get_analysis_task_manager(user_id)
    analysis_descendant_ids = analysis_tm.get_descendant_task_ids(task_id)
    worker_descendant_ids = await tm.get_descendant_task_ids(task_id)
    task_ids_to_clear = _dedupe_task_ids(
        [*analysis_descendant_ids, *worker_descendant_ids, task_id]
    )

    analysis_tasks = {
        current_task_id: analysis_task
        for current_task_id in task_ids_to_clear
        if (analysis_task := analysis_tm.get_task(current_task_id)) is not None
    }

    cleared_worker_ids: list[str] = []
    for current_task_id in task_ids_to_clear:
        for cleared_worker_id in await tm.clear_task_tree(current_task_id):
            if cleared_worker_id not in cleared_worker_ids:
                cleared_worker_ids.append(cleared_worker_id)
    cleared_worker_id_set = set(cleared_worker_ids)

    cleared_analysis_ids: list[str] = []
    analysis_only_events: list[tuple[str, str]] = []
    for current_task_id in task_ids_to_clear:
        analysis_task = analysis_tasks.get(current_task_id)
        if analysis_task is None:
            continue
        analysis_tm.clear_task(current_task_id)
        cleared_analysis_ids.append(current_task_id)
        if current_task_id not in cleared_worker_id_set:
            analysis_only_events.append((current_task_id, analysis_task.workspace_id))

    timestamp = time.time()
    for removed_task_id, analysis_workspace_id in analysis_only_events:
        await tm.emit(
            user_id,
            analysis_workspace_id,
            {
                "type": "task_removed",
                "task_id": removed_task_id,
                "workspace_id": analysis_workspace_id,
                "timestamp": timestamp,
            },
        )

    cleared_task_ids = _dedupe_task_ids([*cleared_worker_ids, *cleared_analysis_ids])

    return {
        "state": "successful",
        "data": {
            "cleared_worker": bool(cleared_worker_ids),
            "cleared_analysis": bool(cleared_analysis_ids),
            "cleared_worker_ids": cleared_worker_ids,
            "cleared_analysis_ids": cleared_analysis_ids,
            "cleared_task_ids": cleared_task_ids,
        },
        "message": "Task cleared successfully.",
    }


@router.post("/cancel", response_model=TaskCancelActionResponse)
async def cancel_task(
    task_id: str = Query(...),
    current_user: dict = Depends(get_current_user),
):
    """Stop a running task and mark it as cancelled.

    Unlike ``/tasks/clear``, the task record is kept so the user can see the
    cancelled state in the task list and explicitly clear it afterwards.
    """
    user_id = current_user["id"]
    tm = workspace_manager.get_task_manager(user_id)
    stopped = await tm.stop_task(task_id)
    return {
        "state": "successful",
        "data": {"stopped": stopped},
        "message": "Task cancelled."
        if stopped
        else "Task not found or already finished.",
    }


async def _get_stream_user(
    authorization: str | None = Header(None),
    token: str | None = Query(None),
):
    """Resolve auth for the SSE stream endpoint.

    Accepts token from the ``Authorization`` header (fetch clients) or a
    ``token`` query parameter (native ``EventSource`` clients that cannot
    set custom HTTP headers).
    """
    if not authorization and token:
        authorization = f"Bearer {token}"
    return await get_current_user(authorization)


@router.get("/stream")
async def stream_tasks(
    current_user: dict = Depends(_get_stream_user),
):
    """Unified SSE stream for task center.

    Includes all user tasks from a single per-user task manager channel.

        - frontend Task Center SSE subscriber

        Why:
        - Streams all per-user task updates through one connection.

        Refactor note:
        - Nested helper closures inside endpoint are sizeable; extraction to a small
            streaming service object could improve testability.
    """
    user_id = current_user["id"]

    async def event_generator():
        tm = workspace_manager.get_task_manager(user_id)
        queue = await tm.subscribe(user_id)

        try:
            tasks = await tm.list(user_id=user_id)
            snapshot = {
                "type": "tasks_snapshot",
                "tasks": [task for task in tasks if isinstance(task, dict)],
                "timestamp": time.time(),
            }
            yield f"data: {json.dumps(snapshot)}\n\n"

            last_heartbeat = time.time()

            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=30.0)
                except asyncio.TimeoutError:
                    if time.time() - last_heartbeat > 30:
                        heartbeat = {
                            "type": "heartbeat",
                            "timestamp": time.time(),
                        }
                        yield f"data: {json.dumps(heartbeat)}\n\n"
                        last_heartbeat = time.time()
                    continue

                if not isinstance(event, dict):
                    continue

                yield f"data: {json.dumps(event)}\n\n"
                last_heartbeat = time.time()

        except asyncio.CancelledError:  # pragma: no cover
            logger.debug("Unified SSE stream cancelled for user %s", user_id)
        except Exception as exc:  # pragma: no cover
            logger.error("Unified SSE stream error: %s", exc)
            error_data = {
                "type": "error",
                "message": str(exc),
                "timestamp": time.time(),
            }
            yield f"data: {json.dumps(error_data)}\n\n"
        finally:
            try:
                await tm.unsubscribe(user_id, None, queue)
            except Exception as exc:
                logger.error(
                    "Error unsubscribing unified stream for user %s: %s", user_id, exc
                )

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Cache-Control",
        },
    )
