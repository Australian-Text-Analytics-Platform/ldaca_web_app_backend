"""Topic Modeling analysis endpoints (background-task based)."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import cast
from uuid import uuid4

import polars as pl
from fastapi import APIRouter, Depends, HTTPException

from docworkspace import Node

from ....analysis.implementations.topic_modeling import (
    TopicModelingRequest as AnalysisTopicModelingRequest,
)
from ....analysis.manager import get_task_manager
from ....analysis.models import AnalysisStatus, AnalysisTask
from ....analysis.results import GenericAnalysisResult
from ....core.auth import get_current_user
from ....core.i18n import effective_language
from ....core.worker_tasks_topic import reaggregate_exact_topic_modeling_result
from ....core.workspace import workspace_manager
from ....models import (
    TopicModelingData,
    TopicModelingDetachOptionsResponse,
    TopicModelingDetachRequest,
    TopicModelingDetachResponse,
    TopicModelingRequest,
    TopicModelingResponse,
)
from ....core.utils import get_user_cache_folder, get_user_data_folder
from ..utils import ensure_task_synced, update_workspace
from .cleanup import clear_previous_completed_analysis_task
from .current_tasks import get_current_task_ids_for_analysis
from .generated_columns import TOPIC_COLUMN, TOPIC_MEANING_COLUMN

router = APIRouter(prefix="/workspaces", tags=["topic-modeling"])
logger = logging.getLogger(__name__)

_TOPIC_SUBMISSION_LOCKS: dict[tuple[str, str], asyncio.Lock] = {}


def _topic_submission_lock(user_id: str, workspace_id: str) -> asyncio.Lock:
    key = (user_id, workspace_id)
    lock = _TOPIC_SUBMISSION_LOCKS.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _TOPIC_SUBMISSION_LOCKS[key] = lock
    return lock


def _prepare_topic_artifact_target(user_id: str, workspace_id: str) -> tuple[Path, str]:
    workspace_artifacts_dir = workspace_manager.ensure_workspace_artifacts_dir(
        user_id, workspace_id
    )
    if workspace_artifacts_dir is None:
        raise HTTPException(status_code=404, detail="Workspace not found")
    artifact_prefix = f"topic_modeling_{uuid4()}"
    return workspace_artifacts_dir, artifact_prefix


def _task_result_payload(task: AnalysisTask) -> dict:
    if task.result is None:
        return {}
    payload = task.result.to_json()
    if not isinstance(payload, dict):
        return {}
    return payload


def _topic_artifacts_from_task(task: AnalysisTask) -> dict:
    payload = _task_result_payload(task)
    artifacts = payload.get("artifacts")
    if not isinstance(artifacts, dict):
        raise HTTPException(
            status_code=404,
            detail="Topic modeling artifacts are not available for this task",
        )
    node_artifacts = artifacts.get("nodes")
    meanings_path = artifacts.get("topic_meanings_parquet_path")
    if not isinstance(node_artifacts, list) or not isinstance(meanings_path, str):
        raise HTTPException(
            status_code=500,
            detail="Topic modeling artifact manifest is invalid",
        )
    return artifacts


def _task_request_payload(task: AnalysisTask) -> dict[str, object]:
    request = task.request
    if request is None:
        return {}
    if hasattr(request, "model_dump"):
        payload = request.model_dump()
    elif hasattr(request, "dict"):
        payload = request.dict()
    elif isinstance(request, dict):
        payload = request
    else:
        return {}
    return payload if isinstance(payload, dict) else {}


def _format_sampling_scalar(value: float | int) -> str:
    return str(value).replace(".", "_")


def _build_sampling_auto_node_name(
    *,
    base_name: str,
    sample_fraction: float,
    random_seed: int | None,
) -> str:
    sample_token = f"fr_{_format_sampling_scalar(sample_fraction)}"
    seed_token = (
        f"_rs_{random_seed}"
        if isinstance(random_seed, int) and random_seed >= 0
        else ""
    )
    return f"{base_name}_sampled_{sample_token}{seed_token}"


def _topic_sampling_details_for_node(
    task: AnalysisTask,
    node_id: str,
) -> tuple[float | None, int]:
    request_payload = _task_request_payload(task)
    random_seed = request_payload.get("random_seed")
    if isinstance(random_seed, bool):
        seed = int(random_seed)
    elif isinstance(random_seed, int | float | str):
        try:
            seed = int(random_seed)
        except ValueError:
            seed = 42
    else:
        seed = 42

    node_ids = request_payload.get("node_ids")
    sample_fractions = request_payload.get("sample_fractions")
    if not isinstance(node_ids, list) or not isinstance(sample_fractions, list):
        return None, seed

    node_index = next(
        (index for index, value in enumerate(node_ids) if str(value) == node_id),
        None,
    )
    if node_index is None or node_index >= len(sample_fractions):
        return None, seed

    sample_fraction = sample_fractions[node_index]
    if sample_fraction is None:
        return None, seed
    if isinstance(sample_fraction, bool):
        fraction_value = float(sample_fraction)
    elif isinstance(sample_fraction, int | float | str):
        try:
            fraction_value = float(sample_fraction)
        except ValueError:
            return None, seed
    else:
        return None, seed

    if not (0.0 < fraction_value < 1.0):
        return None, seed
    return fraction_value, seed


def _default_topic_detach_node_name(
    task: AnalysisTask,
    artifact_payload: dict,
    node_id: str,
) -> str:
    raw_base_name = str(artifact_payload.get("node_name") or node_id).strip() or "node"
    base_name = f"{raw_base_name}_topic"
    sample_fraction, seed = _topic_sampling_details_for_node(task, node_id)
    if sample_fraction is None:
        return base_name
    return _build_sampling_auto_node_name(
        base_name=base_name,
        sample_fraction=sample_fraction,
        random_seed=seed,
    )


@router.delete("/topic-modeling")
async def clear_topic_modeling_results(
    current_user: dict = Depends(get_current_user),
):
    """Clear stored topic-modeling task state for a workspace.

    Used by:
    - Frontend clear action: `DELETE /workspaces/{id}/topic-modeling`

    Why:
    - Removes stale result/task pointers before reruns and keeps UI state
        aligned with backend task registries.
    """
    user_id = current_user["id"]
    workspace_id = workspace_manager.get_current_workspace_id(user_id)
    if not workspace_id:
        raise HTTPException(status_code=404, detail="No active workspace selected")

    task_manager = get_task_manager(user_id)
    current_id = task_manager.get_current_task_ids("topic_modeling")
    if current_id:
        task_manager.clear_task(current_id[0])

    worker_tm = workspace_manager.get_task_manager(user_id)
    if current_id:
        await worker_tm.clear_task(current_id[0])

    return {
        "state": "successful",
        "message": "Topic modeling analysis results have been cleared.",
    }


def _embedding_cache_dirs(user_id: str) -> list[Path]:
    """Return all embedding-cache directories that may hold parquet entries.

    Includes the canonical location (`user_cache/embeddings`) and the legacy
    `user_data/embedding_cache` so a Clear sweeps both during the migration
    window. Only directories that actually exist are returned.
    """
    candidates = [
        get_user_cache_folder(user_id) / "embeddings",
        get_user_data_folder(user_id) / "embedding_cache",
    ]
    return [d for d in candidates if d.exists() and d.is_dir()]


def _measure_embedding_cache(user_id: str) -> dict:
    """Compute total size and file count across all embedding-cache parquets."""
    bytes_total = 0
    file_count = 0
    for cache_dir in _embedding_cache_dirs(user_id):
        for entry in cache_dir.glob("*.parquet"):
            try:
                bytes_total += entry.stat().st_size
                file_count += 1
            except OSError:
                continue
    return {"bytes": bytes_total, "files": file_count}


@router.get("/topic-modeling/embedding-cache/size")
async def get_topic_modeling_embedding_cache_size(
    current_user: dict = Depends(get_current_user),
):
    """Report current embedding-cache size and file count for the user.

    Used by the frontend Clear-cache confirmation dialog so the user can see
    "X MB will be freed" before they confirm.
    """
    return {
        "state": "successful",
        "data": _measure_embedding_cache(current_user["id"]),
    }


@router.delete("/topic-modeling/embedding-cache")
async def clear_topic_modeling_embedding_cache(
    current_user: dict = Depends(get_current_user),
):
    """Delete every parquet entry in the user's embedding cache.

    Sweeps the canonical `user_cache/embeddings/` directory and the legacy
    `user_data/embedding_cache/` directory (one-time migration cleanup).
    Returns total bytes and file count freed so the UI can confirm the
    reclaim. Clearing forces the next topic-modelling run to re-encode all
    documents from scratch.
    """
    user_id = current_user["id"]
    measured = _measure_embedding_cache(user_id)
    bytes_freed = 0
    files_removed = 0
    for cache_dir in _embedding_cache_dirs(user_id):
        for entry in cache_dir.glob("*.parquet"):
            try:
                size = entry.stat().st_size
                entry.unlink()
            except OSError as exc:
                logger.debug("Failed to remove %s: %s", entry, exc)
                continue
            bytes_freed += size
            files_removed += 1
        # Drop the legacy folder once empty so the file tree stays clean.
        if cache_dir.parent == get_user_data_folder(user_id):
            try:
                cache_dir.rmdir()
            except OSError:
                pass
    return {
        "state": "successful",
        "message": "Embedding cache cleared.",
        "data": {
            "bytes_freed": bytes_freed,
            "files_removed": files_removed,
            "measured_before": measured,
        },
    }


@router.post("/topic-modeling", response_model=TopicModelingResponse)
async def run_topic_modeling(
    request: TopicModelingRequest,
    current_user: dict = Depends(get_current_user),
):
    """Submit topic-modeling analysis as a worker-backed background task.

    Used by:
    - Frontend run route: `POST /workspaces/{id}/topic-modeling`

    Why:
    - Offloads heavy modeling work to worker processes and returns `task_id`
        for progress/result polling.
    """
    user_id = current_user["id"]
    workspace_id = workspace_manager.get_current_workspace_id(user_id)
    ws = workspace_manager.get_current_workspace(user_id)
    if not workspace_id or ws is None:
        raise HTTPException(status_code=404, detail="No active workspace selected")

    if not request.node_ids:
        raise HTTPException(
            status_code=400, detail="At least one node ID must be provided"
        )

    node_infos: list[dict[str, object]] = []
    for node_id in request.node_ids:
        node = ws.nodes[node_id]
        node_data = node.data

        column_name = request.node_columns[node_id]
        available_columns = list(node_data.collect_schema().names())

        try:
            node.document = column_name
        except Exception as exc:
            logger.debug(
                "Failed to set topic-modeling node.document for node %s column %s: %s",
                node_id,
                column_name,
                exc,
            )

        node_infos.append(
            {
                "node_id": node_id,
                "node_name": getattr(node, "name", None) or node_id,
                "text_column": column_name,
                "original_columns": available_columns,
            }
        )

    tm = workspace_manager.get_task_manager(user_id)
    submission_lock = _topic_submission_lock(user_id, workspace_id)
    async with submission_lock:
        # Match token-frequencies behavior: short-circuit when topic modeling is
        # already running for this workspace/user, with lock to avoid duplicate
        # concurrent submissions.
        try:
            if await tm.any_running(
                task_type="topic_modeling", user_id=user_id, workspace_id=workspace_id
            ):
                latest = await tm.latest_by_type(
                    "topic_modeling", user_id=user_id, workspace_id=workspace_id
                )
                return TopicModelingResponse(
                    state="running",
                    message="Topic Modeling analysis already running",
                    data=None,
                    metadata={"task_id": latest.id if latest else None},
                )
        except Exception:
            # Non-fatal: proceed to submit a new task.
            pass

        # Drop any prior completed/failed topic-modeling task before submitting
        # a new one. Prevents unbounded accumulation of in-memory task records
        # and on-disk parquet artifacts as the user iterates on parameters.
        await clear_previous_completed_analysis_task(
            user_id, workspace_id, ["topic_modeling", "topic-modeling"]
        )

        workspace_dir = update_workspace(user_id, workspace_id, ws)
        if workspace_dir is None:
            raise HTTPException(
                status_code=500,
                detail="Failed to persist workspace before topic modeling",
            )

        artifact_dir, artifact_prefix = _prepare_topic_artifact_target(
            user_id, workspace_id
        )
        # Phase 3.5: resolve a single effective language for the label-stage
        # CountVectorizer. Explicit request param wins; otherwise we read
        # from the first node's derived metadata (decision 7). Multi-language
        # corpora left to the user — the frontend should send "multi" or the
        # union language label when mixing nodes.
        first_node = ws.nodes[request.node_ids[0]]
        topic_language = effective_language(request.language, first_node)
        worker_task = await tm.submit_task(
            user_id=user_id,
            workspace_id=workspace_id,
            task_type="topic_modeling",
            task_args={
                "workspace_dir": str(workspace_dir),
                "node_infos": node_infos,
                "artifact_dir": str(artifact_dir),
                "artifact_prefix": artifact_prefix,
                "min_topic_size": request.min_topic_size,
                "random_seed": request.random_seed,
                "representative_words_count": request.representative_words_count,
                "embedding_cache_dir": str(
                    get_user_cache_folder(user_id) / "embeddings"
                ),
                "force_mode": request.force_mode,
                "n_clusters": request.n_clusters,
                "sample_fractions": request.sample_fractions,
                "topic_size_mode": request.topic_size_mode,
                "topic_size_value": request.topic_size_value,
                "language": topic_language,
            },
            task_name="Topic Modeling",
        )

    analysis_tm = get_task_manager(user_id)
    min_topic_size = (
        request.min_topic_size if request.min_topic_size is not None else 10
    )
    random_seed = request.random_seed if request.random_seed is not None else 42
    representative_words_count = (
        request.representative_words_count
        if request.representative_words_count is not None
        else 5
    )

    analysis_request = AnalysisTopicModelingRequest(
        node_ids=request.node_ids,
        node_columns=request.node_columns,
        min_topic_size=min_topic_size,
        random_seed=random_seed,
        representative_words_count=representative_words_count,
        force_mode=request.force_mode,
        n_clusters=request.n_clusters,
        sample_fractions=request.sample_fractions,
        topic_size_mode=request.topic_size_mode,
        topic_size_value=request.topic_size_value,
    )
    analysis_tm.save_task(
        AnalysisTask(
            task_id=worker_task.id,
            user_id=user_id,
            workspace_id=workspace_id,
            request=analysis_request,
            status=AnalysisStatus.RUNNING,
        )
    )
    analysis_tm.set_current_task("topic_modeling", worker_task.id)
    return TopicModelingResponse(
        state="running",
        message="Topic Modeling analysis started",
        data=None,
        metadata={"task_id": worker_task.id},
    )


@router.get("/topic-modeling/tasks/current")
async def topic_modeling_current_tasks(
    current_user: dict = Depends(get_current_user),
):
    """Return current task IDs for topic-modeling analysis."""
    user_id = current_user["id"]
    workspace_id = workspace_manager.get_current_workspace_id(user_id)
    if not workspace_id:
        raise HTTPException(status_code=404, detail="No active workspace selected")
    return await get_current_task_ids_for_analysis(
        user_id, workspace_id, ["topic_modeling", "topic-modeling"]
    )


@router.get("/topic-modeling/tasks/{task_id}/request")
async def topic_modeling_task_request(
    task_id: str,
    current_user: dict = Depends(get_current_user),
):
    """Return stored request payload for a topic-modeling task."""
    user_id = current_user["id"]
    workspace_id = workspace_manager.get_current_workspace_id(user_id)
    if not workspace_id:
        raise HTTPException(status_code=404, detail="No active workspace selected")
    task_manager = get_task_manager(user_id)
    task = task_manager.get_task(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    request = task.request
    return request.model_dump()


@router.get(
    "/topic-modeling/tasks/{task_id}/result",
    response_model=TopicModelingResponse,
)
async def topic_modeling_task_result(
    task_id: str,
    current_user: dict = Depends(get_current_user),
):
    """Return current status or final payload for a topic-modeling task.

    Used by:
    - Frontend polling route:
        `GET /workspaces/{id}/topic-modeling/tasks/{task_id}/result`

    Why:
    - Normalizes task lifecycle states into one response contract for UI polling.
    """
    user_id = current_user["id"]
    workspace_id = workspace_manager.get_current_workspace_id(user_id)
    if not workspace_id:
        raise HTTPException(status_code=404, detail="No active workspace selected")

    task = await ensure_task_synced(
        user_id, workspace_id, task_id, get_task_manager(user_id)
    )

    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    if task.status == AnalysisStatus.RUNNING:
        return TopicModelingResponse(
            state="running",
            message="Topic Modeling analysis is running",
            data=None,
            metadata={"task_id": task_id},
        )

    if task.status == AnalysisStatus.FAILED:
        return TopicModelingResponse(
            state="failed",
            message=(task.error or "Topic Modeling analysis failed"),
            data=None,
            metadata={"task_id": task_id},
        )

    if task.status == AnalysisStatus.COMPLETED and task.result:
        payload = task.result.to_json()
        if not isinstance(payload, dict):
            payload = {}
        result_data = TopicModelingData.model_validate(payload)
        return TopicModelingResponse(
            state="successful",
            message="Topic Modeling analysis complete",
            data=result_data,
            metadata={"task_id": task_id},
        )

    return TopicModelingResponse(
        state="failed",
        message="Topic Modeling analysis failed",
        data=None,
        metadata={"task_id": task_id},
    )


@router.post(
    "/topic-modeling/tasks/{task_id}/result",
    response_model=TopicModelingResponse,
)
async def update_topic_modeling_task_result(
    task_id: str,
    updates: dict | None,
    current_user: dict = Depends(get_current_user),
):
    """Re-aggregate a completed exact topic-modeling task without refitting."""
    user_id = current_user["id"]
    workspace_id = workspace_manager.get_current_workspace_id(user_id)
    if not workspace_id:
        raise HTTPException(status_code=404, detail="No active workspace selected")

    task_manager = get_task_manager(user_id)
    task = await ensure_task_synced(user_id, workspace_id, task_id, task_manager)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    if task.status != AnalysisStatus.COMPLETED or not task.result:
        raise HTTPException(
            status_code=409,
            detail="Topic modeling task is not completed",
        )

    request_payload = task.request.model_dump()
    if request_payload.get("topic_size_mode") != "exact":
        raise HTTPException(
            status_code=409,
            detail="Only exact topic modeling results can be re-aggregated",
        )

    requested_topic_count = updates.get("topic_size_value") if isinstance(updates, dict) else None
    if isinstance(requested_topic_count, bool) or not isinstance(requested_topic_count, int):
        raise HTTPException(
            status_code=400,
            detail="topic_size_value must be an integer",
        )

    payload = _task_result_payload(task)
    artifacts = _topic_artifacts_from_task(task)
    exact_artifact_path = artifacts.get("exact_reduction_artifact_path")
    if not isinstance(exact_artifact_path, str) or not exact_artifact_path:
        raise HTTPException(
            status_code=409,
            detail="This exact topic-modeling result cannot be re-aggregated",
        )

    node_artifacts = artifacts.get("nodes") or []
    node_infos = [
        {
            "node_id": node_payload.get("node_id"),
            "node_name": node_payload.get("node_name"),
            "text_column": node_payload.get("text_column"),
            "original_columns": node_payload.get("original_columns") or [],
        }
        for node_payload in node_artifacts
        if isinstance(node_payload, dict)
    ]
    if len(node_infos) != len(node_artifacts):
        raise HTTPException(
            status_code=500,
            detail="Topic modeling artifact manifest is invalid",
        )

    try:
        updated_payload = reaggregate_exact_topic_modeling_result(
            artifact_path=exact_artifact_path,
            existing_artifacts=artifacts,
            node_infos=node_infos,
            topic_size_value=requested_topic_count,
            representative_words_count=int(
                request_payload.get("representative_words_count") or 5
            ),
            random_seed=int(request_payload.get("random_seed") or 42),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover
        raise HTTPException(
            status_code=500,
            detail=f"Failed to re-aggregate exact topic model: {exc}",
        ) from exc

    existing_meta = cast(
        dict[str, object], payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
    )
    updated_meta = cast(
        dict[str, object],
        updated_payload.get("meta") if isinstance(updated_payload.get("meta"), dict) else {},
    )
    payload.update(
        {
            "topics": updated_payload.get("topics", []),
            "corpus_sizes": updated_payload.get("corpus_sizes", []),
            "per_corpus_topic_counts": updated_payload.get(
                "per_corpus_topic_counts", []
            ),
            "artifacts": updated_payload.get("artifacts", artifacts),
            "meta": {
                **existing_meta,
                **updated_meta,
                "topic_size_mode": "exact",
                "topic_size_value": requested_topic_count,
                "representative_words_count": int(
                    request_payload.get("representative_words_count") or 5
                ),
            },
        }
    )

    # Leave `task.request.topic_size_value` as the original rerun target —
    # the post-fit slider is a display-only re-aggregation, decoupled from
    # the "Target Topic Number" parameter that drives rerun. The new value
    # is recorded in `payload.meta.topic_size_value` (above) so the frontend
    # can restore the slider position on reload.
    task.result = GenericAnalysisResult(payload)
    task_manager.save_task(task)

    return TopicModelingResponse(
        state="successful",
        message="Topic Modeling analysis updated",
        data=TopicModelingData.model_validate(payload),
        metadata={"task_id": task_id},
    )


def _resolve_topic_column_name(base_name: str, existing_columns: set[str]) -> str:
    """Return a unique output column name for detached topic data.

    Used by:
    - `detach_topic_modeling`

    Why:
    - Prevents overwriting source columns when attaching generated topic labels.
    """
    candidate = base_name.strip() or TOPIC_COLUMN
    if candidate not in existing_columns:
        return candidate
    idx = 1
    while f"{candidate}_{idx}" in existing_columns:
        idx += 1
    return f"{candidate}_{idx}"


@router.get(
    "/topic-modeling/tasks/{task_id}/detach-options",
    response_model=TopicModelingDetachOptionsResponse,
)
async def topic_modeling_detach_options(
    task_id: str,
    current_user: dict = Depends(get_current_user),
):
    """List detachable node/column options for a completed topic task.

    Used by:
    - Frontend detach-options route:
        `GET /workspaces/{id}/topic-modeling/tasks/{task_id}/detach-options`

    Why:
    - Exposes artifact-backed node metadata so users can choose output columns safely.
    """
    user_id = current_user["id"]
    workspace_id = workspace_manager.get_current_workspace_id(user_id)
    ws = workspace_manager.get_current_workspace(user_id)
    if not workspace_id or ws is None:
        raise HTTPException(status_code=404, detail="No active workspace selected")

    analysis_tm = get_task_manager(user_id)
    task = await ensure_task_synced(user_id, workspace_id, task_id, analysis_tm)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    if task.status != AnalysisStatus.COMPLETED:
        raise HTTPException(
            status_code=409,
            detail="Topic modeling task is not completed",
        )

    artifacts = _topic_artifacts_from_task(task)
    node_artifacts = artifacts.get("nodes") or []

    nodes = []
    for payload in node_artifacts:
        if not isinstance(payload, dict):
            continue
        node_id = payload.get("node_id")
        if not isinstance(node_id, str) or not node_id:
            continue
        source_node = ws.nodes[node_id]
        source_data = source_node.data
        original_columns = list(source_data.collect_schema().names())
        topic_column_name = _resolve_topic_column_name(
            TOPIC_COLUMN, set(original_columns)
        )
        nodes.append(
            {
                "node_id": source_node.id,
                "node_name": payload.get("node_name") or node_id,
                "text_column": payload.get("text_column"),
                "available_columns": [topic_column_name, *original_columns],
                "disabled_columns": [topic_column_name],
            }
        )

    return TopicModelingDetachOptionsResponse(
        state="successful",
        message="Topic detach options loaded",
        data={"nodes": nodes},
        metadata={"task_id": task_id},
    )


@router.post(
    "/topic-modeling/tasks/{task_id}/detach",
    response_model=TopicModelingDetachResponse,
)
async def detach_topic_modeling(
    task_id: str,
    request: TopicModelingDetachRequest,
    current_user: dict = Depends(get_current_user),
):
    """Create detached nodes from artifact-backed topic-modeling outputs.

    Used by:
    - Frontend detach route:
        `POST /workspaces/{id}/topic-modeling/tasks/{task_id}/detach`

        Why:
        - Materializes user-selected columns and topic labels as reusable workspace
            nodes without rerunning the model.
    """
    user_id = current_user["id"]
    workspace_id = workspace_manager.get_current_workspace_id(user_id)
    ws = workspace_manager.get_current_workspace(user_id)
    if not workspace_id or ws is None:
        raise HTTPException(status_code=404, detail="No active workspace selected")

    analysis_tm = get_task_manager(user_id)
    task = await ensure_task_synced(user_id, workspace_id, task_id, analysis_tm)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    if task.status != AnalysisStatus.COMPLETED:
        raise HTTPException(
            status_code=409,
            detail="Topic modeling task is not completed",
        )

    artifacts = _topic_artifacts_from_task(task)
    node_artifacts = artifacts.get("nodes") or []
    assignments_by_node_id = {
        str(payload.get("node_id")): payload
        for payload in node_artifacts
        if isinstance(payload, dict) and payload.get("node_id")
    }
    meanings_path = Path(str(artifacts.get("topic_meanings_parquet_path")))
    if not meanings_path.exists():
        raise HTTPException(
            status_code=404,
            detail="Topic meanings artifact is missing",
        )
    meanings_lf = pl.scan_parquet(meanings_path)
    selected_topic_ids = sorted(
        {int(topic_id) for topic_id in (request.topic_ids or [])}
    )

    target_node_ids = request.node_ids or list(assignments_by_node_id.keys())
    if not target_node_ids:
        raise HTTPException(status_code=400, detail="No node IDs provided for detach")

    detached_nodes: list[dict[str, str]] = []
    for node_id in target_node_ids:
        artifact_payload = assignments_by_node_id.get(node_id)
        if not artifact_payload:
            raise HTTPException(
                status_code=400,
                detail=f"Node {node_id} is not available in topic artifact manifest",
            )

        assignments_path = Path(str(artifact_payload.get("assignments_parquet_path")))
        if not assignments_path.exists():
            raise HTTPException(
                status_code=404,
                detail=f"Topic assignments artifact missing for node {node_id}",
            )
        assignments_lf = pl.scan_parquet(assignments_path)

        # Filter assignments to selected topics if topic_ids specified
        if selected_topic_ids:
            assignments_lf = assignments_lf.filter(
                pl.col(TOPIC_COLUMN).is_in(selected_topic_ids)
            )

        # Per-corpus meanings must match the topic IDs actually present in
        # this corpus's filtered assignments — with two corpora, the global
        # selected set can include topic IDs absent from one corpus, which
        # previously produced a topic_meanings block that wasn't a subset of
        # the associated detached topics block.
        corpus_topic_ids = sorted(
            int(value)
            for value in assignments_lf.select(pl.col(TOPIC_COLUMN))
            .unique()
            .collect()
            .get_column(TOPIC_COLUMN)
            .drop_nulls()
            .to_list()
        )
        filtered_meanings_lf = (
            meanings_lf.filter(pl.col(TOPIC_COLUMN).is_in(corpus_topic_ids))
            if corpus_topic_ids
            else meanings_lf.filter(pl.lit(False))
        ).select(pl.col(TOPIC_COLUMN), pl.col(TOPIC_MEANING_COLUMN))

        source_node = ws.nodes[node_id]
        source_data = source_node.data

        original_columns = list(source_data.collect_schema().names())
        selected_columns = list((request.selected_columns or {}).get(node_id) or [])
        if not selected_columns:
            raise HTTPException(
                status_code=400,
                detail=f"No columns selected for node {node_id}",
            )

        invalid = [col for col in selected_columns if col not in original_columns]
        if invalid:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid selected columns for node {node_id}: {invalid}",
            )

        topic_column_name = _resolve_topic_column_name(
            request.topic_column_name or TOPIC_COLUMN,
            set(original_columns) | set(selected_columns),
        )

        output_lf = (
            assignments_lf.join(
                source_data.with_row_index("__row_nr__"),
                on="__row_nr__",
                how="inner",
            )
            .select(
                [pl.col(col) for col in selected_columns]
                + [pl.col(TOPIC_COLUMN).alias(topic_column_name)]
            )
        )

        parents = [source_node] if source_node else []
        node_name = (
            (request.new_node_names or {}).get(node_id)
            if request.new_node_names
            else None
        ) or _default_topic_detach_node_name(task, artifact_payload, node_id)

        # Materialize the detached outputs into workspace-owned parquet files
        # so the new nodes are self-contained. The originals live under
        # `data/artifacts/` which gets cleaned by the next analysis submit
        # (clear_previous_completed_analysis_task) and by workspace unload —
        # a detached node that still scanned them would silently corrupt on
        # the next run. The top-level workspace data dir is protected by
        # `_garbage_collect_workspace_data` (deletes only unreferenced files).
        workspace_data_dir = Path(ws.ws_root_dir) / "data"
        workspace_data_dir.mkdir(parents=True, exist_ok=True)
        new_node_id = str(uuid4())
        new_node_parquet = workspace_data_dir / f"topic_detach_{new_node_id}.parquet"
        cast(pl.DataFrame, output_lf.collect()).write_parquet(new_node_parquet)
        new_node = Node(
            data=pl.scan_parquet(new_node_parquet),
            id=new_node_id,
            name=node_name,
            workspace=ws,
            operation="topic_modeling_detach",
            parents=parents,
        )
        ws.add_node(new_node)

        text_column = artifact_payload.get("text_column")
        if text_column and text_column in selected_columns:
            try:
                new_node.document = text_column
            except Exception as exc:
                logger.debug(
                    "Failed to set detached topic node document column '%s' for node %s: %s",
                    text_column,
                    new_node.id,
                    exc,
                )

        meanings_node_name = f"{node_name}_topic_meanings"
        meanings_node_id = str(uuid4())
        meanings_node_parquet = (
            workspace_data_dir / f"topic_meanings_detach_{meanings_node_id}.parquet"
        )
        cast(pl.DataFrame, filtered_meanings_lf.collect()).write_parquet(
            meanings_node_parquet
        )
        meanings_node = Node(
            data=pl.scan_parquet(meanings_node_parquet),
            id=meanings_node_id,
            name=meanings_node_name,
            workspace=ws,
            operation="topic_modeling_meanings_detach",
            parents=[new_node],
        )
        ws.add_node(meanings_node)

        detached_nodes.append(
            {
                "source_node_id": node_id,
                "new_node_id": new_node.id,
                "topic_meanings_node_id": meanings_node.id,
            }
        )

    update_workspace(user_id, workspace_id, ws)

    return TopicModelingDetachResponse(
        state="successful",
        message="Topic detach completed",
        data={"detached_nodes": detached_nodes},
        metadata={"task_id": task_id},
    )
