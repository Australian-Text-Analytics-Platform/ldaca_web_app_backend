"""Process-pool worker facade and task registry."""

from __future__ import annotations

import importlib
import importlib.util
import logging
import os
import time
from concurrent.futures import Future, ProcessPoolExecutor
from typing import Any, Callable

from .worker_tasks_concordance import (
    run_concordance_detach_task,
    run_concordance_dispersion_detach_task,
    run_concordance_materialize_task,
)
from .worker_tasks_download import run_workspace_download_task
from .worker_tasks_import import run_ldaca_import_task
from .worker_tasks_quotation import (
    run_quotation_detach_task,
    run_quotation_materialize_task,
)
from .worker_tasks_token import run_token_frequencies_task
from .worker_tasks_topic import run_topic_modeling_task

logger = logging.getLogger(__name__)


def _build_progress_callback(
    progress_queue: Any | None,
    progress_callback: Callable[[float, str], None] | None,
) -> Callable[[float, str], None] | None:
    if progress_queue is None and progress_callback is None:
        return None

    def _cb(progress: float, message: str) -> None:
        payload = {
            "progress": float(progress),
            "message": str(message),
            "timestamp": time.time(),
        }

        if progress_queue is not None:
            try:
                progress_queue.put_nowait(payload)
            except Exception as exc:
                logger.debug(
                    "progress_queue.put_nowait failed, retrying with put: %s", exc
                )
                try:
                    progress_queue.put(payload)
                except Exception as put_exc:
                    logger.debug(
                        "progress_queue.put failed; dropping progress payload: %s",
                        put_exc,
                    )

        if progress_callback is not None:
            try:
                progress_callback(progress, message)
            except Exception as exc:
                logger.debug("progress_callback invocation failed: %s", exc)

    return _cb


def _configure_worker_environment() -> None:
    """Initialize worker process runtime environment."""
    os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")

    # Numba threading layer selection: prefer Intel TBB when installed (fastest
    # on multi-core), otherwise fall back to the workqueue layer which is
    # pure-Python and always available. The TBB import is wrapped because
    # `find_spec` may succeed for a partially-installed distribution and the
    # subsequent import can still fail.
    tbb_available = False
    try:
        if importlib.util.find_spec("tbb"):
            importlib.import_module("tbb")
            tbb_available = True
        elif importlib.util.find_spec("tbb4py"):
            importlib.import_module("tbb4py")
            tbb_available = True
    except Exception:
        tbb_available = False

    if tbb_available:
        os.environ.setdefault("NUMBA_THREADING_LAYER_PRIORITY", "tbb workqueue omp")
        os.environ.setdefault("NUMBA_THREADING_LAYER", "tbb")
    else:
        os.environ.setdefault("NUMBA_THREADING_LAYER", "workqueue")
        os.environ.setdefault("NUMBA_THREADING_LAYER_PRIORITY", "workqueue omp tbb")
        os.environ.setdefault("NUMBA_NUM_THREADS", "1")


def ldaca_import_task(
    user_id: str,
    workspace_id: str,
    url: str,
    filename: str | None = None,
    api_token: str | None = None,
    progress_callback: Callable[[float, str], None] | None = None,
    progress_queue: Any | None = None,
) -> dict[str, Any]:
    cb = _build_progress_callback(progress_queue, progress_callback)
    return run_ldaca_import_task(
        _configure_worker_environment,
        user_id,
        workspace_id,
        url,
        filename,
        api_token,
        cb,
    )


def workspace_download_task(
    user_id: str,
    workspace_id: str,
    target_workspace_dir: str | None = None,
    progress_callback: Callable[[float, str], None] | None = None,
    progress_queue: Any | None = None,
) -> dict[str, Any]:
    cb = _build_progress_callback(progress_queue, progress_callback)
    return run_workspace_download_task(
        _configure_worker_environment,
        user_id,
        workspace_id,
        target_workspace_dir,
        cb,
    )


def concordance_detach_task(
    user_id: str,
    workspace_id: str,
    workspace_dir: str,
    node_corpus: list[str],
    parent_node_id: str,
    document_column: str,
    search_word: str,
    num_left_tokens: int,
    num_right_tokens: int,
    regex: bool,
    whole_word: bool,
    case_sensitive: bool,
    new_node_name: str,
    include_document_column: bool = False,
    include_extraction: bool = False,
    extra_columns_data: dict[str, list] | None = None,
    extra_columns_dtypes: dict[str, Any] | None = None,
    materialized_path: str | None = None,
    language: str | None = None,
    progress_callback: Callable[[float, str], None] | None = None,
    progress_queue: Any | None = None,
) -> dict[str, Any]:
    cb = _build_progress_callback(progress_queue, progress_callback)
    return run_concordance_detach_task(
        _configure_worker_environment,
        workspace_dir,
        node_corpus,
        parent_node_id,
        document_column,
        search_word,
        num_left_tokens,
        num_right_tokens,
        regex,
        whole_word,
        case_sensitive,
        new_node_name,
        include_document_column=include_document_column,
        include_extraction=include_extraction,
        extra_columns_data=extra_columns_data,
        extra_columns_dtypes=extra_columns_dtypes,
        materialized_path=materialized_path,
        language=language,
        progress_callback=cb,
    )


def concordance_dispersion_detach_task(
    user_id: str,
    workspace_id: str,
    workspace_dir: str,
    node_corpus: list[str],
    parent_node_id: str,
    document_column: str,
    search_word: str,
    num_left_tokens: int,
    num_right_tokens: int,
    regex: bool,
    whole_word: bool,
    case_sensitive: bool,
    new_node_name: str,
    parent_task_id: str | None = None,
    include_document_column: bool = True,
    extra_columns_data: dict[str, list] | None = None,
    extra_columns_dtypes: dict[str, Any] | None = None,
    materialized_path: str | None = None,
    selected_bins: list[int] | None = None,
    total_bins: int | None = None,
    selected_matched_texts: list[str] | None = None,
    match_case_insensitive: bool = False,
    language: str | None = None,
    progress_callback: Callable[[float, str], None] | None = None,
    progress_queue: Any | None = None,
) -> dict[str, Any]:
    cb = _build_progress_callback(progress_queue, progress_callback)
    return run_concordance_dispersion_detach_task(
        _configure_worker_environment,
        workspace_dir,
        node_corpus,
        parent_node_id,
        document_column,
        search_word,
        num_left_tokens,
        num_right_tokens,
        regex,
        whole_word,
        case_sensitive,
        new_node_name,
        parent_task_id=parent_task_id,
        include_document_column=include_document_column,
        extra_columns_data=extra_columns_data,
        extra_columns_dtypes=extra_columns_dtypes,
        materialized_path=materialized_path,
        selected_bins=selected_bins,
        total_bins=total_bins,
        selected_matched_texts=selected_matched_texts,
        match_case_insensitive=match_case_insensitive,
        language=language,
        progress_callback=cb,
    )


def concordance_materialize_task(
    user_id: str,
    workspace_id: str,
    workspace_dir: str,
    node_corpus: list[str],
    parent_task_id: str,
    parent_node_id: str,
    document_column: str,
    search_word: str,
    num_left_tokens: int,
    num_right_tokens: int,
    regex: bool,
    whole_word: bool,
    case_sensitive: bool,
    extra_columns_data: dict[str, list] | None = None,
    extra_columns_dtypes: dict[str, Any] | None = None,
    search_mode: str = "regex",
    node_tokens: list[Any] | None = None,
    language: str | None = None,
    progress_callback: Callable[[float, str], None] | None = None,
    progress_queue: Any | None = None,
) -> dict[str, Any]:
    cb = _build_progress_callback(progress_queue, progress_callback)
    return run_concordance_materialize_task(
        _configure_worker_environment,
        workspace_dir,
        node_corpus,
        parent_task_id,
        parent_node_id,
        document_column,
        search_word,
        num_left_tokens,
        num_right_tokens,
        regex,
        whole_word,
        case_sensitive,
        extra_columns_data=extra_columns_data,
        extra_columns_dtypes=extra_columns_dtypes,
        search_mode=search_mode,
        node_tokens=node_tokens,
        language=language,
        progress_callback=cb,
    )


def quotation_detach_task(
    user_id: str,
    workspace_id: str,
    workspace_dir: str,
    node_corpus: list[str],
    parent_node_id: str,
    document_column: str,
    engine_config: dict[str, Any],
    new_node_name: str,
    include_document_column: bool = False,
    include_extraction: bool = False,
    extra_columns_data: dict[str, list] | None = None,
    extra_columns_dtypes: dict[str, Any] | None = None,
    materialized_path: str | None = None,
    progress_callback: Callable[[float, str], None] | None = None,
    progress_queue: Any | None = None,
) -> dict[str, Any]:
    cb = _build_progress_callback(progress_queue, progress_callback)
    return run_quotation_detach_task(
        _configure_worker_environment,
        workspace_dir,
        node_corpus,
        parent_node_id,
        document_column,
        engine_config,
        new_node_name,
        include_document_column=include_document_column,
        include_extraction=include_extraction,
        extra_columns_data=extra_columns_data,
        extra_columns_dtypes=extra_columns_dtypes,
        materialized_path=materialized_path,
        progress_callback=cb,
    )


def quotation_materialize_task(
    user_id: str,
    workspace_id: str,
    workspace_dir: str,
    node_corpus: list[str],
    parent_task_id: str,
    parent_node_id: str,
    document_column: str,
    engine_config: dict[str, Any],
    extra_columns_data: dict[str, list] | None = None,
    extra_columns_dtypes: dict[str, Any] | None = None,
    progress_callback: Callable[[float, str], None] | None = None,
    progress_queue: Any | None = None,
) -> dict[str, Any]:
    cb = _build_progress_callback(progress_queue, progress_callback)
    return run_quotation_materialize_task(
        _configure_worker_environment,
        workspace_dir,
        node_corpus,
        parent_task_id,
        parent_node_id,
        document_column,
        engine_config,
        extra_columns_data=extra_columns_data,
        extra_columns_dtypes=extra_columns_dtypes,
        progress_callback=cb,
    )


def topic_modeling_task(
    user_id: str,
    workspace_id: str,
    node_infos: list[dict[str, Any]],
    artifact_dir: str,
    artifact_prefix: str,
    min_topic_size: int,
    workspace_dir: str | None = None,
    corpora: list[list[str]] | None = None,
    random_seed: int = 42,
    representative_words_count: int = 5,
    progress_callback: Callable[[float, str], None] | None = None,
    progress_queue: Any | None = None,
    embedding_cache_dir: str | None = None,
    sample_fractions: list[float | None] | None = None,
    topic_size_mode: str | None = "target",
    topic_size_value: int | None = 25,
    language: str | None = None,
) -> dict[str, Any]:
    cb = _build_progress_callback(progress_queue, progress_callback)
    return run_topic_modeling_task(
        configure_worker_environment=_configure_worker_environment,
        user_id=user_id,
        workspace_id=workspace_id,
        workspace_dir=workspace_dir,
        corpora=corpora,
        node_infos=node_infos,
        artifact_dir=artifact_dir,
        artifact_prefix=artifact_prefix,
        min_topic_size=min_topic_size,
        random_seed=random_seed,
        representative_words_count=representative_words_count,
        progress_callback=cb,
        embedding_cache_dir=embedding_cache_dir,
        sample_fractions=sample_fractions,
        topic_size_mode=topic_size_mode,
        topic_size_value=topic_size_value,
        language=language,
    )


def token_frequencies_task(
    user_id: str,
    workspace_id: str,
    node_corpora: dict[str, list[str]],
    node_display_names: dict[str, str],
    artifact_dir: str,
    artifact_prefix: str,
    token_limit: int,
    stop_words: list[str] | None = None,
    progress_callback: Callable[[float, str], None] | None = None,
    progress_queue: Any | None = None,
    node_tokens: dict[str, list[list[str]]] | None = None,
    node_token_streams: dict[str, str] | None = None,
) -> dict[str, Any]:
    cb = _build_progress_callback(progress_queue, progress_callback)
    return run_token_frequencies_task(
        configure_worker_environment=_configure_worker_environment,
        user_id=user_id,
        workspace_id=workspace_id,
        node_corpora=node_corpora,
        node_display_names=node_display_names,
        artifact_dir=artifact_dir,
        artifact_prefix=artifact_prefix,
        token_limit=token_limit,
        stop_words=stop_words,
        progress_callback=cb,
        node_tokens=node_tokens,
        node_token_streams=node_token_streams,
    )


def _pid_reporting_wrapper(task_func: Any, **kwargs: Any) -> Any:
    """Wrapper executed inside the worker process.

    Sends the worker's own PID as the first message on the progress queue so
    the main process can terminate it if the user requests cancellation.
    """
    pq = kwargs.get("progress_queue")
    if pq is not None:
        try:
            pq.put_nowait({"type": "pid", "pid": os.getpid()})
        except Exception:
            pass
    return task_func(**kwargs)


TASK_REGISTRY: dict[str, Any] = {
    "ldaca_import": ldaca_import_task,
    "workspace_download": workspace_download_task,
    "concordance_detach": concordance_detach_task,
    "concordance_dispersion_detach": concordance_dispersion_detach_task,
    "concordance_materialize": concordance_materialize_task,
    "quotation_detach": quotation_detach_task,
    "quotation_materialize": quotation_materialize_task,
    "topic_modeling": topic_modeling_task,
    "token_frequencies": token_frequencies_task,
}

_worker_pool: WorkerTaskManager | None = None


def get_worker_pool(max_workers: int = 2) -> "WorkerTaskManager":
    global _worker_pool
    if _worker_pool is None:
        _worker_pool = WorkerTaskManager(max_workers=max_workers)
    return _worker_pool


class WorkerTaskManager:
    """Simple process-pool task manager for CPU-heavy operations."""

    def __init__(self, max_workers: int = 2):
        self.max_workers = max_workers
        self.executor: ProcessPoolExecutor | None = None
        self.is_running = False

    def start(self) -> None:
        if self.executor is None:
            self.executor = ProcessPoolExecutor(max_workers=self.max_workers)
        self.is_running = True

    def submit_task(self, task_func: Any, **kwargs: Any) -> Future:
        if self.executor is None:
            self.start()

        assert self.executor is not None
        return self.executor.submit(_pid_reporting_wrapper, task_func, **kwargs)

    def shutdown(self, wait: bool = True, timeout: float | None = None) -> None:
        if self.executor is None:
            self.is_running = False
            return
        self.executor.shutdown(wait=wait)
        self.executor = None
        self.is_running = False
