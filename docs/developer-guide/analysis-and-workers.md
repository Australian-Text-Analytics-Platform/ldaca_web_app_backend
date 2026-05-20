# Analysis And Workers

## Two Task Layers

The backend has two related but separate task concepts.

`analysis/manager.py` stores user-visible analysis tasks. These records track
which feature ran, what request produced the result, the current task for a
tab, and the terminal result or error.

`core/worker_task_manager.py` manages process-pool futures. It starts workers,
captures progress, tracks worker pids, cancels running work, emits SSE events,
and applies side effects when a worker returns.

This split lets the UI show one coherent task center while the backend keeps
worker execution details out of analysis result models.

## Worker Registry

`core/worker.py` defines the worker registry. Every process worker goes through
`_configure_worker_environment()` before importing heavy dependencies. That
setup disables tokenizer parallelism and configures Numba threading so worker
processes do not oversubscribe CPU cores.

Registered worker tasks include:

- LDaCA import,
- workspace ZIP download,
- token frequencies,
- concordance detach, dispersion detach, and materialization,
- quotation detach and materialization,
- topic modeling.

Worker functions should be picklable, import heavy modules inside the worker
body, report progress through the provided queue, and write large outputs to
artifacts instead of returning huge payloads.

## Worker Completion Side Effects

`WorkerTaskManager` monitors each future. On completion it may:

- store an analysis result in the analysis task manager,
- add a materialized node to the active workspace,
- save workspace metadata,
- emit `task_changed`, `workspace_updated`, or `analysis_materialized`,
- record failed or cancelled state.

Detach tasks generally create new workspace nodes from worker-produced
artifacts. Materialize tasks update an existing analysis task request/result so
future paging can read from a persisted artifact.

## SSE Task Stream

`api/tasks.py` exposes `/api/tasks/stream`. The stream sends:

- an initial `tasks_snapshot`,
- `task_changed` events for progress and terminal states,
- `workspace_updated` when worker side effects change the graph,
- `analysis_materialized` when a paged analysis result has been persisted,
- heartbeat events to keep the connection alive.

Native `EventSource` cannot send an `Authorization` header, so the endpoint
also accepts `?token=...` and adapts it to the normal auth dependency.

## Analysis Modules

The analysis routes live under `api/workspaces/analyses/`.

- Token frequencies submit worker jobs, store result artifacts, support
  current request/result endpoints, and expose update/clear flows.
- Concordance supports regex and token modes, result paging, dispersion bins,
  detach, dispersion detach, and materialization.
- Quotation can use a local extractor or remote quotation service, then pages,
  detaches, or materializes quote results.
- Sequential analysis runs synchronously over lazy Polars expressions for time
  and group buckets, with selected-period detach.
- Topic modeling submits BERTopic/embedding work to workers and uses embedding
  caches.
- AI annotation calls OpenAI structured-output classification and can detach
  saved labels into workspace data.
- Derived columns create and remove hidden token columns used by downstream
  analyses.

Shared helpers in `cleanup.py`, `current_tasks.py`, `generated_columns.py`,
`page_size_estimation.py`, and core cache modules keep route code smaller.

## Cache Ownership

Analysis caches are user and workspace scoped. Workspace save garbage
collection deliberately skips dotfile parquet artifacts because analysis
materialization and cache cleanup own those lifecycles. Clearing a task should
also clear its analysis cache files when they are no longer referenced.
