# Backend — Topic Modelling Optimisation Plan

**Branch:** `tm-optimisation`
**Parent plan:** `../../../docs/tm-optimisation/PLAN.md` (root repo)
**Started:** 2026-05-04

This is the backend-specific work. Read the parent plan first for goals, constraints, and phasing rationale.

---

## Surface area in this module

All topic-modelling code lives in:

- `src/ldaca_wordflow/core/worker_tasks_topic.py` — the BERTopic worker task (the file that does all the heavy lifting)
- `src/ldaca_wordflow/core/model_prefetch.py` — startup-time model download helper (runs in a daemon thread)
- `src/ldaca_wordflow/core/worker.py` — `ProcessPoolExecutor` + task-registry plumbing
- `src/ldaca_wordflow/api/workspaces/analyses/topic_modeling.py` — FastAPI routes
- `pyproject.toml` — dependency block

Tests live under `tests/` (need to enumerate which tests exercise the topic-modelling path during phase 1).

## Phase 1 — ONNX-quantised embeddings

### Files to change

| File | Change |
|------|--------|
| `src/ldaca_wordflow/core/worker_tasks_topic.py:21-31` | Replace `_get_embedder` body with ONNX path. Keep the cache key + per-worker caching pattern. |
| `src/ldaca_wordflow/core/worker_tasks_topic.py:34-55` | `_encode_embeddings_in_chunks` — adapt to new embedder API (likely a thin wrapper class implementing `.encode(list[str]) -> np.ndarray` so BERTopic's `embedding_model=` argument keeps working). |
| `src/ldaca_wordflow/core/worker_tasks_topic.py:235` | `embedding_model_name = "all-MiniLM-L6-v2"` — stays the same conceptually; backend code resolves the ONNX variant. |
| `src/ldaca_wordflow/core/worker_tasks_topic.py:239-246` | Drop `import torch` block if torch is fully removed; keep numpy/random seeding. |
| `src/ldaca_wordflow/core/worker_tasks_topic.py:257-268` | BERTopic instantiation — pass our ONNX embedder wrapper as `embedding_model=`. UMAP block unchanged in phase 1. |
| `src/ldaca_wordflow/core/model_prefetch.py:18` | `_TOPIC_EMBEDDER_REPO_ID` — switch to the ONNX-export repo (likely `sentence-transformers/all-MiniLM-L6-v2` `onnx/` subdirectory or a quantised variant). |
| `src/ldaca_wordflow/core/model_prefetch.py:_prefetch_topic_embedder` | Switch to downloading the ONNX file + tokenizer config. Keep the idempotent-cache behaviour. |
| `pyproject.toml` | Add `onnxruntime>=1.18`, `tokenizers>=0.20`. Investigate dropping `sentence-transformers`. Verify `torch` can become absent (it's currently transitive via sentence-transformers/transformers). |

### New module

`src/ldaca_wordflow/core/onnx_embedder.py` — thin wrapper class:

- `__init__(model_path, provider_preference)`: build an `onnxruntime.InferenceSession` with provider preference (`CoreMLExecutionProvider`, `DmlExecutionProvider`, then `CPUExecutionProvider`)
- `encode(docs: list[str], batch_size: int) -> np.ndarray`: tokenize → run session → mean-pool → L2-normalise. Mirror `SentenceTransformer.encode` shape so it's a drop-in.
- Lazy import of `onnxruntime` and `tokenizers` at instantiation, not at module load (keeps server startup fast).

### Provider selection

Run-time detection, no compile-time branching:

```python
def _select_providers() -> list[str]:
    available = onnxruntime.get_available_providers()
    preferred = [
        "CoreMLExecutionProvider",       # Mac
        "DmlExecutionProvider",          # Windows
        "CUDAExecutionProvider",         # Linux server (if user added CUDA)
        "CPUExecutionProvider",          # universal
    ]
    return [p for p in preferred if p in available] or ["CPUExecutionProvider"]
```

This is platform-agnostic — Linux Docker stays on CPU; Tauri Mac picks CoreML; Tauri Windows picks DirectML; everyone falls back to CPU.

### Quantisation choice

- **First pass:** use the upstream ONNX export (fp32) to validate the path works
- **Second pass:** swap to int8 dynamic quantisation. Benchmark accuracy on a small labelled set before committing.
- Static (calibrated) quantisation is overkill for a sentence encoder; dynamic is fine and avoids needing a calibration corpus.

### Determinism

ONNX Runtime is deterministic for a given model+input on a given provider. The current `torch.manual_seed` calls are only for the BERTopic/UMAP path, not the embedder. Drop the torch block; keep `random.seed` and `np.random.seed`. UMAP has its own `random_state` param (already set).

### Risks

- **Tokenizer mismatch:** ONNX export may package a different tokenizer than `SentenceTransformer` uses — verify embeddings match within float tolerance on a small fixed corpus before declaring phase 1 done.
- **Mean pooling correctness:** `all-MiniLM-L6-v2` uses mean pooling with attention-mask weighting. Get this wrong and topic quality silently degrades.
- **CoreML provider quirks:** sometimes silently falls back to CPU on unsupported ops. Log the actual provider chosen at runtime.
- **`sentence_transformers` may still be imported elsewhere.** Grep before declaring it removable.

### Exit checklist

- [ ] ONNX embedder produces numerically equivalent vectors to torch baseline (cosine sim >0.999) on a 100-doc fixture
- [ ] Topic-modelling test suite passes
- [ ] Throughput on 50k docs: 2× faster on M1 Max CPU, 3× faster on M1 Max with CoreML
- [ ] `pyproject.toml` no longer requires `torch` (transitive or otherwise)
- [ ] `du -sh` of installed deps reduced (record number)
- [ ] `model_prefetch.py` still completes without errors

## Phase 2 — Embedding disk cache

### New module

`src/ldaca_wordflow/core/embedding_cache.py`:

```
class EmbeddingCache:
    def __init__(self, cache_dir: Path, model_id: str, provider_id: str): ...
    def lookup(self, docs: list[str]) -> tuple[np.ndarray, list[int]]:
        """Returns (cached_embeddings, missing_indices)."""
    def store(self, docs: list[str], embeddings: np.ndarray) -> None: ...
    def clear(self) -> None: ...
```

Storage: `{user_data_dir}/embedding_cache/{model_id}__{provider_id}.parquet` with columns `hash` (binary[32]), `embedding` (FixedSizeList[float16, 384]).

### Worker integration

In `worker_tasks_topic.py` after `embedder = _get_embedder(...)`:

1. `cache.lookup(all_docs)` → split into hits + misses
2. Encode only misses
3. `cache.store(missed_docs, missed_embeddings)`
4. Reassemble full embedding array in original order
5. Pass to BERTopic

### Cache key

`sha256(text.encode("utf-8"))` is the natural key. For a 1M-doc corpus that's ~1 second of hashing in pure Python — fine. **If profiling shows hashing is >5% of pipeline time**, consider moving to a polars-text Rust kernel (see polars-text plan). Until then, Python `hashlib` is the right call.

### Cache management

- Add API: `DELETE /workspaces/{id}/topic-modeling/cache` to clear cache (lives in `api/workspaces/analyses/topic_modeling.py`)
- Add the model+provider tuple to filename — different ONNX providers can produce slightly different floats on edge ops, so don't share across providers
- Cap cache size per user (e.g., 2 GB). LRU eviction by file mtime.

### Exit checklist

- [ ] Cold run: cache populated, no functional regression
- [ ] Warm run on identical corpus: <5% of cold-run wall time
- [ ] Warm run after parameter change (different `min_topic_size`): still skips embedding
- [ ] Cache survives backend restart
- [ ] Clear-cache API works and frontend can call it (frontend deferred to phase 4 follow-up if low-priority)

## Phase 3 — Online pipeline for large datasets

### Threshold

Initial: switch when `len(all_docs) > 100_000` OR `sum(len(doc) for doc in all_docs) > 250_000_000`. Make the threshold configurable in code, not in the API initially — adjust after we have telemetry.

### Online configuration

Reference: BERTopic's "Online Topic Modeling" docs.

```
from sklearn.decomposition import IncrementalPCA
from sklearn.cluster import MiniBatchKMeans
from bertopic.vectorizers import OnlineCountVectorizer

topic_model = BERTopic(
    umap_model=IncrementalPCA(n_components=5),
    hdbscan_model=MiniBatchKMeans(n_clusters=K, random_state=...),
    vectorizer_model=OnlineCountVectorizer(stop_words="english", decay=0.005),
    embedding_model=our_onnx_embedder,
    min_topic_size=int(min_topic_size),
)
for chunk in chunks(all_docs, size=10_000):
    topic_model.partial_fit(chunk)
```

### K selection

`MiniBatchKMeans` requires a fixed `n_clusters` upfront. Heuristic: `K = max(10, min(200, int(sqrt(N / 2))))`. Make it overridable via API param `n_clusters` (optional, only for online mode).

### Tradeoffs to surface in UI

- No outlier topic (-1) — every doc gets a topic
- Topics may be less semantically tight than HDBSCAN's
- Trade for: actually completes on GB-scale data

Add a one-liner to the result payload indicating which mode was used so the frontend can show it.

### API change

The route in `api/workspaces/analyses/topic_modeling.py:141` (`run_topic_modeling()`) takes a `min_topic_size`. Add optional fields:

- `force_mode`: `"classic" | "online" | "auto"` (default `"auto"`)
- `n_clusters`: optional int, only used in online mode

Treat as additive — old clients keep working.

### Exit checklist

- [ ] 1 GB synthetic corpus completes on M1 Max in <30 minutes
- [ ] Auto-engages above threshold without user action
- [ ] Force-classic still works for correctness comparison
- [ ] Result payload reports the mode used
- [ ] Tests cover the threshold boundary (just under and just over)

## Phase 4 — Progress + cancel

### Progress reporting

`run_topic_modeling_task` already takes a `progress_callback: Optional[Callable[[float, str], None]]` (line 70). Use it. Currently it's not called from inside the function — phase 4 wires it up.

Stages to report:
- `"Loading embedder"` — once
- `"Embedding documents"` — every 10 chunks (based on `_EMBEDDING_CHUNK_SIZE = 512`)
- `"Reducing dimensions (UMAP)"` / `"Reducing dimensions (IncrementalPCA)"` — once
- `"Clustering"` — once
- `"Extracting topic words"` — once
- `"Writing artifacts"` — once

Progress numeric: weighted estimate. Embedding usually 70% of time on small data, 40% on large (online mode). Hard-code two progress profiles.

### Cancel

`ProcessPoolExecutor` doesn't support cancelling running tasks gracefully. Options:

1. Sentinel file the worker polls between chunks — simple, works
2. Move to `multiprocessing.Process` with `terminate()` — cleaner but needs reworking the worker module

Start with option 1 (sentinel file in artifact_dir, worker checks every chunk). It's enough.

### API

- Add `POST /workspaces/{id}/topic-modeling/tasks/{task_id}/cancel` (writes sentinel)
- The polling endpoint already returns task status — extend with stage + progress numeric

### Exit checklist

- [ ] Long-running task emits stage updates ≥ once per 10 s
- [ ] Cancel button kills the worker within 5 s
- [ ] Cancelled tasks leave no partial artifacts behind

## Phase 5 — MPS for Apple Silicon cold-run performance

**Status:** complete

**Scope:** On Apple Silicon, prefer `SentenceTransformer(device="mps")` over ONNX
Runtime for the embedding step.  The full BERT graph runs on Metal/Neural Engine
as a single unit with no graph-partition overhead, giving ~3× cold throughput vs
the ONNX ARM64 CPU path.

**Files changed:**
- `src/ldaca_wordflow/core/mps_embedder.py` (new) — `is_mps_available()`,
  `get_active_provider_id()`, `MpsEmbedder` class
- `src/ldaca_wordflow/core/worker_tasks_topic.py` — `_get_embedder` branches on
  `is_mps_available()`; MPS path creates `MpsEmbedder`, ONNX path unchanged
- `src/ldaca_wordflow/core/model_prefetch.py` — `_prefetch_topic_embedder` now
  dispatches to `_prefetch_topic_embedder_mps` (downloads ST weights) or
  `_prefetch_topic_embedder_onnx` based on `is_mps_available()`
- `src/ldaca_wordflow/api/workspaces/analyses/topic_modeling.py` — cache-clear
  endpoint uses `get_active_provider_id()` so it clears the right cache file

**Notes:**
- `MpsEmbedder.provider = "MPS"` → separate Parquet cache from ONNX providers
- Graceful fallback: if MPS unavailable at runtime, `MpsEmbedder.__init__` falls
  back to `device="cpu"` with `provider = "CPU-ST"`
- `is_mps_available()` returns `False` on `ImportError` (no torch installed),
  so Windows/Linux workers fall through to ONNX unchanged
- `normalize_embeddings=True` passed to `SentenceTransformer.encode` to match
  the L2-normalised output of `OnnxEmbedder`
- 12 new unit tests; all 232 suite tests pass

## Open questions specific to backend

1. **`sentence-transformers` removability.** Grep the whole backend for `from sentence_transformers` / `import sentence_transformers`. If only `_get_embedder` uses it, we can drop entirely. If `quotation_extractor` or anything else uses it, keep it.
2. **Are there other models that need similar treatment?** spaCy is mentioned in `model_prefetch.py` for quotation extraction. Out of scope for this plan, but flag if the spaCy model has comparable size/perf issues.
3. **Telemetry.** Should we log timing per stage to a file users can share when reporting performance issues? Cheap to add; very useful for triaging.

## Decisions log

| Date       | Decision                                                                         | Rationale |
|------------|----------------------------------------------------------------------------------|-----------|
| 2026-05-04 | Keep BERTopic; replace embedding backend, not the algorithm                      | Smallest blast radius for biggest win |
| 2026-05-04 | Single ONNX wrapper class, not per-platform code                                 | Provider list does platform routing — clean |
| 2026-05-04 | Cache keyed by (text-sha256, model_id, provider_id)                              | Provider differences in floats can break downstream UMAP determinism |
| 2026-05-04 | float16 cache storage                                                            | ~50% disk saving with negligible quality cost on cosine-similarity workloads |
| 2026-05-04 | Online mode auto-engages by threshold                                            | Most users won't know to choose; matches "just works" expectation |
