# Lazy / On-Demand Tokenisation

**Status:** Shipped 2026-05-20.
**Scope:** Backend (`core/derived_columns.py`, `core/tokens_cache.py`), `polars-text` (Rust expression + Python wrapper).

Tokenisation in Wordflow is lazy and on-demand. Clicking **Tokenise** is instant — it stamps the node's plan with a `polars_text.tokenize_with_cache_lookup` expression. The first analysis that touches that node materialises the tokens it actually needs, caches them by content hash, and returns the result. Subsequent analyses are cache hits.

This is also the mechanism that makes tokenised workspaces **cross-machine portable**: the serialised plan carries no absolute paths, so a workspace bundle can be opened on any machine and just works.

## 1) User-visible behaviour

- **Tokenise click**: stamps a lazy expression on the node's plan. No documents are tokenised; no parquet files are written. Instant.
- **First analysis** (concordance tokens mode, token frequency, topic modelling): the lazy expression materialises tokens for the rows the analysis actually consumes, caches them under `LDACA_TOKENS_CACHE_DIR/{user_id}/tokens/`, returns the result. The expression is sized to what's analysed, not to the whole node.
- **Subsequent analyses on the same tokens**: cache hits — fast.
- **Re-tokenise**: same path as a fresh tokenise. Plan rewrite. Re-running an analysis populates the cache afresh on demand.
- **Cross-machine transfer**: a workspace bundle saved on machine A and opened on machine B works without any repair step. The lazy expression's cache root resolves at execution time from the receiver's env; missing files are treated as cache misses.
- **Delete the cache folder mid-session**: next analysis recomputes everything. The expression treats missing files as misses by construction.
- **Storage** scales with what's actually been analysed, not with what's nominally tokenised.

## 2) Why a Rust polars-plugin expression (not a Python UDF)

Workspace plans persist to `.plbin` via `LazyFrame.serialize(format="binary")`. Python `map_batches` UDFs are not serialisable — they survive in-process but break on workspace save/load. Custom Rust expressions registered via `pyo3-polars`'s `polars_expr` derive macro **are** serialisable (same path as `tokenize_with_offsets` and the existing concordance plugin). Round-tripping through `.plbin` is non-negotiable.

## 3) The expression — `tokenize_with_cache_lookup`

Signature (see `polars-text/src/expressions.rs` + `polars-text/polars_text/functions.py`):

```python
polars_text.tokenize_with_cache_lookup(
    expr,
    *,
    user_id: str,
    bucket_filename: str,          # hash of (model, params), stable across machines
    model: str | None = None,
    lowercase: bool = True,
    remove_punct: bool = True,
    require_env_cache_dir: bool = False,
)
```

Takes two polars inputs: the source text column and its precomputed `hash()`. The Python wrapper computes the hash via `pl.col(source).hash()` so polars itself (not our Rust code) owns the hash function — that decouples the expression from any specific polars hash-internal version.

### Execution semantics (per call)

1. Resolve cache dir: `${LDACA_TOKENS_CACHE_DIR}/{user_id}/tokens/`. `mkdir -p` if missing.
2. Glob `<bucket_stem>{.parquet,__delta__*.parquet}` for every file in the bucket and union into an in-memory `HashMap<u64, ListChunked-row>` of cached tokens. First-writer-wins on duplicates (oldest-mtime first).
3. For each input row:
   - **Cache hit** → push cached tokens into the output builder.
   - **Cache miss** → call the same `tokenize_text_with_offsets` backend that `tokenize_with_offsets` uses, push the result, and queue the `(hash, tokens)` pair for the post-loop delta write.
4. If any rows missed, write a fresh `<bucket>__delta__<uuid>.parquet` under an advisory `flock` on `<bucket>.parquet.lock`. Atomic temp + rename so concurrent readers never see a torn parquet.

### Output dtype

Identical to `tokenize_with_offsets`: `List<Struct<token: Utf8, start: Int64, end: Int64>>`. Matches `TOKENS_CACHE_SCHEMA` so downstream consumers (token-frequency Rust paths, concordance Rust paths) work unchanged.

## 4) Concurrency + locking

- **Writes use an advisory `flock`** on `<bucket>.parquet.lock` (POSIX) or a marker-file fallback on Windows. Held across the delta-parquet write.
- **Duplicate writes are harmless.** Even without strict locking, the reader does `scan_parquet([all bucket files]) + unique(keep="first")`, so a content hash showing up in two delta files just means one row gets dropped at read time.
- **Reads never block.** The lock is exclusively for writes. Parquet writes are atomic-on-close (tmp + rename), so partial writes are invisible.

## 5) Cache-dir resolution

- The plan carries only the **bucket filename** (`bert-base-uncased_a1b2c3d4e5f6.parquet`), which is stable across machines because it's a hash of the `(model, params)` tuple.
- The Rust expression reads `LDACA_TOKENS_CACHE_DIR` and the `user_id` kwarg at execution time. Same env var the Python side uses.
- Default umbrella: `{data_root}/.cache/` (dot-prefixed so it's hidden from the data-loader file tree; the data-loader's listing also filters dotfiles explicitly). The backend's lifespan sets the env to this default if not externally configured. Operators / Tauri / conftest can pre-set the env to override.
- Layout: `{LDACA_TOKENS_CACHE_DIR}/{user_id}/tokens/{bucket}__delta__*.parquet`. Future cross-machine-portable per-row caches (lemmatisation, embeddings, …) can live as siblings of `tokens/` under the same `{user_id}/` per-user dir.
- For tests, `tests/conftest.py` sets `LDACA_TOKENS_CACHE_DIR` per session via an autouse fixture.

## 5.1) Compaction

Each cache miss writes a fresh `<bucket>__delta__<uuid>.parquet`. Without compaction a long-lived install would accumulate dozens or hundreds of small deltas per bucket, slowing every cache read (each `scan_parquet` includes a per-file footer parse).

Compaction merges every file in a bucket into one fresh delta when the file count exceeds `DEFAULT_COMPACTION_THRESHOLD` (16). It runs in two places:

1. **Per-tokenise trigger** (`tokenise_column`, after `add_reference`): catches buckets that crossed the threshold during the current session. Cheap when below threshold — just a directory listing.
2. **Startup trigger** (`main.py` lifespan, alongside `sweep_unreferenced`): walks every per-user bucket and compacts those over threshold. Catches cross-session accumulation.

Both triggers go through `compact_bucket_if_needed` / `compact_all_buckets` in `tokens_cache.py`. The merge logic is:
- Read the union of every file in the bucket and dedupe by `__ldaca_content_hash__`.
- Write the merged content to a fresh `<bucket>__delta__<uuid>.parquet`.
- Delete the prior files.

No lock is taken on compaction — the new delta file is brand-new (no concurrent writer targets it) and the old files are only deleted after the new one lands. Readers racing with compaction see either the old files, the new merged delta, or both; the Rust expression's `load_cache_map` dedupes by hash on read (first-writer-wins) so duplication is harmless.

## 6) Python integration

`core/derived_columns.tokenise_column` stamps the lazy expression on the node's plan:

```python
def tokenise_column(node, *, source_column, model, language, user_id, workspace_id=None):
    ...
    bucket_filename = tokens_cache.cache_filename(model, params)
    new_lf = base_lf.with_columns(
        polars_text.tokenize_with_cache_lookup(
            pl.col(source_column),
            user_id=user_id,
            bucket_filename=bucket_filename,
            model=model,
            lowercase=params["lowercase"],
            remove_punct=params["remove_punct"],
        ).alias(derived_name)
    )
    node.data = new_lf
    node.register_derived_column(derived_name, {...})
```

The cache parquet files are managed by the Rust expression itself; Python only owns the manifest (`tokens_cache.add_reference` / `drop_reference` / `sweep_unreferenced`), which lets the sweep reclaim a bucket once no node references it.

## 7) Test coverage

- `polars-text/src/tokens_cache_io.rs` (Rust unit tests): cache dir resolution, bucket-file ordering, delta round-trip, dedup ordering, no-op-on-empty.
- `polars-text/tests/test_tokenize_with_cache_lookup.py` (Python integration): first-collect miss, full-hit reuse, mixed hit/miss delta append, missing env error, empty input, null rows, per-bucket isolation, per-user isolation.
- `backend/tests/unit/test_derived_columns_tokenise.py`: end-to-end that tokenise stamps the expression, that the schema round-trips, that re-tokenise on the same `(source, model)` replaces in place.

## 8) Reusable pattern — lazy on-demand cache for serialisable plans

**When this pattern applies:**

A pre-computed, expensive-per-row transformation `T(x)` feeds into a lazy plan that gets serialised, shared across machines, and may be partially consumed (only some rows ever materialise).

If all four properties hold, the eager-cache + hash-join pattern is the wrong shape. The right shape is:

1. **A Rust plugin expression** registered via `pyo3-polars`'s `#[polars_expr]`, so the plan survives `LazyFrame.serialize(format="binary")` round-trips. (Python `map_batches` UDFs do not survive serialisation.)
2. **Kwargs carry only stable identifiers**: the (model, params) hash, a bucket filename, NO absolute paths.
3. **Execution-time resolution** of the cache root from `LDACA_<DOMAIN>_CACHE_DIR + user_id`. Missing dir → `mkdir -p` and treat as all-misses.
4. **Bucket-delta layout** for concurrent-safe append: `<bucket>.parquet` (canonical) + `<bucket>__delta__<uuid>.parquet` (incremental). Read with `scan_parquet([all]) + unique(keep="first")`.
5. **Side-effecting write inside the expression**, guarded by an advisory `flock` on `<bucket>.lock`. Duplicate writes are harmless because of (4).

**Properties this gives you, for free:**

- **Cross-machine portability** — plans serialise without absolute paths.
- **Partial-collect efficiency** — only the rows the analysis actually pulls get materialised; predicate push-down works through the expression because polars doesn't know it's side-effecting.
- **Concurrent worker safety** — delta-layout + flock means two workers running the same plan don't corrupt the cache.
- **No "missing cache" failure mode** — a missing file is by construction equivalent to a cache miss.
- **Storage scales with actual analysis usage**, not nominal precompute count.

**Candidate next applications inside Wordflow:**

- **Lemmatisation cache** — same shape (model = spaCy / UDpipe model, params = language).
- **Embedding cache** — same shape, modulo larger row payloads.
- **NER / dependency-parse cache** — same shape.
- **Topic-model document representations** — the per-doc representation step (BERTopic's UMAP-reduced embeddings before clustering) is currently re-run every workspace open. Same pattern would help.

When designing any of these, START from the framework in this section rather than from an eager-write pattern.
