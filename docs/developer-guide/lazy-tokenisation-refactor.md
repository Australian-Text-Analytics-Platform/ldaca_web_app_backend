# Lazy / On-Demand Tokenisation Refactor — Design Doc

**Status:** Approved 2026-05-20 (Option A — auto-migrate + retire repair infrastructure).
**Scope:** Backend (`core/derived_columns.py`, `core/tokens_cache.py`, `core/tokens_cache_repair.py`), `polars-text` (Rust + Python wrapper), frontend (banner removal in Phase 4.5).
**Trigger:** User report 2026-05-20 — "Re-tokenise all" on a cross-machine workspace import takes 15 s for a 70 MB cache, projected minutes for GB-scale workspaces. Blocks user on the banner while a substantial portion of the tokenised content may never be analysed.

## 0) Key insight — this also eliminates the cross-machine portability problem

The repair pass + sidecar + banner + bulk-retokenise + friendly-error preflight machinery (~700 lines across backend and frontend, shipped late 2026-05) exists because the **eager hash-join plan bakes absolute parquet paths into the serialised `.plbin`**. Move the workspace to a different machine and those paths point nowhere.

The lazy expression doesn't carry parquet paths. It carries only:

- the **bucket filename** (a hash of `model + tokeniser params`, stable across machines), and
- the **tokeniser kwargs** (`model`, `lowercase`, `remove_punct`).

The cache root is resolved at **execution time** from `LDACA_TOKENS_CACHE_DIR + user_id`. On a fresh machine the dir is missing → it's `mkdir -p`'d and treated as a cache miss (compute on the fly, write the result, return). The user never sees a banner because there's nothing to repair.

**Consequence for the rollout:** once every workspace's tokenisation step has been migrated to the lazy expression (Phase 2.5 auto-migration), the entire repair infrastructure becomes dead code and is deleted in Phase 4.5. This is the central justification for Option A (auto-migrate) over Option B (parallel paths forever).

The pattern generalises beyond tokens — see §14 for the reusable framework.

## 1) What changes (the user-visible promise)

**Question:** *What does "on-demand tokenisation" mean from the user's point of view?*

**Answer:**

- Clicking **Tokenise** on a fresh block is instant. The plan is stamped with a lazy "tokenise-with-cache-lookup" expression; **no documents are tokenised yet**.
- The **first analysis** (concordance tokens mode, token-frequency, topic modelling) that touches a tokenised node materialises the tokens it actually needs, caches them by content hash, and returns the result. Subsequent analyses are cache hits.
- **Re-tokenise** (banner button + title-bar button) becomes a plan rewrite, not a cache write. Banner clears immediately; the cache fills incrementally as the user runs analyses.
- **Storage** scales with what was actually analysed, not what was nominally tokenised.

## 2) Why the current design isn't this

**Question:** *What does today's tokenise click actually do?*

**Answer:** See `_upsert_for_node` in `core/derived_columns.py:177-251`. Three steps run in order at every Tokenise click:

1. Lazy plan: `source → hash → unique → anti-join cached hashes`.
2. **`.collect()`** — *eager* materialisation of the entire missing-hashes frame.
3. `write_or_append_cache(...)` — writes a delta parquet.

Only *after* those three eager steps does the node's `data` get rewritten to the cache-join plan that downstream analyses consume lazily. The user's intuition that tokenisation is on-demand is incorrect — only consumption is lazy; production is eager.

## 3) The chosen design — Rust plugin expression with side-effecting cache write

**Question:** *Why a Rust plugin and not a Python UDF (`map_batches`)?*

**Answer:** Workspace plans persist to `.plbin` via `LazyFrame.serialize(format="binary")`. Python UDFs registered via `map_batches` are not serialisable through plbin — they survive in-process but break on workspace save/load. Custom Rust expressions registered via `pyo3-polars`'s `polars_expr` derive macro **are** serialisable (same path as `tokenize_with_offsets` and the existing concordance plugin). Round-tripping through plbin is non-negotiable; Python UDFs are off the table.

## 4) New expression — `tokenize_with_cache_lookup`

**Question:** *What's the API shape?*

**Answer:**

```rust
#[polars_expr(output_type_func=list_token_struct_output)]
pub fn tokenize_with_cache_lookup(
    inputs: &[Series],
    kwargs: TokenizeWithCacheKwargs,
) -> PolarsResult<Series>
```

```rust
#[derive(Deserialize)]
struct TokenizeWithCacheKwargs {
    // Tokeniser knobs (same as tokenize_with_offsets):
    model: String,
    lowercase: bool,
    remove_punct: bool,

    // Cache plumbing — bucket-relative; the cache root is resolved at
    // EXECUTION time from `LDACA_TOKENS_CACHE_DIR` + user_id so cross-machine
    // transfer doesn't bake any absolute path into the serialised plan.
    bucket_filename: String,   // e.g. "bert-base-uncased_a1b2c3d4e5f6.parquet"

    // Set to true when running inside an analysis worker process, where the
    // env var has been set by the worker-bootstrap. The library defaults to
    // false and panics with a clear message if the env var is missing — so
    // a misconfigured worker fails loudly instead of silently re-computing.
    require_env_cache_dir: bool,
}
```

Python wrapper:

```python
def tokenize_with_cache_lookup(
    expr, *, model: str, lowercase: bool, remove_punct: bool, bucket_filename: str
):
    return register_plugin_function(
        plugin_path=Path(__file__).parent,
        function_name="tokenize_with_cache_lookup",
        args=[expr],
        kwargs={
            "model": model,
            "lowercase": lowercase,
            "remove_punct": remove_punct,
            "bucket_filename": bucket_filename,
            "require_env_cache_dir": True,
        },
        is_elementwise=False,  # operates per batch, needs the whole chunk
        returns_scalar=False,
    )
```

### Execution semantics, per call

1. Read `LDACA_TOKENS_CACHE_DIR` from the environment + the runtime-derived user id (see §6 for the resolution rule).
2. Glob `<cache_root>/<user_id>/tokens/<bucket_stem>{,__delta__*}.parquet`. Build an in-memory `HashMap<u64, ListChunked-row>` of cached tokens.
3. For each input row:
   - Compute hash of `source` (matches `pl.col(source).hash()` — `xxHash64`).
   - **Cache hit** → push cached tokens row into the output builder.
   - **Cache miss** → call existing `tokenize_text_with_offsets` (same Rust function `tokenize_with_offsets` already uses), push result into both the output builder AND a "to-write" buffer.
4. If `to-write` is non-empty, append a new delta parquet `<bucket_stem>__delta__<uuid>.parquet` under an advisory file lock (see §5).
5. Return the assembled `tokens` Series.

### Output dtype

Identical to `tokenize_with_offsets`: `List<Struct<token: Utf8, start: Int64, end: Int64>>`. Matches `TOKENS_CACHE_SCHEMA` so downstream consumers (token-frequency Rust paths, concordance Rust paths) work unchanged.

## 5) Concurrency + locking

**Question:** *Multiple workers may execute the same plan concurrently. What stops two workers from writing duplicate delta parquets or corrupting the cache?*

**Answer:**

- **Write path uses an advisory `flock`** on `<cache_root>/<user_id>/tokens/<bucket>.lock`. Acquired before the delta-parquet write, released after. The existing Python `_file_lock` helper already uses this pattern; the Rust expression takes the same lock so Python and Rust paths can coexist during the migration window.
- **Duplicate writes are harmless.** Even without strict locking, the reader does `scan_parquet([all bucket files]) → unique(subset=[CONTENT_HASH_COLUMN], keep="first")`, so a content hash showing up in two delta files just means one row gets dropped at read time.
- **Reads never block.** The lock is exclusively for writes. Reads always see whatever delta files exist at the time of scan — including partial writes from in-flight delta files, but parquet write is atomic-on-close so partials never expose torn data.

## 6) Cache-dir resolution

**Question:** *How does the Rust expression find the cache without baking absolute paths into the plan?*

**Answer:**

- The plan carries only the **bucket filename** (`bert-base-uncased_a1b2c3d4e5f6.parquet`), which is stable across machines because it's a hash of the (model, params) tuple.
- The expression reads `LDACA_TOKENS_CACHE_DIR` and the per-user subdir at execution time. Same env var the rest of the cache code uses; no new config surface.
- For tests and Tauri builds the env var override already works; nothing changes.
- If the env var is missing at execution time (misconfigured worker) the expression returns an error with the path it was looking for — fail loud, not silent compute-and-discard.

## 7) Python integration

**Question:** *What changes in `core/derived_columns.py`?*

**Answer:**

```python
# Today:
def tokenise_column(node, source_column, model, language, user_id, workspace_id=None):
    ...
    cache_path = _upsert_for_node(...)           # ← removed in new flow
    new_lf = _build_cache_join(...)              # ← replaced with _build_lazy_plan
    node.data = new_lf
    node.register_derived_column(...)
    ...

# New:
def tokenise_column(node, source_column, model, language, user_id, workspace_id=None):
    ...
    # No eager work. The plan carries a tokenize_with_cache_lookup
    # expression; the cache is populated when an analysis runs.
    new_lf = _build_lazy_plan(
        base_lf=node.data,
        source_column=source_column,
        model=model,
        params=params,
        derived_name=derived_name,
    )
    node.data = new_lf
    node.register_derived_column(...)
    ...

def _build_lazy_plan(*, base_lf, source_column, model, params, derived_name):
    bucket_filename = tokens_cache.cache_filename(model, params)
    return base_lf.with_columns(
        polars_text.tokenize_with_cache_lookup(
            pl.col(source_column),
            model=model,
            lowercase=params["lowercase"],
            remove_punct=params["remove_punct"],
            bucket_filename=bucket_filename,
        ).alias(derived_name)
    )
```

`_upsert_for_node` stays as a callable helper (still useful for explicit "warm the cache for this node" flows) but is no longer invoked from `tokenise_column`.

## 8a) Migration limitation discovered during Phase 2.5 field test (2026-05-20)

The Phase 2.5 auto-migration walker overlays the lazy expression on top of the eager hash-join plan rather than surgically removing the join (the design doc originally said "replace it"). The reason is empirical: a polars `LEFT JOIN` whose output column is dropped is NOT pruned by polars 1.40's optimizer — the join is retained on the off-chance that it could change row count (polars doesn't know our cache parquet is unique on `__hash__`).

Tested:
```python
plan.drop("derived_tokens").explain(optimized=True)
# Still shows the LEFT JOIN; only the projection on the right narrows
```

Consequences:

- **Freshly lazy-tokenised nodes** (built via `_build_lazy_plan` from scratch) are pure lazy — no `scan_parquet([absolute paths])` anywhere in the plan. They survive cache-folder deletion mid-session because the lazy expression treats missing files as cache misses and recomputes.

- **Migrated nodes** (eager plans that picked up the lazy overlay on load) still carry the eager `scan_parquet([cache_paths])` in their plan. The lazy expression's output shadows the eager output via the same `with_columns(...alias(name))` so analyses produce correct results — BUT the underlying scan still needs the files to *exist*. The Phase 2.5 wiring calls `repair_tokens_cache_paths` at load time which writes 0-row stub parquets where files are missing, so workspace loads succeed. Mid-session deletion of those stubs breaks the plan again.

User workaround for migrated nodes that need full portability: re-tokenise once via the Workspace Graph view. That goes through the lazy `tokenise_column` path and produces a clean pure-lazy plan with no eager substrate. After that, the node is fully portable (deletion-tolerant, cross-machine-clean).

Long-term path to full migration: Phase 4.5 deletes the eager `_upsert_for_node` + `_build_cache_join` paths entirely. Once that lands, any node still on the eager plan shape will fail to construct, so the user MUST re-tokenise — which gives us the clean lazy plan as a side effect. At that point the overlay-vs-replace distinction disappears (only one shape exists).

For the Phase 2.5 soak window: gate the friendly-preflight check (`assert_tokens_available_for_nodes`) and the banner detection (`_runtime_tokens_cache_state`) behind `LDACA_LAZY_TOKENISE` so they don't surface confusing "missing tokens" errors for nodes whose actual output comes from the lazy overlay.

## 8) Old-plan migration (auto-migrate on load — Option A)

**Question:** *Existing workspaces have hash-join plans (`scan_parquet([bucket files]) ⋈ hash(source)`). What happens on load?*

**Answer (revised — auto-migrate path, Option A):**

- **Read path stays compatible** during the migration window. `tokens_cache_lazyframe` keeps working; the new expression is additive.
- **Auto-rewrite on workspace load** (Phase 2.5). When a workspace is opened, walk every node's lazy plan and detect the hash-join shape:
  ```
  base_lf
    .with_columns(pl.col(source).hash().alias("__ldaca_content_hash__"))
    .join(scan_parquet([bucket files]), on="__ldaca_content_hash__", how="left")
  ```
  Replace it with:
  ```
  base_lf
    .with_columns(
      polars_text.tokenize_with_cache_lookup(
        pl.col(source),
        model=..., lowercase=..., remove_punct=...,
        bucket_filename="<bucket>.parquet",
      ).alias(derived_name)
    )
  ```
  The (model, params) tuple comes from the node's `derived_columns` metadata (already persisted). The bucket filename is recomputed from those, so it's self-consistent across machines.
- **Migration runs once per node**, results are saved back to the node, the workspace's next save persists the new plan shape.
- **Edge cases:**
  - Plan doesn't match the hash-join shape (custom user plan, already-lazy, unfamiliar) → leave untouched, log debug.
  - `derived_columns` metadata missing → leave untouched, log warning, surface in repair-state list (so the legacy banner can still fire as a fallback during the transition).
  - Migration failure (exception inside the walk) → leave untouched, log warning, do not block load.
- **The repair pass stays alive during Phases 2.5 → 4.5** as a belt-and-braces safety net: any plan that auto-migration *didn't* handle still gets the stub-parquet fix on load.
- **In Phase 4.5** (after a 2-week soak with auto-migration enabled and no field reports), the repair pass + sidecar + banner + bulk-retokenise + friendly-error preflight all get deleted in a single focused commit. See §15.

## 9) Performance characteristics — what to expect

**Question:** *How does this compare to eager on real data?*

**Answer:**

| Scenario | Eager (today) | Lazy (proposed) |
|---|---|---|
| Tokenise click on a 50k-doc block | ~30s blocking | <100ms |
| First concordance on a freshly-tokenised block | ~10ms (cache hit) | ~30s blocking (cache miss, materialises all 50k) |
| Second concordance | ~10ms | ~10ms (cache populated) |
| Token-frequency filtered to 1k rows | tokenises all 50k upfront | tokenises only 1k (via predicate push-down) |
| Re-tokenise after cross-machine import | ~15s per 70MB | <500ms (plan rewrite only) |
| Workspace with 50 derived blocks tokenised, 5 analysed | 50× tokenisation cost | 5× tokenisation cost |

The user's "tokenise many, analyse few" workflow is the dominant case; the proposal is a strict win there. Workflows that tokenise once and run many analyses pay the same total cost, just shifted from tokenise click to first-analysis click.

## 10) Test plan

**Question:** *What proves this works?*

**Answer:**

- **Rust unit tests** in `polars-text/tests/`:
  - Empty cache + tokenise → all rows go through compute path, delta file written, return shape matches.
  - Pre-populated cache covering all input hashes → no compute calls, no delta written.
  - Mixed: half cached, half new → split correctly, delta contains only the new ones.
  - Concurrent writes from two threads → no torn parquet, dedup works on read.
  - Missing `LDACA_TOKENS_CACHE_DIR` env var → returns a `polars` error with the lookup path.

- **Python integration tests** in `tests/unit/test_tokens_cache.py`:
  - Tokenise click writes no parquet eagerly; bucket dir stays empty until a collect runs.
  - First `node.data.collect()` populates the cache; second is a no-op.
  - Slicing the plan (`node.data.filter(...).collect()`) only tokenises the surviving rows.
  - Cross-machine repair test moves to "no-op for new lazy plans" assertion.

- **Workspace round-trip test** (new): save a workspace with a lazy-tokenised node, reload from disk, confirm `node.data.collect()` still works and uses the same cache bucket.

- **Field perf check**: rerun the user's 70 MB-cache scenario and report the timing delta (banner-clear-after-re-tokenise: from ~15s to <1s).

## 11) Rollout (revised — Option A)

**Question:** *What's the safest sequencing?*

**Answer:**

- **Phase 1** — Rust expression + Python wrapper + Rust unit tests. No Python integration yet. Behind no flag because nothing calls it.
- **Phase 2** — Wire `_build_lazy_plan` into `tokenise_column`; keep `_upsert_for_node` available as a callable. Add a feature flag `LDACA_LAZY_TOKENISE` (env var) defaulting to **false**; when true, `tokenise_column` uses the lazy path. Lets us flip the flag in a Tauri build first.
- **Phase 2.5** — Auto-migrate hash-join plans on workspace load (see §8). Gated by `LDACA_LAZY_TOKENISE`: when off, no migration runs and the old repair pass continues to handle broken plans. When on, every load attempts migration before the repair pass runs. Add tests for the four edge cases.
- **Phase 3** — Flip default to true once Phase 2 + 2.5 have 1–2 weeks of clean field signal. Update the rollback runbook: setting `LDACA_LAZY_TOKENISE=false` reverts to the eager-with-repair path with no plan corruption (migrated plans still work; un-migrated plans take the repair pass).
- **Phase 4** — Remove the flag and the now-dead `_upsert_for_node` call site. Keep `_upsert_for_node` itself in case a future "warm the cache" UI wants it.
- **Phase 4.5** — Delete the now-vestigial repair machinery in a single focused commit. See §15 for the exact deletion list.

## 12) Out of scope (explicitly deferred)

- **Cache-size limits / LRU eviction on the lazy-fill path.** The current sweep code (`sweep_unreferenced`) already handles unreferenced buckets; per-bucket size budgets are a separate concern.
- **Streaming output across plan boundaries.** Even with on-demand inside a single node, the analysis worker still collects at the end. Streaming end-to-end through token-frequency / concordance is a larger architectural change.
- **Tauri behaviour audit.** Tauri's worker bootstrap sets `LDACA_TOKENS_CACHE_DIR` already; should JustWork™ but worth a manual check before flipping the default.
- **Other derived-column kinds (lemmatised text, embeddings).** This refactor is scoped to tokenisation. The reusable pattern in §14 is the foundation for those follow-ups.

## 13) Open questions — DECIDED 2026-05-20

1. **Cache write batching.** **Decision:** per-batch. Simpler; downstream `unique(keep="first")` already dedupes on read.
2. **Telemetry.** **Decision:** log hits/misses at DEBUG only. Surface in tests via a thread-local counter rather than parsing logs.
3. **Migration cutover.** **Decision:** keep the flag, default false, flip after a 2-week soak with no field issues.
4. **`is_elementwise` setting.** **Decision:** `false`. The expression maintains state across rows in a batch (the in-memory cache map).

## 14) Reusable pattern — lazy on-demand cache for serialisable plans

**When this pattern applies:**

A pre-computed, expensive-per-row transformation T(x) feeds into a lazy plan that gets serialised (`.plbin` or equivalent), shared across machines, and may be partially consumed (only some rows ever materialise).

If all four of those properties hold, the eager-cache + hash-join pattern is the wrong shape. The right shape is:

1. **A Rust plugin expression** registered via `pyo3-polars`'s `#[polars_expr]`, so the plan survives `LazyFrame.serialize(format="binary")` round-trips. (Python `map_batches` UDFs do not survive serialisation.)
2. **Kwargs carry only stable identifiers**: the (model, params) hash, a bucket filename, NO absolute paths.
3. **Execution-time resolution** of the cache root from `LDACA_<DOMAIN>_CACHE_DIR + user_id`. Missing dir → `mkdir -p` and treat as all-misses.
4. **Bucket-delta layout** for concurrent-safe append: `<bucket>.parquet` (canonical) + `<bucket>__delta__<uuid>.parquet` (incremental). Read with `scan_parquet([all]) + unique(keep="first")`.
5. **Side-effecting write inside the expression**, guarded by an advisory `flock` on `<bucket>.lock`. Duplicate writes are harmless because of (4).
6. **Plan-shape auto-migration on load** for any older eager+hash-join plans, so users don't carry two incompatible serialised shapes.

**Properties this gives you, for free:**

- **Cross-machine portability** — plans serialise without absolute paths.
- **Partial-collect efficiency** — only the rows the analysis actually pulls get materialised; predicate push-down works through the expression because polars doesn't know it's side-effecting.
- **Concurrent worker safety** — delta-layout + flock means two workers running the same plan don't corrupt the cache.
- **No "missing cache" failure mode** — a missing file is by construction equivalent to a cache miss.
- **Storage scales with actual analysis usage**, not nominal tokenisation count.

**Candidate next applications inside Wordflow:**

- **Lemmatisation cache** — same shape (model = spaCy/UDpipe model, params = language). Currently doesn't exist; users do this manually. If added later, build it lazy from day 1.
- **Embedding cache** — same shape, modulo larger row payloads. The bucket-filename + delta layout matters more because re-embedding is even more expensive than re-tokenising.
- **NER / dependency-parse cache** — same shape.
- **Topic-model document representations** — the per-doc representation step (BERTopic's UMAP-reduced embeddings before clustering) is currently re-run every workspace open. Same pattern would help.

When designing any of these, START from the §14 framework rather than the eager-write pattern. The eager pattern's portability cost is hidden until the workspace moves; the lazy pattern doesn't have that cost.

## 15) Phase 4.5 — deletion list

Once Phase 3 has soaked for 2 weeks with no field issues, this commit lands in a single focused PR titled `chore: retire tokens-cache repair infrastructure (replaced by lazy expression)`. Code to delete:

**Backend:**
- `backend/src/ldaca_wordflow/core/tokens_cache_repair.py` (entire file)
- `backend/tests/unit/test_tokens_cache_repair.py` (entire file)
- The `tokens_cache_repair` field on the workspace-graph response in `backend/src/ldaca_wordflow/api/workspaces/graph.py` (and the schema)
- `bulk_retokenise_nodes` endpoint in `backend/src/ldaca_wordflow/api/workspaces/analyses/derived_columns.py`
- The friendly missing-tokens preflight in concordance/topic-modelling/token-frequency analyses (the lazy expression makes it impossible to have a "missing tokens" state)
- `repair_tokens_cache_paths` call site in workspace-load (the load no longer needs repair)
- Any `_file_lock` / advisory-flock helper in Python — the Rust expression owns this now

**Frontend:**
- `frontend/src/features/workspace/graph-view/components/TokensCacheRepairBanner.tsx`
- The banner mount point in `frontend/src/components/layout/WorkspaceView.tsx`
- `bulkRetokenise` method on `frontend/src/api/nodes.ts`
- The `tokens_cache_repair` field handling in `useWorkspaceData` / `useWorkspaceGraph`
- `sessionStorage` dismiss-key constant (no longer needed)

**Docs:**
- `backend/docs/developer-guide/tokens-cache-portability.md` (entire file — superseded by this doc's §0 and §14)
- The portability section of any user-facing docs

**Expected diffstat:** ~700 lines deleted, ~50 lines added (mostly the migration walker tests in Phase 2.5 remain). Net reduction is the central win of Option A.

**Rollback plan if Phase 4.5 surfaces an issue:** revert the commit. The lazy expression and auto-migration stay; the repair infrastructure comes back. No data loss because no data was migrated, only plan shapes.
