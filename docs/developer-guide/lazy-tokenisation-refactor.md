# Lazy / On-Demand Tokenisation Refactor — Design Doc

**Status:** Proposal, awaiting sign-off before implementation.
**Scope:** Backend (`core/derived_columns.py`, `core/tokens_cache.py`), `polars-text` (Rust + Python wrapper), frontend (banner UX confirmation only — no logic change).
**Trigger:** User report 2026-05-20 — "Re-tokenise all" on a cross-machine workspace import takes 15 s for a 70 MB cache, projected minutes for GB-scale workspaces. Blocks user on the banner while a substantial portion of the tokenised content may never be analysed.

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

## 8) Old-plan migration

**Question:** *Existing workspaces have hash-join plans (`scan_parquet([bucket files]) ⋈ hash(source)`). What happens on load?*

**Answer:**

- **Read path stays compatible.** `tokens_cache_lazyframe` keeps working; the new expression is additive, not a replacement.
- **No automatic plan rewrite.** Old plans continue to use the hash-join path; new tokenise clicks use the lazy expression. Workspaces gradually convert as users re-tokenise.
- **Cross-machine repair (`repair_tokens_cache_paths`) stays.** Old-style plans still need the stub-parquet fix. The lazy expression itself doesn't reference parquet paths so it's immune to the cross-machine issue.
- **Explicit migration command** (deferred to follow-up): `POST /workspaces/migrate-tokens-plans` that walks every node's plan, detects hash-join shape, and rewrites to the lazy expression. Tabled until the new path proves stable in field use.

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

## 11) Rollout

**Question:** *What's the safest sequencing?*

**Answer:**

- **Phase 1** — Rust expression + python wrapper + Rust unit tests. No Python integration yet. Behind no flag because nothing calls it.
- **Phase 2** — Wire `_build_lazy_plan` into `tokenise_column`; keep `_upsert_for_node` available as a callable. Add a feature flag `LDACA_LAZY_TOKENISE` (env var) defaulting to **false**; when true, `tokenise_column` uses the lazy path. Lets us flip the flag in a Tauri build first.
- **Phase 3** — Flip default to true once Phase 2 has 1–2 weeks of clean field signal.
- **Phase 4** — Remove the flag and the now-dead `_upsert_for_node` call site. Keep the function itself in case a future "warm the cache" UI wants it.

## 12) Out of scope (explicitly deferred)

- **Automatic old-plan migration on workspace load.** Tabled until we have the new path stable; users gradually convert by re-tokenising. Old plans continue to work.
- **Cache-size limits / LRU eviction on the lazy-fill path.** The current sweep code (`sweep_unreferenced`) already handles unreferenced buckets; per-bucket size budgets are a separate concern.
- **Streaming output across plan boundaries.** Even with on-demand inside a single node, the analysis worker still collects at the end. Streaming end-to-end through token-frequency / concordance is a larger architectural change.
- **Tauri behaviour audit.** Tauri's worker bootstrap sets `LDACA_TOKENS_CACHE_DIR` already; should JustWork™ but worth a manual check before flipping the default.

## 13) Open questions for review

1. **Cache write batching.** Per-batch (default Polars chunk size ~64k rows) or accumulate across multiple batches before writing? Per-batch is simpler; multi-batch reduces parquet file count at the cost of holding more in RAM. Recommendation: per-batch.
2. **Telemetry.** Should the Rust expression log how many hits vs misses per call? Useful for debugging but adds log volume. Recommendation: log at DEBUG only.
3. **Migration cutover.** Do we want the flag period? Or commit to "all new tokenisations are lazy" the moment Phase 2 lands? Recommendation: keep the flag, default false, flip after a quiet week.
4. **`is_elementwise` setting.** Need to confirm via experimentation. Likely false (we maintain state across rows in a batch — the cache map). If false, polars may execute the expression less efficiently but correctness is guaranteed.

Sign off the open questions and I'll start Phase 1.
