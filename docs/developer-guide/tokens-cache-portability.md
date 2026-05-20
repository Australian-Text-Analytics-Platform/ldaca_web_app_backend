# Tokens-Cache Portability (Workspace Load Repair)

**Scope statement:** This page explains why loading a workspace that contains tokenised nodes can crash when the workspace is moved between machines or operating systems, and what the `tokens_cache_repair` module does about it. It is aimed at developers who maintain the tokens cache, the workspace serialiser, or anyone debugging a "first collect after load crashes with a Linux/Mac path on Windows" report.

## 1) What problem is being solved?

**Question:** *Why does a workspace that loads fine on the machine that created it crash on a different machine?*

**Answer:** Tokens cache parquet paths get baked into the serialised lazy plan as absolute strings. They survive the workspace ZIP intact, but the cache files themselves do not travel with the workspace (cache can be gigabytes — it is intentionally not part of the bundle). On the receiver the donor's absolute paths point at nothing, and the first `collect()` against a tokenised node raises `FileNotFoundError`.

The smoking gun in code is `tokens_cache.tokens_cache_lazyframe()`:

```python
pl.scan_parquet([str(p) for p in files])
```

Each `p` is an absolute path resolved at plan-construction time (e.g.
`/home/ubuntu/ldaca/users/<id>/user_cache/tokens/bert_a1b2c3.parquet` on
Nectar). Polars serialises that list verbatim into the node's `.plbin`.

## 2) Why doesn't `rebase_workspace_sources` already fix this?

**Question:** *We already rewrite plan paths on workspace load. Why isn't that enough?*

**Answer:** `docworkspace.workspace.rebase_workspace_sources` only rewrites a path when a same-basename file exists inside the workspace's own `data/` directory:

```python
target = (data_dir / Path(old).name).resolve()
if not target.exists():
    continue
```

Tokens cache files live under `user_cache/tokens/` on the donor — outside the workspace bundle by design — so the rebaser never sees a matching basename and skips them. The donor's absolute path stays in the plan.

## 3) What does the repair pass do?

**Question:** *What is `repair_tokens_cache_paths` actually doing?*

**Answer:** It runs once per workspace load, immediately after `rebase_workspace_sources` and before `Workspace.load()` deserialises any node plans. For every `.plbin` listed in `metadata.json` it walks the scan source paths and, for each path that does not exist on disk:

- If the path **does not look like a tokens cache reference** (no `tokens` component in its directory parts), it is left alone. That keeps the repair scope narrow — other classes of missing parquet still surface as the same errors they always did.
- If it **does** look like a tokens cache reference, the repair tries to relocate it (Case A) or fabricate a stub (Case B).

**Case A — Relocate to the receiver's cache:**
The receiver may already have a cache file with the same basename (e.g. they previously tokenised with the same model + params hash). The basename uniquely identifies a `(model, params)` bucket, so a same-basename match on the receiver is the same bucket. The repair rewrites the plbin to point at the local file via `polars_text.replace_source_paths`.

**Case B — Write an empty stub:**
No same-basename file exists on the receiver. The repair writes a 0-row parquet with the canonical cache schema (`TOKENS_CACHE_SCHEMA`) at the local equivalent path and rewrites the plbin to point at it.

Both cases mean every plan source resolves to an existing local file by the time `Workspace.load()` runs.

## 4) What does "stub" mean for the user?

**Question:** *If we silently substitute an empty parquet, won't the user think their tokenised analyses are broken?*

**Answer:** Yes, until they re-tokenise. The trade-off is:

- The workspace **loads** rather than crashing.
- Joins against the empty stub yield empty token lists for every row, not fabricated tokens — there is no risk of silently wrong results.
- Tokenised analyses (concordance tokens-mode, token-frequency) return zero hits / empty tables.
- The user re-runs the Tokenise dialog on the affected node. The writer resolves `tokens_cache_dir(user_id)` at execution time on the **receiver's** machine, so the new cache lands at the right place. The empty stub is naturally subsumed because the reader does `pl.scan_parquet([all files in bucket])` + `unique(subset=CONTENT_HASH_COLUMN, keep="first")` — the stub's zero rows lose every dedup.

A future follow-up (deferred) is to surface the `TokensCacheRepairReport.stubbed` list to the frontend as a banner reading "Tokens missing for nodes X, Y — re-tokenise to restore." The backend already collects this data; the UI just needs to consume it.

## 5) Why a stub at all? Why not just delete the tokens column?

**Question:** *Could the repair scrub the tokens column from the plan instead of stubbing the cache?*

**Answer:** It could, but that requires rewriting the lazy plan structure (removing a join and propagating the schema change downstream), which is much more complex than fabricating an empty parquet. The stub approach is purely a path-level change: the plan structure is untouched, polars sees the same shape it expected, and the empty list flows naturally through downstream ops.

It also means a later re-tokenisation does not need to mutate the plan back — once the stub is superseded by real delta files in the same bucket, the same plan starts producing real tokens with no further intervention.

## 6) What is the canonical cache schema?

**Question:** *What columns and dtypes does a tokens-cache parquet have?*

**Answer:** `TOKENS_CACHE_SCHEMA` in `core/tokens_cache.py`:

| Column | Dtype | Source of truth |
| --- | --- | --- |
| `__ldaca_content_hash__` | `UInt64` | `pl.col(source).hash()` — polars' hash returns u64 |
| `tokens` | `List(Struct{ token: String, start: Int64, end: Int64 })` | `polars_text.tokenize_with_offsets` FFI plugin (see `polars-text/src/expressions.rs` `Field::new(...)` declarations) |

If the FFI plugin's output ever changes shape, `TOKENS_CACHE_SCHEMA` is the single point of truth that both the writer (`write_or_append_cache`) and the stub creator (`_write_stub_parquet`) consult. Schema drift would surface as a polars schema-mismatch error rather than silent corruption.

## 7) Why is `LDACA_TOKENS_CACHE_DIR` still useful?

**Question:** *If the cache path is fully managed by the repair pass, do we still need the env var override?*

**Answer:** Yes. The env var controls **where the receiver's cache root lives**, not what is in the plan. Tauri builds and test fixtures set `LDACA_TOKENS_CACHE_DIR` to relocate the cache out of the bundle (Tauri) or into a tmp dir (tests). The repair pass calls `tokens_cache_dir(user_id)` which honours the override, so stub creation and Case A relocation both land at whatever path the override resolves to. There is no separate config knob to keep in sync.

## 8) What about the Topic Modelling embedding cache?

**Question:** *Does the BERTopic / sentence-transformer embedding cache have the same issue?*

**Answer:** No. `core/embedding_cache.py` uses `pl.read_parquet(self._path)` — eager, executed in the topic-modelling worker, returning numpy arrays. The path never enters a LazyFrame plan, so nothing about the embedding cache gets baked into the workspace's `.plbin` files. Embedding caches on the donor are simply ignored on the receiver; on cache miss the worker re-embeds. The repair pass deliberately does not touch the embedding cache.

The architectural reason this works for embeddings but not tokens: embeddings feed `BERTopic.fit()` directly and never become a workspace node's column. Tokens become a derived column on a node, which is composed lazily with downstream analyses (concordance, token-frequency), so the cache has to be in the plan.

## 9) Where does the repair pass run?

**Question:** *What is the call site, and what invariants does it assume?*

**Answer:** [`core/workspace.py:172-181`](../../src/ldaca_wordflow/core/workspace.py), inside `WorkspaceManager.load_workspace`:

```python
# 3. Rebase plbin source paths to the finalized folder.
rebase_workspace_sources(updated_dir)

# 3b. Repair tokens-cache scan paths. ...
repair_tokens_cache_paths(updated_dir, user_id)

# 4. Full load (deserialize nodes — paths are now correct).
new_ws = Workspace.load(updated_dir)
```

Order matters:

1. **`rebase_workspace_sources` first** because it handles the common case (workspace-internal `data/` parquets) more efficiently, and its rewrites can produce paths that the repair pass would then need to ignore.
2. **`repair_tokens_cache_paths` second** because by this point every "easy" path has been handled and the only remaining stale paths are the cross-machine cache references this module is responsible for.
3. **`Workspace.load` last** because it triggers `LazyFrame.deserialize`, and we want every source path to already point at an existing file by then.

The repair pass is idempotent — running it twice produces the same final state — so a workspace that has already been repaired (Case B stubs in place) just no-ops on the second pass.

## 10) What changed in the codebase?

**Question:** *What are the concrete files and commits?*

**Answer:**

- New: [`core/tokens_cache_repair.py`](../../src/ldaca_wordflow/core/tokens_cache_repair.py) — repair function + `TokensCacheRepairReport` dataclass.
- New: [`tests/unit/test_tokens_cache_repair.py`](../../tests/unit/test_tokens_cache_repair.py) — Case A, Case B, no-op, conservative scope, idempotence, end-to-end collect-against-stub.
- Modified: [`core/tokens_cache.py`](../../src/ldaca_wordflow/core/tokens_cache.py) — exported `TOKENS_CACHE_SCHEMA` and `TOKENS_CACHE_SUBDIR` so the repair pass and any future caller don't re-declare the canonical types.
- Modified: [`core/workspace.py`](../../src/ldaca_wordflow/core/workspace.py) — call site between `rebase_workspace_sources` and `Workspace.load`.

The backend lands as a single commit; the Wordflow super-repo bumps the backend submodule pointer.

## 11) What is explicitly out of scope?

**Question:** *What does this fix not address?*

**Answer:**

- **Carrying cache files inside the workspace bundle.** Cache can be tens of GB and is not part of the workspace by design. This fix accepts that and degrades gracefully when the cache is missing.
- **Re-deriving tokens at load time.** The repair pass does not call the tokeniser. The user re-runs the Tokenise dialog when they want real tokens back.
- **UI surface for "stubbed nodes — re-tokenise to restore".** The backend exposes this via `TokensCacheRepairReport.stubbed`; the frontend banner is a follow-up.
- **Re-enabling the workspace upload/download UI.** Currently gated by `WORKSPACE_TRANSFER_ENABLED=false` in `WorkspaceManagerCard.tsx`. With this fix the gate can flip back to `true`, but you may want to wait for the UI banner so users aren't confused by empty token columns after a transfer.

## Recap

**Question:** *What should I take away from this page?*

**Answer:** Tokens cache paths are baked absolute into lazy plans. The existing rebaser only handles paths inside the workspace bundle. `repair_tokens_cache_paths` runs after the rebaser and either relocates donor paths to the receiver's local cache (when the bucket exists locally) or fabricates a 0-row stub (when it doesn't), so workspace load never crashes on cross-machine transfer. The cost is empty token lists until the user re-tokenises — the writer resolves cache paths dynamically, so the new cache always lands at the correct local location.
