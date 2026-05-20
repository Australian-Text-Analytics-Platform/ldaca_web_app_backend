"""Per-user, content-addressed cache of tokenisation results.

The lazy `polars_text.tokenize_with_cache_lookup` Rust expression in
each tokenised node's plan writes its delta parquets into this layout.
This Python module owns the surrounding bookkeeping: the manifest
(which workspace/node references each bucket), the reference-tracking
API (`add_reference`, `drop_reference`, …) so the sweep can reclaim
buckets with no live references.

Layout: ``{LDACA_TOKENS_CACHE_DIR}/{user_id}/tokens/``.
  * ``manifest.json`` — per-user manifest of bucket references
  * ``{model}__{params_hash}.parquet`` — legacy single-file bucket
  * ``{model}__{params_hash}__delta__{uuid}.parquet`` — delta parquet
    appended by each `tokenize_with_cache_lookup` cache miss
  * ``{model}__{params_hash}.parquet.lock`` — advisory flock for writes

Schema for every cache parquet (legacy + delta):
  * ``__ldaca_content_hash__: UInt64`` — polars' `Series.hash()` of the
    source text
  * ``tokens: List<Struct<token: String, start: Int64, end: Int64>>``

The env var is set at backend startup (`main.py` lifespan) if not
externally configured, so the Python and Rust sides always resolve to
the same directory. See `developer-guide/lazy-tokenisation-refactor.md`
for the full design.

Collision risk: 64-bit hashes are fine for personal-corpora scale
(low collision probability up to ~10⁸ unique documents). Document
collisions WOULD produce wrong tokens for one of the colliding rows;
upgrade to blake3 / sha-2 if this becomes observable.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import os
import shutil
import tempfile
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator, Optional

import polars as pl


logger = logging.getLogger(__name__)

# POSIX file-lock support; on Windows the import is absent and the
# context manager below degrades to a no-op (single-user desktop case).
try:
    import fcntl  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover — Windows path
    fcntl = None  # type: ignore[assignment]


# When set, points at a base directory under which each user gets a
# ``{base}/{user_id}/`` subdir. Used by the test suite (autouse fixture
# in conftest) and by Tauri builds that need to relocate caches away
# from the bundle.
CACHE_ROOT_ENV = "LDACA_TOKENS_CACHE_DIR"

MANIFEST_FILENAME = "manifest.json"
TOKENS_CACHE_SUBDIR = "tokens"
DEFAULT_GRACE_PERIOD_DAYS = 7

# Filename infix that marks a delta file. Each upsert writes one
# ``<bucket>__delta__<uuid>.parquet`` instead of merging into a single
# shared file — readers union all files for the bucket. The chosen infix
# is impossible to produce inside a bucket key (model is sanitised, hash
# is hex), so the glob never matches across buckets.
DELTA_INFIX = "__delta__"

# Compact when the bucket has more than this many delta files. Reading
# many small parquets pays a per-file footer cost; compaction merges
# them back into one.
DEFAULT_COMPACTION_THRESHOLD = 16

# The hashed-content key column that the join uses. Kept here so callers
# don't accidentally collide with a user column of the same name on a
# different code path.
CONTENT_HASH_COLUMN = "__ldaca_content_hash__"

# Canonical schema for every per-bucket cache parquet (including delta files
# and stub parquets emitted by the workspace-load repair pass when a donor's
# cache files are missing on the receiver). Matches what
# ``polars_text.tokenize_with_offsets`` produces:
#   - ``token`` is ``DataType::String``  (polars-text/src/expressions.rs:242)
#   - ``start`` / ``end`` are ``DataType::Int64``  (lines 243-244)
# The content hash comes from ``pl.col(source).hash()`` which is ``UInt64``.
TOKENS_CACHE_SCHEMA: dict[str, "pl.DataType"] = {
    CONTENT_HASH_COLUMN: pl.UInt64,
    "tokens": pl.List(
        pl.Struct(
            {
                "token": pl.String,
                "start": pl.Int64,
                "end": pl.Int64,
            }
        )
    ),
}


# --------------------------------------------------------------------------- #
# Paths                                                                       #
# --------------------------------------------------------------------------- #


def cache_relpath_for_user(user_id: str) -> str:
    """Path under `LDACA_TOKENS_CACHE_DIR` for ``user_id``'s cache umbrella.

    Returns the same relative path Wordflow uses for the rest of the
    per-user data tree (single-user: ``user_root/user_cache``; multi-user:
    ``user_<id>/user_cache``). Combined with the env var (which the
    startup hook points at ``{data_root}/{user_data_folder}``) this lands
    the cache inside each user's own folder, sibling of ``embeddings/``
    and ``snapshots/`` — so backups, ACLs, and "delete this user"
    operations all see the cache as part of the user's data.

    Crucially, this value is also passed as the ``user_id`` kwarg to
    the Rust lazy expression — the expression resolves to the same
    path Python's manifest writes to. The workspace-load alignment
    hook (see :func:`align_tokens_for_current_user`) detects when a
    plan's baked relpath doesn't match the current user's expected
    one and scrub-and-recreates the expression, so a workspace
    imported by user B never writes into user A's tree.
    """
    # Avoid the circular-import problem at module level — settings is
    # imported lazily, only when this helper is called.
    from ..settings import settings

    folder_name = "user_root" if not settings.multi_user else f"user_{user_id}"
    return f"{folder_name}/user_cache"


def tokens_cache_dir(user_id: str) -> Path:
    """Return the absolute cache directory for ``user_id``.

    Combines `LDACA_TOKENS_CACHE_DIR` with :func:`cache_relpath_for_user`
    and the ``tokens/`` subdir — the absolute path is e.g.
    ``{data_root}/{user_data_folder}/user_root/user_cache/tokens/``
    in single-user mode.

    Both this module and the Rust ``tokenize_with_cache_lookup``
    expression resolve to this same path: Python composes it directly
    here; Rust takes the (env, kwarg) pair and joins them. The startup
    hook in `main.py` lifespan sets the env if not externally configured.
    """
    override = os.environ.get(CACHE_ROOT_ENV)
    if not override:
        raise RuntimeError(
            f"{CACHE_ROOT_ENV} is not set. The backend's startup hook "
            f"sets a default; if you're seeing this in a script or REPL, "
            f"set the env var explicitly before importing tokens_cache."
        )
    root = (
        Path(override).expanduser()
        / cache_relpath_for_user(user_id)
        / TOKENS_CACHE_SUBDIR
    )
    root.mkdir(parents=True, exist_ok=True)
    return root


def _manifest_path(user_id: str) -> Path:
    return tokens_cache_dir(user_id) / MANIFEST_FILENAME


def _params_hash(params: dict) -> str:
    """Stable short hash of the tokenisation parameters."""
    # ``sort_keys`` makes ``{"a": 1, "b": 2}`` and ``{"b": 2, "a": 1}`` hash
    # the same; without it a Python dict-ordering change between callers
    # would silently split the cache.
    blob = json.dumps(params, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(blob).hexdigest()[:12]


def cache_filename(model: str, params: dict) -> str:
    """Filename for the cache parquet covering one (model, params) tuple.

    Filename only — no user_id component, because the file already lives
    under a per-user directory. Sanitises the model id so a hypothetical
    ``../etc/passwd`` model name can't escape the cache dir.
    """
    safe_model = "".join(c if c.isalnum() or c in "-._" else "_" for c in model)
    return f"{safe_model}__{_params_hash(params)}.parquet"


def cache_path(user_id: str, model: str, params: dict) -> Path:
    """Canonical bucket-path identifier for a (user, model, params) tuple.

    The returned ``Path`` may not exist on disk — after the delta-files
    refactor, new caches are written as ``<bucket>__delta__<uuid>.parquet``
    siblings, so the legacy ``<bucket>.parquet`` only exists for buckets
    that were populated before the refactor. The function is still useful
    as a stable identifier the caller can store in derived metadata; use
    :func:`_bucket_files` / :func:`tokens_cache_lazyframe` for actual I/O.
    """
    return tokens_cache_dir(user_id) / cache_filename(model, params)


def _bucket_key_from_filename(filename: str) -> str:
    """Strip ``.parquet`` from a legacy bucket filename to get the bucket key.

    The bucket key is the prefix shared between the legacy single-file
    cache (``<bucket>.parquet``) and delta files
    (``<bucket>__delta__<uuid>.parquet``). Derived metadata persists
    legacy ``.parquet`` filenames for back-compat; this helper normalises
    them so manifest / sweep code can key by bucket.
    """
    return filename[: -len(".parquet")] if filename.endswith(".parquet") else filename


def _bucket_key(model: str, params: dict) -> str:
    """Bucket key for a (model, params) tuple — the filename stem."""
    return _bucket_key_from_filename(cache_filename(model, params))


def _bucket_files(user_id: str, bucket: str) -> list[Path]:
    """All cache files belonging to one bucket, ordered oldest-first.

    Includes both the legacy ``<bucket>.parquet`` (if a workspace was
    created before the delta refactor) and every
    ``<bucket>__delta__*.parquet`` written since. Order: legacy first
    (oldest write of all), then deltas sorted by mtime ascending. The
    read-side ``.unique(keep="first")`` in :func:`tokens_cache_lazyframe`
    relies on this ordering so the *earliest-written* tokens for a
    given content hash win — same semantic as the pre-delta single-file
    cache, which read-merged-replaced in write order.
    """
    d = tokens_cache_dir(user_id)
    files: list[Path] = []
    legacy = d / f"{bucket}.parquet"
    if legacy.exists():
        files.append(legacy)
    deltas: list[tuple[float, Path]] = []
    for p in d.glob(f"{bucket}{DELTA_INFIX}*.parquet"):
        try:
            deltas.append((p.stat().st_mtime, p))
        except OSError:
            continue
    deltas.sort(key=lambda pair: pair[0])
    files.extend(p for _, p in deltas)
    return files


def _new_delta_path(user_id: str, bucket: str) -> Path:
    """Allocate a fresh delta filename for ``bucket``. UUID4 hex is
    collision-free in practice; no need to coordinate via the manifest."""
    return tokens_cache_dir(user_id) / f"{bucket}{DELTA_INFIX}{uuid.uuid4().hex}.parquet"


# --------------------------------------------------------------------------- #
# File locks                                                                  #
# --------------------------------------------------------------------------- #


@contextlib.contextmanager
def _file_lock(path: Path) -> Iterator[None]:
    """Coarse exclusive lock over a sidecar lockfile.

    POSIX: ``fcntl.flock(LOCK_EX)`` — blocks until the lock is free.
    Windows: falls back to a marker-file presence check with retries
    (best-effort; the desktop build is single-user single-process for
    its core path so contention is unlikely).
    """
    lock_path = path.with_suffix(path.suffix + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    if fcntl is not None:
        with open(lock_path, "w") as lock_file:
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
                yield
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
    else:  # pragma: no cover — Windows path
        attempts = 0
        while lock_path.exists() and attempts < 50:
            time.sleep(0.05)
            attempts += 1
        try:
            lock_path.touch(exist_ok=True)
            yield
        finally:
            with contextlib.suppress(FileNotFoundError):
                lock_path.unlink()


def _atomic_write_parquet(df: pl.DataFrame, dest: Path) -> None:
    """Write ``df`` to ``dest`` via a tmp + rename so partial writes are
    invisible to readers using ``scan_parquet``."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(
        prefix=dest.name + ".",
        suffix=".tmp",
        dir=str(dest.parent),
    )
    os.close(fd)
    try:
        df.write_parquet(tmp_path)
        os.replace(tmp_path, dest)
    except Exception:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(tmp_path)
        raise


# --------------------------------------------------------------------------- #
# Manifest                                                                    #
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class CacheReference:
    """One node's claim on a cache file within a single user's cache.

    The owning user is implicit in the cache directory the manifest
    lives in, so the reference itself only carries the workspace +
    node pair.
    """

    workspace_id: str
    node_id: str

    def to_dict(self) -> dict:
        return {
            "workspace_id": self.workspace_id,
            "node_id": self.node_id,
        }


def _empty_manifest() -> dict:
    return {"version": 2, "entries": {}}


def _read_manifest_unlocked(user_id: str) -> dict:
    path = _manifest_path(user_id)
    if not path.exists():
        return _empty_manifest()
    try:
        with open(path) as fh:
            data = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning(
            "tokens-cache manifest at %s is unreadable (%s); starting fresh",
            path,
            exc,
        )
        return _empty_manifest()
    if not isinstance(data, dict) or "entries" not in data:
        logger.warning(
            "tokens-cache manifest %s has unexpected shape; resetting", path
        )
        return _empty_manifest()
    # Lazy upgrade: pre-delta manifests keyed entries by ``<bucket>.parquet``
    # filenames. Re-key them to bucket form so the rest of the module can
    # treat both shapes uniformly without branching everywhere.
    entries = data.get("entries", {})
    if isinstance(entries, dict) and any(k.endswith(".parquet") for k in entries):
        rekeyed: dict = {}
        for key, entry in entries.items():
            bucket = _bucket_key_from_filename(key) if isinstance(key, str) else key
            # If both the legacy and bucket-form keys somehow exist, merge
            # references; the union is the safe choice.
            existing = rekeyed.get(bucket)
            if existing and isinstance(entry, dict):
                refs = list(existing.get("references", []))
                for r in entry.get("references", []):
                    if r not in refs:
                        refs.append(r)
                existing["references"] = refs
            else:
                rekeyed[bucket] = entry
        data["entries"] = rekeyed
    return data


def _write_manifest_unlocked(user_id: str, manifest: dict) -> None:
    path = _manifest_path(user_id)
    fd, tmp_path = tempfile.mkstemp(
        prefix=path.name + ".", suffix=".tmp", dir=str(path.parent)
    )
    os.close(fd)
    try:
        with open(tmp_path, "w") as fh:
            json.dump(manifest, fh, indent=2, sort_keys=True)
        os.replace(tmp_path, path)
    except Exception:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(tmp_path)
        raise


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_entry(manifest: dict, bucket: str, *, size_bytes: int = 0) -> dict:
    entries = manifest.setdefault("entries", {})
    entry = entries.get(bucket)
    if entry is None:
        entry = {
            "size_bytes": size_bytes,
            "created_at": _now_iso(),
            "last_accessed_at": _now_iso(),
            "references": [],
        }
        entries[bucket] = entry
    return entry


def add_reference(user_id: str, filename: str, ref: CacheReference) -> None:
    """Record that ``ref`` depends on the bucket the file belongs to.

    ``filename`` may be the legacy ``<bucket>.parquet`` form (what
    ``node.derived[col]['cache_filename']`` stores) or the bucket key
    directly; both are normalised to bucket form. Idempotent.
    """
    bucket = _bucket_key_from_filename(filename)
    with _file_lock(_manifest_path(user_id)):
        manifest = _read_manifest_unlocked(user_id)
        entry = _ensure_entry(manifest, bucket)
        ref_dict = ref.to_dict()
        if ref_dict not in entry["references"]:
            entry["references"].append(ref_dict)
        entry["last_accessed_at"] = _now_iso()
        _write_manifest_unlocked(user_id, manifest)


def drop_reference(user_id: str, filename: str, ref: CacheReference) -> None:
    """Remove one ``(workspace, node)`` claim on the bucket.

    Like :func:`add_reference`, accepts either the legacy filename or the
    bucket key. No-op if the manifest doesn't list the reference.
    """
    bucket = _bucket_key_from_filename(filename)
    with _file_lock(_manifest_path(user_id)):
        manifest = _read_manifest_unlocked(user_id)
        entries = manifest.get("entries", {})
        entry = entries.get(bucket)
        if not entry:
            return
        ref_dict = ref.to_dict()
        entry["references"] = [r for r in entry["references"] if r != ref_dict]
        _write_manifest_unlocked(user_id, manifest)


def drop_workspace_references(user_id: str, workspace_id: str) -> None:
    """Drop every cache reference owned by one workspace.

    Called by the workspace-delete path so the sweep can reclaim files
    that no other workspace still references.
    """
    with _file_lock(_manifest_path(user_id)):
        manifest = _read_manifest_unlocked(user_id)
        for entry in manifest.get("entries", {}).values():
            entry["references"] = [
                r
                for r in entry["references"]
                if r.get("workspace_id") != workspace_id
            ]
        _write_manifest_unlocked(user_id, manifest)


def drop_node_references(
    user_id: str, workspace_id: str, node_id: str
) -> None:
    """Drop every cache reference owned by one node within one workspace.

    Called from the node-delete path. Walks all manifest entries rather
    than asking the caller to enumerate ``node.derived[*]['cache_filename']``
    so a node with mixed-model derived columns is cleaned up in one call
    and so the cleanup is robust to partial-write metadata corruption.
    """
    with _file_lock(_manifest_path(user_id)):
        manifest = _read_manifest_unlocked(user_id)
        for entry in manifest.get("entries", {}).values():
            entry["references"] = [
                r
                for r in entry["references"]
                if not (
                    r.get("workspace_id") == workspace_id
                    and r.get("node_id") == node_id
                )
            ]
        _write_manifest_unlocked(user_id, manifest)


# --------------------------------------------------------------------------- #
# Compaction                                                                  #
# --------------------------------------------------------------------------- #


def compact_bucket_if_needed(
    user_id: str,
    model: str,
    params: dict,
    *,
    threshold: int = DEFAULT_COMPACTION_THRESHOLD,
) -> bool:
    """Public entry point — merge a bucket's many small delta files back
    into one when the file count exceeds ``threshold``. Returns True iff
    a merge actually happened.

    Called from `tokenise_column` after each registration (per-tokenise
    trigger) and from the backend startup sweep (per-session trigger).
    See `docs/developer-guide/lazy-tokenisation-refactor.md`.
    """
    return _compact_bucket_if_needed(
        user_id, _bucket_key(model, params), threshold=threshold
    )


def compact_all_buckets(
    user_id: Optional[str] = None,
    *,
    threshold: int = DEFAULT_COMPACTION_THRESHOLD,
) -> dict[str, int]:
    """Scan every bucket for ``user_id`` (or every user when ``None``)
    and compact those over ``threshold``. Returns ``{user_id: compacted_count}``.

    Cheap when buckets are below threshold — just a directory listing
    per bucket. Designed for the backend startup hook.
    """
    if user_id is None:
        results: dict[str, int] = {}
        for uid in _all_user_ids_with_cache():
            results[uid] = _compact_all_buckets_for_user(uid, threshold=threshold)
        return results
    return {user_id: _compact_all_buckets_for_user(user_id, threshold=threshold)}


def _compact_all_buckets_for_user(user_id: str, *, threshold: int) -> int:
    """Walk every bucket under ``user_id``'s cache dir and compact those
    over ``threshold``. Returns the count of buckets compacted."""
    cache_dir = tokens_cache_dir(user_id)
    seen_buckets: set[str] = set()
    for path in cache_dir.glob("*.parquet"):
        bucket = _bucket_key_from_filename(path.name).split(DELTA_INFIX, 1)[0]
        seen_buckets.add(bucket)
    compacted = 0
    for bucket in seen_buckets:
        if _compact_bucket_if_needed(user_id, bucket, threshold=threshold):
            compacted += 1
    return compacted


def _compact_bucket_if_needed(
    user_id: str,
    bucket: str,
    *,
    threshold: int = DEFAULT_COMPACTION_THRESHOLD,
) -> bool:
    """Merge a bucket's many small delta files back into one when the
    file count exceeds ``threshold``. Returns True iff a merge happened.

    Strategy:
    * Read the union of all current bucket files and dedupe by hash.
    * Write the merged content to one fresh delta file.
    * Delete the prior files.

    A reader racing with compaction (the Rust lazy expression
    `tokenize_with_cache_lookup` building its in-memory cache map) sees
    either the old files, the new merged delta, or both — `load_cache_map`
    dedupes by hash on read with first-writer-wins, and parquet writes
    are atomic-on-close (tmp + rename) so partial writes never expose
    torn data. We do not take the bucket flock here because the new
    delta file is brand-new (no concurrent writer can target it) and
    the old files are only deleted *after* it lands.
    """
    files = _bucket_files(user_id, bucket)
    if len(files) <= threshold:
        return False
    try:
        merged = (
            pl.scan_parquet([str(p) for p in files])
            .unique(subset=[CONTENT_HASH_COLUMN])
            .collect()
        )
    except Exception as exc:
        logger.warning("compaction: failed to read bucket %s: %s", bucket, exc)
        return False
    new_path = _new_delta_path(user_id, bucket)
    try:
        _atomic_write_parquet(merged, new_path)
    except Exception as exc:
        logger.warning(
            "compaction: failed to write merged delta for %s: %s", bucket, exc
        )
        return False
    for old in files:
        try:
            old.unlink()
        except OSError as exc:
            # Windows blocks unlink while a reader holds the file open. Skip
            # quietly; the next compaction (or sweep) will retry.
            logger.debug(
                "compaction: failed to remove %s (will retry next time): %s",
                old,
                exc,
            )
    logger.info(
        "compaction: bucket %s merged %d files -> 1 (user=%s)",
        bucket,
        len(files),
        user_id,
    )
    return True


# --------------------------------------------------------------------------- #
# Sweep                                                                       #
# --------------------------------------------------------------------------- #


def _all_user_ids_with_cache() -> list[str]:
    """Enumerate every user that has a tokens-cache directory on disk.

    Walks the per-user layout
    ``{LDACA_TOKENS_CACHE_DIR}/{user_root_folder}/user_cache/tokens/``,
    extracting the user_id from the ``user_root_folder`` prefix
    (``user_root`` → ``"root"``, ``user_<id>`` → ``<id>``). The startup
    hook in `main.py` lifespan guarantees the env var is set before any
    backend code path calls this; if it isn't (an ad-hoc script), returns
    an empty list rather than raising.
    """
    override = os.environ.get(CACHE_ROOT_ENV)
    if not override:
        return []
    base = Path(override).expanduser()
    if not base.exists():
        return []
    out: list[str] = []
    for entry in sorted(base.iterdir()):
        if not entry.is_dir():
            continue
        # Layout: {base}/{user_root_folder}/user_cache/tokens/
        if not (entry / "user_cache" / TOKENS_CACHE_SUBDIR).exists():
            continue
        name = entry.name
        if name == "user_root":
            out.append("root")
        elif name.startswith("user_"):
            out.append(name[len("user_") :])
        else:
            # Tolerate bare-name dirs (tests, custom layouts) — fall back
            # to the directory name as-is.
            out.append(name)
    return out


def sweep_unreferenced(
    user_id: Optional[str] = None,
    *,
    grace_period_days: int = DEFAULT_GRACE_PERIOD_DAYS,
    now: Optional[datetime] = None,
) -> dict[str, list[str]]:
    """Delete cache files with no references that are also past the grace
    window. Returns ``{user_id: [removed_filenames, ...]}``.

    When ``user_id`` is ``None`` (the startup-hook path), walks every
    user that has a tokens-cache directory on disk and sweeps each in
    turn. Pass a concrete ``user_id`` to scope the sweep to one user
    (e.g. for a per-user maintenance trigger).

    Also reaps:
    * orphan cache files that the manifest doesn't know about (e.g.
      from a previous crashed write) — same grace-period gate based on
      filesystem ``st_mtime``;
    * manifest entries pointing at vanished files (e.g. user deleted
      the parquet by hand) — entry is removed.
    """
    if user_id is None:
        results: dict[str, list[str]] = {}
        for uid in _all_user_ids_with_cache():
            results[uid] = _sweep_unreferenced_for_user(
                uid, grace_period_days=grace_period_days, now=now
            )
        return results
    return {user_id: _sweep_unreferenced_for_user(
        user_id, grace_period_days=grace_period_days, now=now
    )}


def _sweep_unreferenced_for_user(
    user_id: str,
    *,
    grace_period_days: int,
    now: Optional[datetime],
) -> list[str]:
    """Bucket-driven sweep.

    Each manifest entry is now a *bucket* (after the delta-files
    refactor); the on-disk parquet files for that bucket are
    ``<bucket>.parquet`` (legacy, if present) plus
    ``<bucket>__delta__*.parquet``. When a bucket has no references and
    is past the grace window, *every* file in the bucket is removed.

    Also reaps:
    * manifest entries whose bucket has no surviving files on disk;
    * orphan parquet files whose bucket has no manifest entry — same
      ``st_mtime`` grace gate as before.
    """
    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(days=grace_period_days)
    removed: list[str] = []

    cache_dir = tokens_cache_dir(user_id)
    with _file_lock(_manifest_path(user_id)):
        manifest = _read_manifest_unlocked(user_id)
        entries = manifest.setdefault("entries", {})

        # Pass 1 — manifest-driven, per bucket.
        for bucket in list(entries.keys()):
            entry = entries[bucket]
            bucket_key = (
                _bucket_key_from_filename(bucket) if isinstance(bucket, str) else bucket
            )
            bucket_files = _bucket_files(user_id, bucket_key)
            if not bucket_files:
                # No files left on disk — drop the manifest entry.
                del entries[bucket]
                continue
            if entry.get("references"):
                continue
            last_access = _parse_iso(entry.get("last_accessed_at"))
            if last_access is None or last_access <= cutoff:
                ok = True
                for p in bucket_files:
                    try:
                        p.unlink()
                        removed.append(p.name)
                    except OSError as exc:
                        logger.warning("sweep: failed to remove %s: %s", p, exc)
                        ok = False
                if ok:
                    del entries[bucket]

        # Pass 2 — orphan parquet sweep (file on disk, no manifest entry).
        # Bucket-driven so we don't reap one delta of a still-referenced
        # bucket: if any sibling file's bucket key is in ``entries``, skip.
        known_buckets = {
            _bucket_key_from_filename(b) if isinstance(b, str) else b
            for b in entries.keys()
        }
        for path in cache_dir.glob("*.parquet"):
            file_bucket = _bucket_key_from_filename(path.name).split(DELTA_INFIX, 1)[0]
            if file_bucket in known_buckets:
                continue
            try:
                mtime = datetime.fromtimestamp(
                    path.stat().st_mtime, tz=timezone.utc
                )
            except OSError:
                continue
            if mtime > cutoff:
                continue
            try:
                path.unlink()
                removed.append(path.name)
            except OSError as exc:
                logger.warning(
                    "sweep: failed to remove orphan %s: %s", path, exc
                )

        _write_manifest_unlocked(user_id, manifest)

    return removed


def _parse_iso(text: Optional[str]) -> Optional[datetime]:
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


# --------------------------------------------------------------------------- #
# Testing affordances                                                         #
# --------------------------------------------------------------------------- #


def _reset_for_tests(user_id: str) -> None:
    """Wipe the cache directory for one test user.

    Test-only — never call from production code paths. The
    :envvar:`LDACA_TOKENS_CACHE_DIR` should point at a tmpdir during
    tests so this is bounded.
    """
    root = tokens_cache_dir(user_id)
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)


__all__ = [
    "CACHE_ROOT_ENV",
    "CONTENT_HASH_COLUMN",
    "DEFAULT_COMPACTION_THRESHOLD",
    "TOKENS_CACHE_SCHEMA",
    "TOKENS_CACHE_SUBDIR",
    "CacheReference",
    "add_reference",
    "cache_filename",
    "cache_path",
    "compact_all_buckets",
    "compact_bucket_if_needed",
    "drop_node_references",
    "drop_reference",
    "drop_workspace_references",
    "sweep_unreferenced",
    "tokens_cache_dir",
]
