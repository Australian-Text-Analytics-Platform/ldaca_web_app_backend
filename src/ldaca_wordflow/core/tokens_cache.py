"""User-scoped, content-addressed cache of tokenisation results.

Tokenising a column is expensive — especially for the new CJK backends
(Jieba for Chinese, Lindera for Japanese / Korean) where a single 10 k
character document produces thousands of morphemes. The lazy plan in
``derived_columns.tokenise_column`` only appends a
``polars_text.tokenize_with_offsets`` expression, so every downstream
``.collect()`` (every paginated concordance query, every page-size
estimator probe, every token-frequency run) re-executes the tokeniser
from scratch. This module amortises that cost to once per (model,
params, source content) tuple by writing the per-row tokens to a
parquet that persists across workspace open/close.

Design (mirrors PLAN-cjk-tokeniser-perf.md §"Tokens cache"):

* **Location.** ``~/.ldaca/tokens-cache/`` — outside every per-user /
  per-workspace directory so workspace deletion or workspace GC never
  touches the cache. Path is user-scoped (one cache per OS user).
* **Filename.** ``{model}__{params_hash}.parquet`` — one parquet per
  (model, params) globally. New tokens are appended; cache hit rate
  improves over time as the user explores related corpora.
* **Schema.** ``__content_hash: u64`` (``pl.col(source).hash()``) and
  ``tokens: List<Struct<token, start, end>>``. Source rows that share
  content (duplicate documents within or across corpora) share one
  cache row.
* **Lookup.** The lazy plan joins the source frame to the cached
  frame on ``__content_hash``. Filter / sort / select downstream is
  free: surviving rows automatically retrieve their tokens; dropped
  columns trigger projection pushdown and never read the cache.
* **References.** A sidecar ``manifest.json`` tracks which
  ``(user_id, workspace_id, node_id)`` triples reference each cache
  file. ``tokenise_column`` calls :func:`add_reference`; node and
  workspace deletion paths call :func:`drop_reference` /
  :func:`drop_workspace_references`. :func:`sweep_unreferenced` then
  deletes any cache file whose ``references`` is empty AND that has
  not been accessed in ``grace_period_days`` days (default 7).

Concurrency: per-cache-file ``fcntl.flock`` over a ``.lock`` sidecar
serialises writers. The manifest has its own lock. Atomic
write-to-temp + ``os.replace`` makes partial writes invisible to
concurrent readers. Windows (Tauri build) does not have ``fcntl``;
the lock acquisition is best-effort there — multiple concurrent
tokenise calls on the same model are vanishingly rare in the desktop
single-user case.

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


CACHE_ROOT_ENV = "LDACA_TOKENS_CACHE_DIR"
DEFAULT_CACHE_ROOT = Path.home() / ".ldaca" / "tokens-cache"

MANIFEST_FILENAME = "manifest.json"
DEFAULT_GRACE_PERIOD_DAYS = 7

# The hashed-content key column that the join uses. Kept here so callers
# don't accidentally collide with a user column of the same name on a
# different code path.
CONTENT_HASH_COLUMN = "__ldaca_content_hash__"


# --------------------------------------------------------------------------- #
# Paths                                                                       #
# --------------------------------------------------------------------------- #


def tokens_cache_dir() -> Path:
    """Return the user-scoped cache directory, creating it on first use.

    Honours ``LDACA_TOKENS_CACHE_DIR`` for tests and for non-default
    Tauri install layouts.
    """
    root = Path(os.environ.get(CACHE_ROOT_ENV) or DEFAULT_CACHE_ROOT).expanduser()
    root.mkdir(parents=True, exist_ok=True)
    return root


def _manifest_path() -> Path:
    return tokens_cache_dir() / MANIFEST_FILENAME


def _params_hash(params: dict) -> str:
    """Stable short hash of the tokenisation parameters."""
    # ``sort_keys`` makes ``{"a": 1, "b": 2}`` and ``{"b": 2, "a": 1}`` hash
    # the same; without it a Python dict-ordering change between callers
    # would silently split the cache.
    blob = json.dumps(params, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(blob).hexdigest()[:12]


def cache_filename(model: str, params: dict) -> str:
    """Filename for the cache parquet covering one (model, params) tuple."""
    # Sanitise the model id so a hypothetical "../etc/passwd" model name
    # can't escape the cache dir. Models we ship use simple alnum + dash
    # but external callers might be hostile.
    safe_model = "".join(c if c.isalnum() or c in "-._" else "_" for c in model)
    return f"{safe_model}__{_params_hash(params)}.parquet"


def cache_path(model: str, params: dict) -> Path:
    return tokens_cache_dir() / cache_filename(model, params)


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
    """One node's claim on a cache file."""

    user_id: str
    workspace_id: str
    node_id: str

    def to_dict(self) -> dict:
        return {
            "user_id": self.user_id,
            "workspace_id": self.workspace_id,
            "node_id": self.node_id,
        }


def _empty_manifest() -> dict:
    return {"version": 1, "entries": {}}


def _read_manifest_unlocked() -> dict:
    path = _manifest_path()
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
        logger.warning("tokens-cache manifest %s has unexpected shape; resetting", path)
        return _empty_manifest()
    return data


def _write_manifest_unlocked(manifest: dict) -> None:
    path = _manifest_path()
    fd, tmp_path = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=str(path.parent))
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


def _ensure_entry(manifest: dict, filename: str, *, size_bytes: int = 0) -> dict:
    entries = manifest.setdefault("entries", {})
    entry = entries.get(filename)
    if entry is None:
        entry = {
            "size_bytes": size_bytes,
            "created_at": _now_iso(),
            "last_accessed_at": _now_iso(),
            "references": [],
        }
        entries[filename] = entry
    return entry


def add_reference(filename: str, ref: CacheReference) -> None:
    """Record that ``ref`` depends on the cache file named ``filename``.

    Idempotent — calling twice with the same triple inserts one entry.
    """
    with _file_lock(_manifest_path()):
        manifest = _read_manifest_unlocked()
        entry = _ensure_entry(manifest, filename)
        ref_dict = ref.to_dict()
        if ref_dict not in entry["references"]:
            entry["references"].append(ref_dict)
        entry["last_accessed_at"] = _now_iso()
        _write_manifest_unlocked(manifest)


def drop_reference(filename: str, ref: CacheReference) -> None:
    """Remove one ``(user, workspace, node)`` claim on the cache file.

    No-op if the manifest doesn't list the reference (idempotent).
    """
    with _file_lock(_manifest_path()):
        manifest = _read_manifest_unlocked()
        entries = manifest.get("entries", {})
        entry = entries.get(filename)
        if not entry:
            return
        ref_dict = ref.to_dict()
        entry["references"] = [r for r in entry["references"] if r != ref_dict]
        _write_manifest_unlocked(manifest)


def drop_workspace_references(user_id: str, workspace_id: str) -> None:
    """Drop every cache reference owned by one workspace.

    Called by the workspace-delete path so the sweep can reclaim files
    that no other workspace still references.
    """
    with _file_lock(_manifest_path()):
        manifest = _read_manifest_unlocked()
        for entry in manifest.get("entries", {}).values():
            entry["references"] = [
                r
                for r in entry["references"]
                if not (r.get("user_id") == user_id and r.get("workspace_id") == workspace_id)
            ]
        _write_manifest_unlocked(manifest)


def drop_node_references(user_id: str, workspace_id: str, node_id: str) -> None:
    """Drop every cache reference owned by one node within one workspace.

    Called from the node-delete path. Walks all manifest entries rather
    than asking the caller to enumerate ``node.derived[*]['cache_filename']``
    so a node with mixed-model derived columns is cleaned up in one call
    and so the cleanup is robust to partial-write metadata corruption.
    """
    with _file_lock(_manifest_path()):
        manifest = _read_manifest_unlocked()
        for entry in manifest.get("entries", {}).values():
            entry["references"] = [
                r
                for r in entry["references"]
                if not (
                    r.get("user_id") == user_id
                    and r.get("workspace_id") == workspace_id
                    and r.get("node_id") == node_id
                )
            ]
        _write_manifest_unlocked(manifest)


def touch_access(filename: str) -> None:
    """Update ``last_accessed_at`` without changing references — call
    on every cache hit so the LRU-sweep doesn't evict hot files."""
    with _file_lock(_manifest_path()):
        manifest = _read_manifest_unlocked()
        entries = manifest.get("entries", {})
        if filename not in entries:
            return
        entries[filename]["last_accessed_at"] = _now_iso()
        _write_manifest_unlocked(manifest)


# --------------------------------------------------------------------------- #
# Cache I/O                                                                   #
# --------------------------------------------------------------------------- #


def cache_exists(model: str, params: dict) -> bool:
    return cache_path(model, params).exists()


def read_cached_hashes(model: str, params: dict) -> set[int]:
    """Return the set of content-hashes currently in the cache parquet.

    Used by :func:`ensure_tokens_cache` to decide which source rows need
    fresh tokenisation. Empty set when the cache file doesn't exist yet.
    """
    path = cache_path(model, params)
    if not path.exists():
        return set()
    df = pl.scan_parquet(path).select(CONTENT_HASH_COLUMN).collect()
    return set(int(h) for h in df.get_column(CONTENT_HASH_COLUMN).to_list())


def write_or_append_cache(
    model: str,
    params: dict,
    new_rows: pl.DataFrame,
) -> Path:
    """Merge ``new_rows`` into the (model, params) cache parquet.

    ``new_rows`` must carry exactly ``CONTENT_HASH_COLUMN`` and a
    ``tokens`` list-of-struct column. Rows whose content-hash is
    already present are dropped (the existing tokens win — they were
    computed by the same model/params, so the result is identical).

    Returns the cache file path. Use :func:`tokens_cache_lazyframe` to
    read it back.
    """
    expected_cols = {CONTENT_HASH_COLUMN, "tokens"}
    missing = expected_cols - set(new_rows.columns)
    if missing:
        raise ValueError(
            f"write_or_append_cache: new_rows missing columns {sorted(missing)}; "
            f"got {new_rows.columns}"
        )

    path = cache_path(model, params)
    with _file_lock(path):
        if path.exists():
            existing = pl.read_parquet(path)
            existing_hashes = set(
                int(h) for h in existing.get_column(CONTENT_HASH_COLUMN).to_list()
            )
            fresh = new_rows.filter(
                ~pl.col(CONTENT_HASH_COLUMN).is_in(list(existing_hashes))
            )
            if fresh.height == 0:
                # Touch access time anyway — somebody just used the cache.
                pass
            else:
                merged = pl.concat([existing, fresh], how="vertical_relaxed")
                _atomic_write_parquet(merged, path)
        else:
            _atomic_write_parquet(new_rows, path)

    with _file_lock(_manifest_path()):
        manifest = _read_manifest_unlocked()
        entry = _ensure_entry(manifest, path.name, size_bytes=path.stat().st_size)
        entry["size_bytes"] = path.stat().st_size
        entry["last_accessed_at"] = _now_iso()
        _write_manifest_unlocked(manifest)

    return path


def tokens_cache_lazyframe(model: str, params: dict) -> Optional[pl.LazyFrame]:
    """Return a LazyFrame over the cache file, or ``None`` if absent.

    The frame has columns ``CONTENT_HASH_COLUMN, tokens``. Join your
    source frame on ``CONTENT_HASH_COLUMN`` to attach the tokens column
    without re-tokenising.
    """
    path = cache_path(model, params)
    if not path.exists():
        return None
    return pl.scan_parquet(path)


# --------------------------------------------------------------------------- #
# Sweep                                                                       #
# --------------------------------------------------------------------------- #


def sweep_unreferenced(
    *,
    grace_period_days: int = DEFAULT_GRACE_PERIOD_DAYS,
    now: Optional[datetime] = None,
) -> list[str]:
    """Delete cache files with no references that are also past the grace
    window. Returns the names of the files removed.

    Also reaps:
    * orphan cache files that the manifest doesn't know about (e.g.
      from a previous crashed write) — same grace-period gate based on
      filesystem ``st_mtime``;
    * manifest entries pointing at vanished files (e.g. user deleted
      the parquet by hand) — entry is removed.
    """
    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(days=grace_period_days)
    removed: list[str] = []

    cache_dir = tokens_cache_dir()
    with _file_lock(_manifest_path()):
        manifest = _read_manifest_unlocked()
        entries = manifest.setdefault("entries", {})

        # Pass 1 — manifest-driven sweep.
        for filename in list(entries.keys()):
            entry = entries[filename]
            path = cache_dir / filename
            if not path.exists():
                # File gone — drop the manifest entry.
                del entries[filename]
                continue
            if entry.get("references"):
                continue
            last_access = _parse_iso(entry.get("last_accessed_at"))
            if last_access is None or last_access <= cutoff:
                try:
                    path.unlink()
                    removed.append(filename)
                    del entries[filename]
                except OSError as exc:
                    logger.warning("sweep: failed to remove %s: %s", path, exc)

        # Pass 2 — orphan parquet sweep (file present, no manifest entry).
        for path in cache_dir.glob("*.parquet"):
            if path.name in entries:
                continue
            try:
                mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
            except OSError:
                continue
            if mtime > cutoff:
                continue
            try:
                path.unlink()
                removed.append(path.name)
            except OSError as exc:
                logger.warning("sweep: failed to remove orphan %s: %s", path, exc)

        _write_manifest_unlocked(manifest)

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


def _reset_for_tests() -> None:
    """Wipe the cache directory. Test-only — never call from production
    code paths. The :envvar:`LDACA_TOKENS_CACHE_DIR` should point at a
    tmpdir during tests so this is bounded."""
    root = tokens_cache_dir()
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)


__all__ = [
    "CONTENT_HASH_COLUMN",
    "CacheReference",
    "add_reference",
    "cache_exists",
    "cache_filename",
    "cache_path",
    "drop_node_references",
    "drop_reference",
    "drop_workspace_references",
    "read_cached_hashes",
    "sweep_unreferenced",
    "tokens_cache_dir",
    "tokens_cache_lazyframe",
    "touch_access",
    "write_or_append_cache",
]
