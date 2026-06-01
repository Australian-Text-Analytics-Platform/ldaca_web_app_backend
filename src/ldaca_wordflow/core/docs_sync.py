"""Background mirror of the version-pinned docs site into a local cache.

Why this exists
---------------
The desktop (Tauri) build can't fetch the remote docs host directly — its
webview CSP only trusts ``localhost``/``self`` — so the app reads docs through
the bundled backend instead. This module keeps a writable mirror of the docs
(markdown + image assets) fresh, and ``resolve_doc_file`` lets the ``/docs``
route serve it with the *bundled* docs as a permanent offline fallback.

Design (mirrors the sample-data sync in ``core/utils.py``)
----------------------------------------------------------
* Seed/floor: the docs shipped inside ``resources/frontend/build`` are always
  present and read-only — that's the offline fallback.
* Freshness: on startup a daemon thread fetches ``registry.json`` and, when its
  ``meta.version`` differs from the locally cached marker, mirrors the whole doc
  set into ``~/.cache/ldaca_wordflow/docs/content`` (atomic dir swap).
* Cache semantics: the cache dir is disposable/regenerable — wiping it just
  drops back to the bundled docs until the next sync.
* Offline-tolerant: any network failure is logged at INFO/WARNING and leaves the
  existing copy untouched. The sync never blocks startup or the first doc open.

Disable by setting ``docs_remote_base_url`` to an empty string.
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
import shutil
import threading
import uuid
from pathlib import Path

from ..settings import settings

logger = logging.getLogger(__name__)

# Index entry points are standalone docs (not keyed registry entries), so they
# won't appear in registry.json's sections — mirror them explicitly.
_INDEX_FILES = (
    "tutorials/index.md",
    "information/index.md",
    "references/index.md",
)

# Markdown ``![alt](path "title")`` and raw ``<img src="path">``.
_MD_IMG_RE = re.compile(r"!\[[^\]]*\]\(\s*<?([^)\s>]+)>?(?:\s+[^)]*)?\)")
_HTML_IMG_RE = re.compile(r"<img\b[^>]*?\bsrc\s*=\s*[\"']([^\"']+)[\"']", re.IGNORECASE)

_CONTENT_DIRNAME = "content"
_VERSION_FILENAME = "VERSION"


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------
def _bundled_docs_dir() -> Path | None:
    """Return the read-only bundled docs root (the frontend build dir)."""
    try:
        from importlib import resources

        build = Path(str(resources.files("ldaca_wordflow.resources.frontend") / "build"))
        return build if build.is_dir() else None
    except Exception:  # pragma: no cover — packaging edge cases
        return None


def _safe_join(root: Path, rel_path: str) -> Path | None:
    """Join ``rel_path`` under ``root``, rejecting traversal outside it."""
    try:
        root_resolved = root.resolve()
        candidate = (root_resolved / rel_path).resolve()
        candidate.relative_to(root_resolved)
    except (ValueError, OSError):
        return None
    return candidate


def _normalize_rel(rel_path: str) -> str:
    """Collapse ``.``/``..``/duplicate slashes in a relative doc path."""
    stack: list[str] = []
    for segment in rel_path.replace("\\", "/").split("/"):
        if not segment or segment == ".":
            continue
        if segment == "..":
            if stack:
                stack.pop()
            continue
        stack.append(segment)
    return "/".join(stack)


def resolve_doc_file(rel_path: str) -> Path | None:
    """Resolve a doc path to a file on disk: synced cache first, bundle fallback.

    Returns ``None`` when neither the cache nor the bundle has the file (or the
    path tries to escape its root). The bundled copy is the permanent offline
    floor, so a missing/empty cache degrades gracefully.
    """
    rel_path = _normalize_rel(rel_path)
    if not rel_path:
        return None

    content_dir = settings.get_docs_cache_dir() / _CONTENT_DIRNAME
    if content_dir.is_dir():
        hit = _safe_join(content_dir, rel_path)
        if hit and hit.is_file():
            return hit

    bundled = _bundled_docs_dir()
    if bundled is not None:
        hit = _safe_join(bundled, rel_path)
        if hit and hit.is_file():
            return hit
    return None


# ---------------------------------------------------------------------------
# Sync
# ---------------------------------------------------------------------------
def _registry_version(registry: dict, raw: bytes) -> str:
    """Derive a change-detection marker from the registry.

    Prefers the published ``meta.version``; falls back to a content hash so the
    sync still detects changes even if a deployment omits the version field.
    """
    meta = registry.get("meta") if isinstance(registry, dict) else None
    version = (meta or {}).get("version") if isinstance(meta, dict) else None
    if isinstance(version, str) and version.strip():
        return version.strip()
    return "sha256:" + hashlib.sha256(raw).hexdigest()[:16]


def _read_cached_version() -> str | None:
    try:
        return (settings.get_docs_cache_dir() / _VERSION_FILENAME).read_text(
            encoding="utf-8"
        ).strip() or None
    except OSError:
        return None


def _collect_markdown_files(registry: dict) -> set[str]:
    """All markdown doc paths: every registry entry's ``file`` + index files."""
    files: set[str] = set(_INDEX_FILES)
    for section in ("tutorial", "info", "reference"):
        entries = registry.get(section) if isinstance(registry, dict) else None
        if not isinstance(entries, dict):
            continue
        for entry in entries.values():
            file = entry.get("file") if isinstance(entry, dict) else None
            if isinstance(file, str) and file.endswith(".md"):
                files.add(_normalize_rel(file))
    return {f for f in files if f}


def _collect_image_refs(markdown: str) -> set[str]:
    """Extract local (non-external) image paths referenced by a markdown doc.

    Image paths in our docs are docs-root-relative (e.g.
    ``tutorials/assets/foo.png``), matching how the web app resolves them
    against the origin root. External/data/blob refs are skipped.
    """
    refs: set[str] = set()
    for match in _MD_IMG_RE.finditer(markdown):
        refs.add(match.group(1))
    for match in _HTML_IMG_RE.finditer(markdown):
        refs.add(match.group(1))

    out: set[str] = set()
    for ref in refs:
        ref = ref.strip()
        if not ref or ref.startswith(
            ("http://", "https://", "//", "data:", "blob:", "mailto:", "#")
        ):
            continue
        norm = _normalize_rel(ref.lstrip("/"))
        if norm:
            out.add(norm)
    return out


def _download(client, url: str) -> bytes | None:
    """GET ``url`` returning its bytes, or ``None`` on any failure (logged)."""
    try:
        resp = client.get(url)
        resp.raise_for_status()
        return resp.content
    except Exception:
        logger.warning("[docs-sync] could not download %s", url)
        return None


def _write_file(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def _promote(cache_dir: Path, staging: Path, version: str) -> None:
    """Atomically swap ``staging`` into place and stamp the version marker.

    The version file is written *last* — it's the commit point. If a crash
    happens mid-swap, the next start sees a version mismatch and re-syncs.
    """
    content = cache_dir / _CONTENT_DIRNAME
    old = cache_dir / ".old"
    shutil.rmtree(old, ignore_errors=True)
    if content.exists():
        os.replace(content, old)  # atomic rename out of the way
    os.replace(staging, content)  # atomic rename into place
    shutil.rmtree(old, ignore_errors=True)

    version_tmp = cache_dir / ".VERSION.tmp"
    version_tmp.write_text(version, encoding="utf-8")
    os.replace(version_tmp, cache_dir / _VERSION_FILENAME)


def sync_docs() -> None:
    """Mirror the remote docs into the cache if a newer version is available.

    No-op when ``docs_remote_base_url`` is empty or the remote is unreachable.
    """
    base = (settings.docs_remote_base_url or "").strip().rstrip("/")
    if not base:
        return

    try:
        import httpx
    except Exception:  # pragma: no cover — httpx is a hard dep, defensive only
        logger.warning("[docs-sync] httpx not importable; skipping docs sync")
        return

    # 1. Fetch the registry and decide whether anything changed.
    try:
        with httpx.Client(timeout=30, follow_redirects=True) as client:
            resp = client.get(f"{base}/registry.json")
            resp.raise_for_status()
            registry_bytes = resp.content
            registry = resp.json()
    except Exception:
        logger.info(
            "[docs-sync] remote docs registry unreachable at %s; "
            "serving bundled/cached docs",
            base,
        )
        return

    remote_version = _registry_version(registry, registry_bytes)
    cache_dir = settings.get_docs_cache_dir()
    if (cache_dir / _CONTENT_DIRNAME).is_dir() and _read_cached_version() == remote_version:
        logger.info("[docs-sync] docs cache already at version %s", remote_version)
        return

    # 2. Mirror markdown + referenced image assets into a staging dir.
    logger.info("[docs-sync] mirroring docs version %s from %s", remote_version, base)
    md_files = _collect_markdown_files(registry)
    cache_dir.mkdir(parents=True, exist_ok=True)
    staging = cache_dir / f".staging_{uuid.uuid4().hex}"
    try:
        _write_file(staging / "registry.json", registry_bytes)
        assets: set[str] = set()
        with httpx.Client(timeout=60, follow_redirects=True) as client:
            for rel in sorted(md_files):
                data = _download(client, f"{base}/{rel}")
                if data is None:
                    continue
                _write_file(staging / rel, data)
                assets |= _collect_image_refs(data.decode("utf-8", "replace"))
            for rel in sorted(assets):
                data = _download(client, f"{base}/{rel}")
                if data is None:
                    continue
                _write_file(staging / rel, data)
        _promote(cache_dir, staging, remote_version)
        logger.info(
            "[docs-sync] docs synced to %s (%d markdown, %d assets)",
            remote_version,
            len(md_files),
            len(assets),
        )
    except Exception:
        logger.warning(
            "[docs-sync] docs sync failed; keeping existing copy", exc_info=True
        )
        shutil.rmtree(staging, ignore_errors=True)


def _run_sync_safe() -> None:
    try:
        sync_docs()
    except Exception:  # pragma: no cover — belt-and-suspenders
        logger.warning("[docs-sync] background docs sync crashed", exc_info=True)


def start_docs_sync() -> None:
    """Kick off the docs mirror in a daemon thread (no-op when disabled)."""
    if not (settings.docs_remote_base_url or "").strip():
        return
    threading.Thread(target=_run_sync_safe, name="docs-sync", daemon=True).start()
