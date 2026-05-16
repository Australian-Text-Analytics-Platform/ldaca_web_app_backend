"""Snapshot bundle storage endpoints.

REST endpoints for the demo-snapshot feature. The backend is a
storage clerk: no analysis logic. Each snapshot is stored as three
sibling files under ``user_cache/snapshots/``:

- ``<basename>.ldaca-snapshot``  — canonical zip bundle
- ``<basename>.manifest.json``   — sidecar manifest (fast listing)
- ``<basename>.md``              — sidecar human description

See ``docs/snapshot-view/plan.md`` §2 for the full design.
"""

from __future__ import annotations

import json
import logging
import re
import zipfile
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse, PlainTextResponse

from ..core.auth import get_current_user
from ..core.utils import get_user_snapshots_folder

router = APIRouter(prefix="/users/me/snapshots", tags=["snapshots"])
logger = logging.getLogger(__name__)

BUNDLE_SUFFIX = ".ldaca-snapshot"
MANIFEST_SIDECAR_SUFFIX = ".manifest.json"
DESCRIPTION_SIDECAR_SUFFIX = ".md"

# Same sanitisation rules as the frontend (plan §2.1). The frontend
# pre-validates; the server check is defence-in-depth.
INVALID_FILENAME_CHARS = re.compile(r'[/\\:*?"<>|]')
MAX_BASENAME_LENGTH = 80

# Compatibility predicate for batch-delete-incompatible. Mirrors the
# frontend predicate (plan §2.4). MAJOR.MINOR equality; per-tool
# allowlists not implemented server-side yet — the frontend's
# TOOL_COMPATIBILITY registry is the source of truth there, but the
# server makes the actual delete call so it independently verifies.
_VERSION_RE = re.compile(r"^v?(\d+)\.(\d+)(?:\.\d+)?(?:[-+].*)?$")


def _parse_major_minor(version: str) -> Optional[str]:
    """Return ``"<MAJOR>.<MINOR>"`` from a version string, or ``None``
    if malformed. Accepts ``"v0.4.4"``, ``"0.4.4"``, ``"0.4"``,
    ``"0.4.0-rc1"`` etc."""
    if not isinstance(version, str):
        return None
    m = _VERSION_RE.match(version.strip())
    if not m:
        return None
    return f"{m.group(1)}.{m.group(2)}"


def _is_compatible(snapshot_version: str, current_version: str) -> bool:
    snap = _parse_major_minor(snapshot_version)
    cur = _parse_major_minor(current_version)
    if snap is None or cur is None:
        return False
    return snap == cur


def _validate_filename(filename: str) -> tuple[bool, str]:
    """Validate a snapshot filename. Returns ``(ok, reason)``.

    The filename must:
    - end with ``.ldaca-snapshot``
    - have a non-empty basename (the part before the suffix)
    - contain only characters that survive ``INVALID_FILENAME_CHARS``
    - not contain ``..`` or path separators
    - be ≤ ``MAX_BASENAME_LENGTH + len(BUNDLE_SUFFIX)`` chars
    """
    if not isinstance(filename, str):
        return False, "filename must be a string"
    if not filename.endswith(BUNDLE_SUFFIX):
        return False, f"filename must end with {BUNDLE_SUFFIX}"
    basename = filename[: -len(BUNDLE_SUFFIX)]
    if not basename:
        return False, "filename basename cannot be empty"
    if ".." in basename:
        return False, "filename cannot contain '..'"
    if INVALID_FILENAME_CHARS.search(basename):
        return False, "filename contains invalid characters"
    if len(basename) > MAX_BASENAME_LENGTH:
        return False, f"filename basename exceeds {MAX_BASENAME_LENGTH} chars"
    return True, ""


def _confined_path(snapshots_dir: Path, filename: str) -> Path:
    """Resolve a filename against the user's snapshots folder and
    assert it stays inside that folder. Raises HTTP 400 on traversal
    attempts or malformed filenames.
    """
    ok, reason = _validate_filename(filename)
    if not ok:
        raise HTTPException(status_code=400, detail=f"invalid filename: {reason}")
    candidate = (snapshots_dir / filename).resolve()
    try:
        candidate.relative_to(snapshots_dir.resolve())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="path traversal blocked") from exc
    return candidate


def _read_manifest_from_zip(bundle_path: Path) -> Optional[dict]:
    """Return the parsed ``manifest.json`` from inside the zip, or
    ``None`` if the zip is malformed or the manifest is missing /
    unparseable. Used by the lazy-extract path on listing.
    """
    try:
        with zipfile.ZipFile(bundle_path) as zf:
            with zf.open("manifest.json") as fh:
                return json.load(fh)
    except (zipfile.BadZipFile, KeyError, json.JSONDecodeError):
        logger.warning("Failed to read manifest from %s", bundle_path)
        return None


def _generate_description_md(manifest: dict) -> str:
    """Auto-generate the ``.md`` sidecar from manifest data. Plain
    template — users may overwrite later via an edit flow that hasn't
    been built yet. Kept narrow on purpose: this is what a fresh
    snapshot looks like out of the box.
    """
    title = manifest.get("title") or "(untitled snapshot)"
    tool = manifest.get("tool", "(unknown tool)")
    captured = manifest.get("captured_at", "")
    version = manifest.get("tool_version", "")
    src = manifest.get("source", {}) or {}
    node_labels = src.get("node_labels") or []
    workspace_name = src.get("workspace_name", "(unknown workspace)")
    total_rows = src.get("total_source_rows", "?")
    lines = [
        f"# {title}",
        "",
        f"- **Tool**: `{tool}`",
        f"- **Captured**: {captured}",
        f"- **Wordflow version**: {version}",
        f"- **Workspace**: {workspace_name}",
        f"- **Source data blocks**: {', '.join(node_labels) if node_labels else '(none)'}",
        f"- **Total source rows**: {total_rows}",
        "",
    ]
    preview = manifest.get("preview") or {}
    if preview:
        lines.append("## Preview")
        lines.append("")
        # Render every key in the preview block except the discriminator.
        for key, value in preview.items():
            if key == "tool":
                continue
            lines.append(f"- **{key}**: {value}")
    return "\n".join(lines).strip() + "\n"


def _sidecar_paths(bundle_path: Path) -> tuple[Path, Path]:
    """Return ``(manifest_sidecar, description_sidecar)`` paths derived
    from a bundle path."""
    basename = bundle_path.name[: -len(BUNDLE_SUFFIX)]
    return (
        bundle_path.with_name(f"{basename}{MANIFEST_SIDECAR_SUFFIX}"),
        bundle_path.with_name(f"{basename}{DESCRIPTION_SIDECAR_SUFFIX}"),
    )


def _ensure_sidecar(bundle_path: Path) -> Optional[dict]:
    """Ensure a sidecar manifest exists for ``bundle_path``, extracting
    from the zip if it's missing. Returns the manifest dict or ``None``
    if the bundle is corrupt / unreadable.
    """
    manifest_sidecar, _ = _sidecar_paths(bundle_path)
    if manifest_sidecar.exists():
        try:
            with manifest_sidecar.open("r", encoding="utf-8") as fh:
                return json.load(fh)
        except (json.JSONDecodeError, OSError):
            logger.warning("Sidecar %s unreadable; re-extracting", manifest_sidecar)
    manifest = _read_manifest_from_zip(bundle_path)
    if manifest is None:
        return None
    try:
        manifest_sidecar.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    except OSError:
        logger.exception("Failed to write sidecar %s", manifest_sidecar)
    return manifest


def _list_user_snapshots(user_id: str, tool_filter: Optional[str]) -> list[dict]:
    """Walk the user's snapshots folder and return ``[{filename, manifest}, ...]``.

    Lazy-extracts a sidecar from the zip when missing. Filters by
    ``tool_filter`` (matching ``manifest.tool``) when supplied.
    """
    folder = get_user_snapshots_folder(user_id)
    items: list[dict] = []
    for bundle_path in sorted(folder.glob(f"*{BUNDLE_SUFFIX}")):
        manifest = _ensure_sidecar(bundle_path)
        if manifest is None:
            # Corrupt or unparseable bundle — skip silently rather than
            # 500-ing the whole list. Should be rare; future cleanup
            # tooling can sweep.
            continue
        if tool_filter is not None and manifest.get("tool") != tool_filter:
            continue
        try:
            size_bytes = bundle_path.stat().st_size
        except OSError:
            size_bytes = 0
        items.append(
            {
                "filename": bundle_path.name,
                "manifest": manifest,
                "size_bytes": size_bytes,
            }
        )
    return items


@router.get("")
async def list_snapshots(
    tool: Optional[str] = None,
    user: dict = Depends(get_current_user),
) -> dict:
    """List snapshots for the current user, optionally filtered by tool."""
    return {"items": _list_user_snapshots(user["id"], tool)}


@router.post("")
async def upload_snapshot(
    file: UploadFile = File(...),
    filename: str = Form(...),
    user: dict = Depends(get_current_user),
) -> dict:
    """Store a bundle uploaded from the frontend.

    Validates the filename, extracts the internal ``manifest.json`` to
    write the sidecar, auto-generates the ``.md`` description. Rejects
    409 on collision (the frontend pre-checks; this is defence-in-depth).
    """
    snapshots_dir = get_user_snapshots_folder(user["id"])
    bundle_path = _confined_path(snapshots_dir, filename)

    if bundle_path.exists():
        raise HTTPException(status_code=409, detail="snapshot filename already exists")

    bundle_bytes = await file.read()
    if not bundle_bytes:
        raise HTTPException(status_code=400, detail="empty upload")

    # Write the bundle first; if anything below fails we still have the
    # zip as the canonical artifact (the lazy sidecar path will recover
    # on next list).
    try:
        bundle_path.write_bytes(bundle_bytes)
    except OSError as exc:
        raise HTTPException(status_code=500, detail=f"failed to write bundle: {exc}") from exc

    manifest = _read_manifest_from_zip(bundle_path)
    if manifest is None:
        # Refuse to keep a bundle whose manifest can't be parsed —
        # listing it would silently skip it forever. Easier for the
        # user to see the upload fail.
        try:
            bundle_path.unlink()
        except OSError:
            pass
        raise HTTPException(status_code=400, detail="bundle has no readable manifest.json")

    manifest_sidecar, description_sidecar = _sidecar_paths(bundle_path)
    try:
        manifest_sidecar.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        description_sidecar.write_text(_generate_description_md(manifest), encoding="utf-8")
    except OSError:
        logger.exception("Failed to write sidecars for %s", bundle_path)

    return {"filename": bundle_path.name, "manifest": manifest}


@router.get("/{filename}")
async def download_snapshot(
    filename: str,
    user: dict = Depends(get_current_user),
) -> FileResponse:
    """Stream a bundle's bytes back as ``application/zip``."""
    snapshots_dir = get_user_snapshots_folder(user["id"])
    bundle_path = _confined_path(snapshots_dir, filename)
    if not bundle_path.exists():
        raise HTTPException(status_code=404, detail="snapshot not found")
    return FileResponse(
        path=bundle_path,
        media_type="application/zip",
        filename=filename,
    )


@router.get("/{filename}/description")
async def get_snapshot_description(
    filename: str,
    user: dict = Depends(get_current_user),
) -> PlainTextResponse:
    """Return the ``.md`` sidecar content. Regenerates from manifest
    if the sidecar is missing — never returns 404 for a present
    bundle.
    """
    snapshots_dir = get_user_snapshots_folder(user["id"])
    bundle_path = _confined_path(snapshots_dir, filename)
    if not bundle_path.exists():
        raise HTTPException(status_code=404, detail="snapshot not found")
    _, description_sidecar = _sidecar_paths(bundle_path)
    if description_sidecar.exists():
        try:
            content = description_sidecar.read_text(encoding="utf-8")
        except OSError:
            content = ""
        if content:
            return PlainTextResponse(content=content, media_type="text/markdown")
    manifest = _ensure_sidecar(bundle_path)
    if manifest is None:
        raise HTTPException(status_code=500, detail="bundle has no readable manifest")
    content = _generate_description_md(manifest)
    try:
        description_sidecar.write_text(content, encoding="utf-8")
    except OSError:
        logger.exception("Failed to write description sidecar")
    return PlainTextResponse(content=content, media_type="text/markdown")


def _delete_bundle_with_sidecars(bundle_path: Path) -> None:
    """Remove the bundle plus both sidecar files. Best-effort on
    sidecars — if a sidecar is already absent the delete is a no-op
    rather than an error.
    """
    manifest_sidecar, description_sidecar = _sidecar_paths(bundle_path)
    bundle_path.unlink(missing_ok=True)
    manifest_sidecar.unlink(missing_ok=True)
    description_sidecar.unlink(missing_ok=True)


@router.delete("/{filename}")
async def delete_snapshot(
    filename: str,
    user: dict = Depends(get_current_user),
) -> dict:
    """Remove one snapshot (bundle + both sidecars)."""
    snapshots_dir = get_user_snapshots_folder(user["id"])
    bundle_path = _confined_path(snapshots_dir, filename)
    if not bundle_path.exists():
        raise HTTPException(status_code=404, detail="snapshot not found")
    _delete_bundle_with_sidecars(bundle_path)
    return {"deleted": [bundle_path.name]}


@router.delete("")
async def batch_delete_snapshots(
    tool: str,
    incompatible_with: Optional[str] = None,
    user: dict = Depends(get_current_user),
) -> dict:
    """Batch delete. Without ``incompatible_with``, removes every
    snapshot for ``tool``. With it, removes only those whose
    ``manifest.tool_version`` is incompatible (MAJOR.MINOR mismatch).
    """
    items = _list_user_snapshots(user["id"], tool)
    snapshots_dir = get_user_snapshots_folder(user["id"])
    deleted: list[str] = []
    for item in items:
        manifest = item["manifest"]
        if incompatible_with is not None:
            snap_version = manifest.get("tool_version", "")
            if _is_compatible(snap_version, incompatible_with):
                continue
        bundle_path = _confined_path(snapshots_dir, item["filename"])
        _delete_bundle_with_sidecars(bundle_path)
        deleted.append(item["filename"])
    return {"deleted": deleted}
