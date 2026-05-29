"""Demo-snapshot catalogue and import endpoints.

Used by:
- FastAPI router aggregation in ``__init__.py``.

Flow:
- Same pattern as sample_data.py but for demo-snapshot bundles. Catalogue
  fetches ``demo_snapshots/catalogue.json`` from the remote; import downloads
  selected bundles, verifies SHA, and writes them to the user's snapshot folder.
"""

import hashlib
import logging
from pathlib import Path
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException

from ...core.auth import get_current_user
from ...core.utils import get_user_snapshots_folder
from ...core.exceptions import AppError, BadGatewayError
from ...models import (
    DemoSnapshotEntry,
    DemoSnapshotImportResult,
    DemoSnapshotsCatalogueResponse,
    ImportDemoSnapshotsRequest,
    ImportDemoSnapshotsResponse,
)

router = APIRouter()
logger = logging.getLogger(__name__)

DemoSnapshotStatus = Literal["downloaded", "not_downloaded", "conflict"]

_DEMO_SNAPSHOT_REMOTE_DIR = "demo_snapshots"


def _compute_demo_snapshot_status(
    entry: dict, snapshots_dir: Path
) -> DemoSnapshotStatus:
    """Return ``downloaded`` / ``conflict`` / ``not_downloaded`` for one entry.

    Steps:
    - Check whether the target file exists at the expected path.
    - SHA256-verify the local copy against the catalogue entry.

    Called by:
    - ``get_demo_snapshots_catalogue`` and ``import_demo_snapshots``.
    """
    filename = entry.get("filename") or ""
    expected_sha256 = entry.get("sha256") or ""
    if not filename:
        return "not_downloaded"
    dest = snapshots_dir / filename
    if not dest.exists() or not dest.is_file():
        return "not_downloaded"
    try:
        digest = hashlib.sha256(dest.read_bytes()).hexdigest()
    except OSError:
        return "not_downloaded"
    if digest == expected_sha256:
        return "downloaded"
    return "conflict"


@router.get(
    "/demo-snapshots/catalogue",
    response_model=DemoSnapshotsCatalogueResponse,
)
async def get_demo_snapshots_catalogue(
    current_user: dict = Depends(get_current_user),
):
    """Return the demo-snapshot catalogue, augmented with per-bundle status.

    Fetches ``demo_snapshots/catalogue.json`` from the configured sample-data
    remote. Each entry is annotated with the user-specific status
    (``downloaded`` / ``conflict`` / ``not_downloaded``) so the frontend can
    render conflict warnings without a second round-trip.

    Returns an empty list (not 404) when the catalogue is absent so the
    frontend tab can render an empty-state instead of erroring out — the
    sample-data repo can ship without the demo-snapshots block until
    bundles are authored.

    Flow:
    - Resolve authentication and request parameters from FastAPI dependencies.
    - Delegate validation, manager calls, artifacts, or state changes to the
      owning helper.
    - Shape the response payload or raise the HTTP error the client should see.

    Used by:
    - Frontend and API clients through the FastAPI GET /demo-snapshots/catalogue route.
    """
    import httpx

    from ...settings import settings

    remote_base = (settings.sample_data_remote_url or "").rstrip("/")
    if not remote_base:
        raise AppError("Sample data remote URL not configured.")
    url = f"{remote_base}/{_DEMO_SNAPSHOT_REMOTE_DIR}/catalogue.json"
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(url)
    except Exception:
        raise BadGatewayError("Could not fetch demo-snapshot catalogue.")
    if resp.status_code == 404:
        return DemoSnapshotsCatalogueResponse(schema_version=1, snapshots=[])
    try:
        resp.raise_for_status()
        catalogue = resp.json()
    except Exception:
        raise BadGatewayError("Could not fetch demo-snapshot catalogue.")
    user_id = current_user["id"]
    snapshots_dir = get_user_snapshots_folder(user_id)

    snapshots: list[DemoSnapshotEntry] = []
    for entry in catalogue.get("snapshots", []):
        status = _compute_demo_snapshot_status(entry, snapshots_dir)
        snapshots.append(
            DemoSnapshotEntry(
                id=entry.get("id", ""),
                filename=entry.get("filename", ""),
                path=entry.get("path", ""),
                tool=entry.get("tool", ""),
                name=entry.get("name", ""),
                description=entry.get("description", ""),
                size=int(entry.get("size", 0) or 0),
                sha256=entry.get("sha256", ""),
                tool_version=entry.get("tool_version"),
                recommended_dataset=entry.get("recommended_dataset"),
                status=status,
            )
        )

    return DemoSnapshotsCatalogueResponse(
        schema_version=int(catalogue.get("schema_version", 1) or 1),
        snapshots=snapshots,
    )


@router.post(
    "/import-demo-snapshots",
    response_model=ImportDemoSnapshotsResponse,
)
async def import_demo_snapshots(
    request: ImportDemoSnapshotsRequest = ImportDemoSnapshotsRequest(),
    current_user: dict = Depends(get_current_user),
):
    """Download selected demo-snapshot bundles into the user's snapshot folder.

    Behaviour per bundle:
      - ``not_downloaded`` → download, verify SHA, write to snapshot folder
      - ``downloaded`` → skip (matching local copy already present)
      - ``conflict`` → skip unless the bundle id is also in ``replace_ids``,
        in which case the local copy is deleted and the bundle re-downloaded

    Failures (network, SHA mismatch, write error) are recorded per-entry as
    ``failed`` so a partial run still reports what landed and what didn't.
    The user's own snapshot saves are never touched unless explicitly
    opted in via ``replace_ids``.

    Flow:
    - Resolve authentication and request parameters from FastAPI dependencies.
    - Delegate validation, manager calls, artifacts, or state changes to the
      owning helper.
    - Shape the response payload or raise the HTTP error the client should see.

    Used by:
    - Frontend and API clients through the FastAPI POST /import-demo-snapshots route.
    """
    from pathlib import PurePosixPath

    import httpx

    from ...settings import settings

    user_id = current_user["id"]
    snapshots_dir = get_user_snapshots_folder(user_id)
    replace_ids = set(request.replace_ids or [])

    remote_base = (settings.sample_data_remote_url or "").rstrip("/")
    if not remote_base:
        raise AppError("Sample data remote URL not configured.")
    catalogue_url = f"{remote_base}/{_DEMO_SNAPSHOT_REMOTE_DIR}/catalogue.json"
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            cat_resp = await client.get(catalogue_url)
            cat_resp.raise_for_status()
            catalogue = cat_resp.json()
    except Exception:
        raise BadGatewayError("Could not fetch demo-snapshot catalogue.")
    by_id: dict[str, dict] = {
        e.get("id", ""): e for e in catalogue.get("snapshots", []) if e.get("id")
    }

    results: list[DemoSnapshotImportResult] = []
    for snap_id in request.snapshot_ids:
        entry = by_id.get(snap_id)
        if not entry:
            results.append(
                DemoSnapshotImportResult(
                    id=snap_id,
                    filename="",
                    status="failed",
                    message="Snapshot id not present in catalogue.",
                )
            )
            continue

        filename = entry.get("filename", "")
        rel_path = entry.get("path", "")
        expected_sha = entry.get("sha256", "")
        if not filename or not rel_path or not expected_sha:
            results.append(
                DemoSnapshotImportResult(
                    id=snap_id,
                    filename=filename,
                    status="failed",
                    message="Catalogue entry missing filename, path, or sha256.",
                )
            )
            continue

        try:
            safe_filename = PurePosixPath(filename)
        except Exception:
            results.append(
                DemoSnapshotImportResult(
                    id=snap_id,
                    filename=filename,
                    status="failed",
                    message="Invalid filename.",
                )
            )
            continue
        if (
            safe_filename.is_absolute()
            or any(part == ".." for part in safe_filename.parts)
            or "/" in filename
            or "\\" in filename
        ):
            results.append(
                DemoSnapshotImportResult(
                    id=snap_id,
                    filename=filename,
                    status="failed",
                    message="Invalid filename.",
                )
            )
            continue

        dest = snapshots_dir / filename
        status = _compute_demo_snapshot_status(entry, snapshots_dir)

        if status == "downloaded":
            results.append(
                DemoSnapshotImportResult(
                    id=snap_id,
                    filename=filename,
                    status="skipped_existing",
                    message="Matching local copy already present.",
                )
            )
            continue

        if status == "conflict" and snap_id not in replace_ids:
            results.append(
                DemoSnapshotImportResult(
                    id=snap_id,
                    filename=filename,
                    status="skipped_conflict",
                    message=(
                        "A local snapshot with this filename already exists "
                        "but differs from the demo. Tick Replace to overwrite."
                    ),
                )
            )
            continue

        url = f"{remote_base}/{rel_path}"
        outcome_status = "replaced" if status == "conflict" else "imported"

        try:
            async with httpx.AsyncClient(timeout=300) as client:
                async with client.stream("GET", url) as stream:
                    stream.raise_for_status()
                    tmp = dest.with_suffix(
                        dest.suffix
                        + f".tmp_{hashlib.sha256(snap_id.encode()).hexdigest()[:12]}"
                    )
                    hasher = hashlib.sha256()
                    try:
                        with tmp.open("wb") as fh:
                            async for chunk in stream.aiter_bytes(chunk_size=1 << 20):
                                hasher.update(chunk)
                                fh.write(chunk)
                        digest = hasher.hexdigest()
                        if digest != expected_sha:
                            tmp.unlink(missing_ok=True)
                            results.append(
                                DemoSnapshotImportResult(
                                    id=snap_id,
                                    filename=filename,
                                    status="failed",
                                    message=(
                                        "Downloaded bundle SHA mismatch — refusing to install."
                                    ),
                                )
                            )
                            continue
                        import os

                        os.replace(tmp, dest)
                    except Exception:
                        tmp.unlink(missing_ok=True)
                        raise
        except Exception as exc:
            logger.warning(
                "Failed to download demo snapshot %s from %s",
                snap_id,
                url,
                exc_info=True,
            )
            results.append(
                DemoSnapshotImportResult(
                    id=snap_id,
                    filename=filename,
                    status="failed",
                    message=f"Download failed: {exc}",
                )
            )
            continue

        results.append(
            DemoSnapshotImportResult(
                id=snap_id,
                filename=filename,
                status=outcome_status,
                message=None,
            )
        )

    return ImportDemoSnapshotsResponse(
        results=results,
        snapshot_dir=str(snapshots_dir),
    )
