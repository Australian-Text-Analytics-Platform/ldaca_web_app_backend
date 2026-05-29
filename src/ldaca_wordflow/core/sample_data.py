"""On-demand sample-data import and remote download.

Used by:
- sample-data import API endpoint and backend API routes because they need a backend
  boundary that validates inputs before delegating to workspace or worker state.

Flow: copy (or re-copy) bundled sample data to the user's local folder, then
    optionally download remote collections via HTTP, checking SHA-256 digests
    to avoid unnecessary transfers.
"""

import hashlib
import logging
import os
import shutil
import uuid
from contextlib import nullcontext
from importlib import resources
from pathlib import Path
from typing import Any

from .user_folders import get_user_data_folder
from ..settings import settings

logger = logging.getLogger(__name__)


def import_sample_data_for_user(user_id: str) -> dict[str, Any]:
    """Import (or re-import) sample data for a user on demand.

    Removes any existing sample_data folder then copies from the canonical
    sample data source. Returns summary statistics.

    Used by:
    - sample-data import API endpoint because they need a backend boundary that validates
      inputs before delegating to workspace or worker state.
    Why:
    - Keeps sample data provisioning explicit and idempotent.
    """
    source_override = settings.get_sample_data_folder()
    target_dir = get_user_data_folder(user_id)
    target_sample_data = target_dir / "sample_data"

    if source_override:
        source_ctx = nullcontext(source_override)
    else:
        source_ctx = resources.as_file(
            resources.files("ldaca_wordflow.resources").joinpath("sample_data")
        )

    with source_ctx as source_sample_data:
        if not source_sample_data.exists():
            raise FileNotFoundError(
                f"Source sample data folder not found: {source_sample_data}"
            )

        removed_existing = False
        if target_sample_data.exists():
            shutil.rmtree(target_sample_data)
            removed_existing = True

        temp_target = target_dir / f".sample_data_tmp_{uuid.uuid4().hex}"
        shutil.copytree(source_sample_data, temp_target)
        os.replace(temp_target, target_sample_data)

    file_count = 0
    bytes_copied = 0
    for fp in target_sample_data.rglob("*"):
        if fp.is_file():
            file_count += 1
            try:
                bytes_copied += fp.stat().st_size
            except OSError:
                logger.debug("Could not stat file %s during sample copy", fp)

    return {
        "removed_existing": removed_existing,
        "file_count": file_count,
        "bytes_copied": bytes_copied,
        "sample_dir": str(target_sample_data),
    }


async def download_remote_sample_data(
    user_id: str,
    collection_ids: list[str] | None = None,
) -> None:
    """Download any missing or updated remote sample datasets into the user's
    sample_data folder.

    Fetches catalogue.json from the configured remote base URL, then downloads
    each listed file whose on-disk SHA-256 does not match. Files that already
    match are skipped. Any download error is logged and skipped so a partial
    failure does not prevent the rest from downloading.

    If collection_ids is given, only those collections are downloaded.
    If None, all non-bundled collections are downloaded (original behaviour).

    Called as a FastAPI BackgroundTask after the bundled data has been copied.
    No-op when sample_data_remote_url is empty/unset.

    Used by:
    - backend API routes because they need a backend boundary that validates inputs before
      delegating to workspace or worker state.
    """
    import httpx

    remote_base = (settings.sample_data_remote_url or "").rstrip("/")
    if not remote_base:
        return

    target_dir = get_user_data_folder(user_id) / "sample_data"
    target_dir.mkdir(parents=True, exist_ok=True)

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(f"{remote_base}/catalogue.json")
            resp.raise_for_status()
            catalogue = resp.json()
    except Exception:
        logger.warning(
            "Could not fetch remote sample data catalogue from %s", remote_base
        )
        return

    collections = catalogue.get("collections", [])
    for col in collections:
        col_id: str = col.get("id", "")
        bundled: bool = col.get("bundled", False)

        if collection_ids is not None:
            if col_id not in collection_ids:
                continue
        else:
            if bundled:
                continue

        for entry in col.get("files", []):
            rel_path: str = entry.get("path", "")
            expected_sha256: str = entry.get("sha256", "")
            if not rel_path or not expected_sha256:
                continue

            dest = target_dir / Path(rel_path)

            if dest.exists():
                digest = hashlib.sha256(dest.read_bytes()).hexdigest()
                if digest == expected_sha256:
                    continue

            dest.parent.mkdir(parents=True, exist_ok=True)
            url = f"{remote_base}/{rel_path}"
            logger.info("Downloading remote sample dataset: %s", rel_path)
            try:
                async with httpx.AsyncClient(timeout=300) as client:
                    async with client.stream("GET", url) as stream:
                        stream.raise_for_status()
                        tmp = dest.with_suffix(dest.suffix + f".tmp_{uuid.uuid4().hex}")
                        try:
                            with tmp.open("wb") as fh:
                                async for chunk in stream.aiter_bytes(
                                    chunk_size=1 << 20
                                ):
                                    fh.write(chunk)
                            os.replace(tmp, dest)
                            logger.info("Downloaded %s", rel_path)
                        except Exception:
                            tmp.unlink(missing_ok=True)
                            raise
            except Exception:
                logger.warning(
                    "Failed to download remote sample dataset: %s",
                    rel_path,
                    exc_info=True,
                )
