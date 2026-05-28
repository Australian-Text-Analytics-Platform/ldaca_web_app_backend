"""LDaCA import worker task implementation.

Used by:
- Backend API routes, worker tasks, workspace services, and backend tests because they
  need a backend boundary that validates inputs before delegating to workspace or worker
  state.

Flow: validate remote or RO-Crate metadata, choose safe output paths, write workspace
    artifacts, and return import records for the caller to attach.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
import tempfile
from contextlib import chdir
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qs, urlparse

from ldaca_wordflow.core.ldaca_tabular_config import load_tabular_config
from ldaca_wordflow.core.oni_client import (
    OniClient,
    extract_ldaca_identifier,
    jsonld_value,
)
from ldaca_wordflow.settings import settings

logger = logging.getLogger(__name__)


def _sanitize_name(name: str) -> str:
    """Sanitize a corpus name for use as a folder/file name.

    Called by:
    - Local helpers, route handlers, or service methods in this module because they need a
      backend boundary that validates inputs before delegating to workspace or worker state.

    Flow: validate remote or RO-Crate metadata, choose safe output paths, write workspace
        artifacts, and return import records for the caller to attach.
    """
    import re

    sanitized = re.sub(r"[^\w.~-]", "_", name)
    sanitized = re.sub(r"_+", "_", sanitized)
    return sanitized.strip("_") or "ldaca_import"


def _metadata_name(metadata: dict[str, Any], fallback: str) -> str:
    """Support LDaCA import worker tasks with a metadata name helper.

    Called by:
    - Local helpers, route handlers, or service methods in this module because they need a
      backend boundary that validates inputs before delegating to workspace or worker state.

    Flow: validate remote or RO-Crate metadata, choose safe output paths, write workspace
        artifacts, and return import records for the caller to attach.
    """

    entities = metadata.get("@graph", [])
    root = next(
        (
            entity
            for entity in entities
            if isinstance(entity, dict) and entity.get("@id") in {"./", fallback}
        ),
        None,
    )
    name = None
    if isinstance(root, dict):
        name = jsonld_value(root.get("name"))
    if name is None:
        name = jsonld_value(metadata.get("name"))
    if isinstance(name, list):
        name = next((item for item in name if item), None)
    return str(name or fallback)


def _as_list(value: Any) -> list[Any]:
    """Support LDaCA import worker tasks with an as list helper.

    Called by:
    - Local helpers, route handlers, or service methods in this module because they need a
      backend boundary that validates inputs before delegating to workspace or worker state.

    Flow: validate remote or RO-Crate metadata, choose safe output paths, write workspace
        artifacts, and return import records for the caller to attach.
    """

    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _reference_id(value: Any) -> str | None:
    """Support LDaCA import worker tasks with a reference id helper.

    Called by:
    - Local helpers, route handlers, or service methods in this module because they need a
      backend boundary that validates inputs before delegating to workspace or worker state.

    Flow: validate remote or RO-Crate metadata, choose safe output paths, write workspace
        artifacts, and return import records for the caller to attach.
    """

    normalized = jsonld_value(value)
    if isinstance(normalized, list):
        normalized = next((item for item in normalized if item), None)
    return str(normalized) if normalized else None


def _first_string(value: Any) -> str | None:
    """Support LDaCA import worker tasks with a first string helper.

    Called by:
    - Local helpers, route handlers, or service methods in this module because they need a
      backend boundary that validates inputs before delegating to workspace or worker state.

    Flow: validate remote or RO-Crate metadata, choose safe output paths, write workspace
        artifacts, and return import records for the caller to attach.
    """

    normalized = jsonld_value(value)
    if isinstance(normalized, list):
        normalized = next((item for item in normalized if item), None)
    return str(normalized) if normalized is not None else None


def _content_size(value: Any) -> int | None:
    """Support LDaCA import worker tasks with a content size helper.

    Called by:
    - Local helpers, route handlers, or service methods in this module because they need a
      backend boundary that validates inputs before delegating to workspace or worker state.

    Flow: validate remote or RO-Crate metadata, choose safe output paths, write workspace
        artifacts, and return import records for the caller to attach.
    """

    normalized = _first_string(value)
    if normalized is None:
        return None
    try:
        return int(normalized)
    except ValueError:
        return None


def _file_path_from_id(file_id: str) -> str | None:
    """Support LDaCA import worker tasks with a file path from id helper.

    Called by:
    - Local helpers, route handlers, or service methods in this module because they need a
      backend boundary that validates inputs before delegating to workspace or worker state.

    Flow: validate remote or RO-Crate metadata, choose safe output paths, write workspace
        artifacts, and return import records for the caller to attach.
    """

    parsed = urlparse(file_id)
    query_path = parse_qs(parsed.query).get("path")
    if query_path:
        return query_path[0]
    if not parsed.scheme:
        return file_id.removeprefix("./")
    return None


def _is_file_entity(entity: dict[str, Any]) -> bool:
    """Check whether file entity applies for LDaCA import worker tasks.

    Called by:
    - Local helpers, route handlers, or service methods in this module because they need a
      backend boundary that validates inputs before delegating to workspace or worker state.

    Flow: validate remote or RO-Crate metadata, choose safe output paths, write workspace
        artifacts, and return import records for the caller to attach.
    """

    types = {str(item) for item in _as_list(jsonld_value(entity.get("@type")))}
    return "File" in types


def _is_text_plain_entity(entity: dict[str, Any]) -> bool:
    """Check whether text plain entity applies for LDaCA import worker tasks.

    Called by:
    - Local helpers, route handlers, or service methods in this module because they need a
      backend boundary that validates inputs before delegating to workspace or worker state.

    Flow: validate remote or RO-Crate metadata, choose safe output paths, write workspace
        artifacts, and return import records for the caller to attach.
    """

    encodings = {
        str(item).lower()
        for item in _as_list(jsonld_value(entity.get("encodingFormat")))
    }
    return "text/plain" in encodings


def _select_text_documents(metadata: dict[str, Any]) -> list[dict[str, Any]]:
    """Support LDaCA import worker tasks with a select text documents helper.

    Called by:
    - Local helpers, route handlers, or service methods in this module because they need a
      backend boundary that validates inputs before delegating to workspace or worker state.

    Flow: validate remote or RO-Crate metadata, choose safe output paths, write workspace
        artifacts, and return import records for the caller to attach.
    """

    entities = {
        entity.get("@id"): entity
        for entity in metadata.get("@graph", [])
        if isinstance(entity, dict) and entity.get("@id")
    }
    selected: dict[str, dict[str, Any]] = {}

    for entity in entities.values():
        file_id = entity.get("@id")
        if not isinstance(file_id, str) or not _is_file_entity(entity):
            continue
        if not _is_text_plain_entity(entity):
            continue
        path = _file_path_from_id(file_id)
        if not path:
            continue

        annotation_of = _reference_id(
            entity.get("ldac:annotationOf") or entity.get("annotationOf")
        )
        work = entities.get(annotation_of) if annotation_of else None
        key = annotation_of or path.removesuffix("-plain.txt").removesuffix(".txt")
        candidate = {
            "file_id": file_id,
            "path": path,
            "name": _first_string(entity.get("name")) or path,
            "encoding_format": "text/plain",
            "content_size": _content_size(entity.get("contentSize")),
            "annotation_of": annotation_of,
            "work_name": _first_string(work.get("name"))
            if isinstance(work, dict)
            else None,
            "date_created": _first_string(work.get("dateCreated"))
            if isinstance(work, dict)
            else None,
        }
        existing = selected.get(key)
        if existing is None or (
            "-plain." in path and "-plain." not in existing["path"]
        ):
            selected[key] = candidate

    return sorted(selected.values(), key=lambda document: document["path"])


def _load_rocrate_tabulator_class() -> type[Any]:
    """Load rocrate tabulator class data for LDaCA import worker tasks.

    Called by:
    - Local helpers, route handlers, or service methods in this module because they need a
      backend boundary that validates inputs before delegating to workspace or worker state.

    Flow: validate remote or RO-Crate metadata, choose safe output paths, write workspace
        artifacts, and return import records for the caller to attach.
    """

    from rocrate_tabular.tabulator import ROCrateTabulator

    return ROCrateTabulator


def _select_output_table(config: dict[str, Any]) -> str:
    """Support LDaCA import worker tasks with a select output table helper.

    Called by:
    - Local helpers, route handlers, or service methods in this module because they need a
      backend boundary that validates inputs before delegating to workspace or worker state.

    Flow: validate remote or RO-Crate metadata, choose safe output paths, write workspace
        artifacts, and return import records for the caller to attach.
    """

    tables = config.get("tables", {})
    for table_name in ("RepositoryObject", "CreativeWork", "File"):
        if table_name in tables:
            return table_name
    return next(iter(tables), "property")


def _quote_sql_identifier(identifier: str) -> str:
    """Support LDaCA import worker tasks with a quote sql identifier helper.

    Called by:
    - Local helpers, route handlers, or service methods in this module because they need a
      backend boundary that validates inputs before delegating to workspace or worker state.

    Flow: validate remote or RO-Crate metadata, choose safe output paths, write workspace
        artifacts, and return import records for the caller to attach.
    """

    return '"' + identifier.replace('"', '""') + '"'


def _write_table_to_parquet(db_file: Path, table_name: str, parquet_path: Path) -> None:
    """Write table to parquet output for LDaCA import worker tasks.

    Called by:
    - Local helpers, route handlers, or service methods in this module because they need a
      backend boundary that validates inputs before delegating to workspace or worker state.

    Flow: validate remote or RO-Crate metadata, choose safe output paths, write workspace
        artifacts, and return import records for the caller to attach.
    """

    import polars as pl

    query = f"SELECT * FROM {_quote_sql_identifier(table_name)}"
    with sqlite3.connect(db_file) as connection:
        df = pl.read_database(query, connection)
    df.write_parquet(parquet_path)


def _write_documents_to_parquet(
    documents: list[dict[str, Any]],
    downloaded_texts: dict[str, str],
    parquet_path: Path,
) -> None:
    """Write documents to parquet output for LDaCA import worker tasks.

    Called by:
    - Local helpers, route handlers, or service methods in this module because they need a
      backend boundary that validates inputs before delegating to workspace or worker state.

    Flow: validate remote or RO-Crate metadata, choose safe output paths, write workspace
        artifacts, and return import records for the caller to attach.
    """

    import polars as pl

    rows = [
        {
            **document,
            "text": downloaded_texts.get(document["path"], ""),
        }
        for document in documents
    ]
    pl.DataFrame(rows).write_parquet(parquet_path)


def run_ldaca_import_task(
    configure_worker_environment,
    user_id: str,
    workspace_id: str,
    url: str,
    filename: str | None = None,
    api_token: str | None = None,
    progress_callback: Callable[[float, str], None] | None = None,
) -> dict[str, Any]:
    """Execute LDaCA dataset import in a worker process.

    Creates a per-corpus folder under ``LDaCA/`` containing:
    - ``<corpus_name>.parquet`` — the tabulated text data
    - ``README.md`` — corpus metadata from ``get_corpus_info()``

    Used by:
    - backend tests, core workspace and worker services because tests need the same
      observable contract that production routes and workers rely on.

    Flow: validate remote or RO-Crate metadata, choose safe output paths, write workspace
        artifacts, and return import records for the caller to attach.
    """
    configure_worker_environment()

    from ldaca_wordflow.core.utils import get_user_data_folder

    identifier = extract_ldaca_identifier(url)
    if not identifier:
        raise ValueError("LDaCA import requires an ARCP identifier or portal URL")

    logger.info(
        "[Worker %d] Starting LDaCA import task for user %s", os.getpid(), user_id
    )

    try:
        if progress_callback:
            progress_callback(0.1, "Fetching RO-Crate metadata from LDaCA...")

        cache_dir = settings.get_data_root() / "ldaca_cache"
        cache_dir.mkdir(parents=True, exist_ok=True)

        with chdir(cache_dir), tempfile.TemporaryDirectory(dir=cache_dir) as temp_dir:
            working_dir = Path(temp_dir)
            crate_dir = working_dir / "crate"
            crate_dir.mkdir()

            client = OniClient.from_settings(settings, token=api_token)
            metadata = asyncio.run(client.get_metadata(identifier))
            (crate_dir / "ro-crate-metadata.json").write_text(
                json.dumps(metadata), encoding="utf-8"
            )
            text_documents = _select_text_documents(metadata)

            corpus_name = filename or _metadata_name(metadata, identifier)
            sanitized = _sanitize_name(corpus_name)

            if progress_callback:
                progress_callback(0.4, "Tabulating RO-Crate metadata...")

            config = load_tabular_config(identifier)
            config_path = working_dir / "tabular-config.json"
            config_path.write_text(json.dumps(config), encoding="utf-8")
            db_path = working_dir / "rocrate.sqlite"

            tabulator_class = _load_rocrate_tabulator_class()
            tabulator = tabulator_class()
            try:
                tabulator.load_config(str(config_path))
                tabulator.crate_to_db(crate_dir, db_path)
                for table_name in config.get("tables", {}):
                    tabulator.entity_table(table_name)
            finally:
                close = getattr(tabulator, "close", None)
                if callable(close):
                    close()

            output_table = _select_output_table(config)
            downloaded_texts: dict[str, str] = {}
            if text_documents:
                if progress_callback:
                    progress_callback(0.65, "Downloading text files from LDaCA...")
                downloaded_texts = asyncio.run(
                    client.download_object_texts(
                        identifier,
                        [document["path"] for document in text_documents],
                        concurrency=getattr(
                            settings, "ldaca_oni_download_concurrency", 8
                        ),
                    )
                )

            if progress_callback:
                progress_callback(0.8, "Saving to user data...")

            user_data_folder = get_user_data_folder(user_id)
            ldaca_folder = user_data_folder / "LDaCA"

            # Create a per-corpus subfolder, suffixing if the name is taken.
            corpus_folder = ldaca_folder / sanitized
            counter = 1
            base_folder = corpus_folder
            while corpus_folder.exists():
                corpus_folder = base_folder.parent / f"{base_folder.name}_{counter}"
                counter += 1
            corpus_folder.mkdir(parents=True, exist_ok=True)

            file_path = corpus_folder / f"{sanitized}.parquet"
            if text_documents:
                _write_documents_to_parquet(text_documents, downloaded_texts, file_path)
            else:
                _write_table_to_parquet(db_path, output_table, file_path)

            (corpus_folder / "README.md").write_text(
                f"# {corpus_name}\n\nSource: {identifier}\n",
                encoding="utf-8",
            )

        if progress_callback:
            progress_callback(1.0, "Import completed successfully")

        logger.info("[Worker %d] LDaCA import completed: %s", os.getpid(), file_path)

        return {
            "success": True,
            "filename": file_path.name,
            "path": str(file_path),
            "size": file_path.stat().st_size,
            "message": f"Successfully imported {corpus_name}",
        }

    except Exception as e:
        logger.error("[Worker %d] LDaCA import failed: %s", os.getpid(), e)
        if progress_callback:
            progress_callback(-1, f"Failed: {str(e)}")
        raise
