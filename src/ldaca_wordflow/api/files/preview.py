"""File preview, Excel helpers, file info, and download endpoints.

Used by:
- FastAPI router aggregation in ``__init__.py``.

Flow:
- Excel helpers detect sheet names and read worksheets.
- ``_lazy_scan`` returns Polars LazyFrames for supported formats.
- ``unified_file_preview`` dispatches per file type and supports pagination.
- ``get_file_info``, ``get_raw_file``, and ``download_file`` serve data directly.
"""

import logging
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path
from typing import Annotated, Any, cast

import polars as pl
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response, StreamingResponse

try:
    import fastexcel
except Exception:  # pragma: no cover - optional import hardening
    fastexcel: Any | None = None

from ...core.auth import get_current_user
from ...core.utils import (
    detect_file_type,
    get_user_data_folder,
    read_text_file,
    read_zip_file,
    validate_file_path,
)
from ...models import FileInfoResponse, FilePreviewRequest, FilePreviewResponse
from .crud import _resolve_user_file_path
from ...core.exceptions import AccessDeniedError, FileNotFoundError, InternalServiceError, InvalidInputError

router = APIRouter()
logger = logging.getLogger(__name__)


# ── Excel helpers ──────────────────────────────────────────────────────────


def _lazy_scan(file_path, file_type: str) -> pl.LazyFrame:
    """Return a Polars LazyFrame for the given file if possible.

    Prefers scan_* readers to avoid loading the whole file into memory.
    Falls back to eager read + .lazy() for formats without a native scanner.

    Steps:
    - Normalize caller input into the representation this module expects.
    - Delegate stateful, expensive, or validating work to the owning
      manager/helper when needed.
    - Return the compact value the caller uses for artifacts, validation, or
      response shaping.

    Used by:
    - ``unified_file_preview``.

    Why:
    - Keeps preview path memory-efficient for large tabular files.
    """
    ft = (file_type or "").lower()
    if ft == "csv":
        return pl.scan_csv(file_path)
    if ft == "tsv":
        return pl.scan_csv(file_path, separator="\t")
    if ft == "parquet":
        return pl.scan_parquet(file_path)
    if ft in ("jsonl", "ndjson"):
        scan_ndjson: Any = getattr(pl, "scan_ndjson", None)
        if callable(scan_ndjson):
            try:
                lf = scan_ndjson(file_path)
                if isinstance(lf, pl.LazyFrame):
                    return lf
            except Exception as exc:
                logger.debug("scan_ndjson failed for %s: %s", file_path, exc)
        return pl.read_ndjson(file_path).lazy()
    if ft == "json":
        return pl.read_json(file_path).lazy()
    if ft == "excel":
        try:
            df = pl.read_excel(file_path, sheet_id=0)
            return df.lazy()
        except Exception:
            return pl.DataFrame().lazy()
    return pl.DataFrame().lazy()


def _get_supported_types_by_extension(file_type: str) -> list[str]:
    """Return supported backend data representations by file extension.

    Steps:
    - Normalize caller input into the representation this module expects.
    - Delegate stateful, expensive, or validating work to the owning
      manager/helper when needed.
    - Return the compact value the caller uses for artifacts, validation, or
      response shaping.

    Used by:
    - ``unified_file_preview``.

    Why:
    - Exposes deterministic frontend capability hints per file type.
    """

    ft = (file_type or "").lower()
    mapping: dict[str, list[str]] = {
        "csv": ["LazyFrame"],
        "tsv": ["LazyFrame"],
        "jsonl": ["LazyFrame"],
        "ndjson": ["LazyFrame"],
        "json": ["LazyFrame"],
        "parquet": ["LazyFrame"],
        "excel": ["LazyFrame"],
        "text": ["LazyFrame"],
        "zip": ["LazyFrame"],
        "unknown": [],
    }
    return mapping.get(ft, [])


def _read_excel_sheet(file_path: Path, sheet_name: str) -> pl.DataFrame:
    """Read one Excel sheet as eager Polars DataFrame.

    Used by:
    - ``unified_file_preview``.

    Why:
    - Isolates sheet-level reads for preview pagination.
    """
    result = pl.read_excel(file_path, sheet_name=sheet_name)
    return _coerce_excel_result_to_dataframe(result, preferred_sheet=sheet_name)


def _coerce_excel_result_to_dataframe(
    result: Any,
    preferred_sheet: str | None = None,
) -> pl.DataFrame:
    """Normalize Polars Excel reads into a single DataFrame.

    Steps:
    - Normalize caller input into the representation this module expects.
    - Delegate stateful, expensive, or validating work to the owning
      manager/helper when needed.
    - Return the compact value the caller uses for artifacts, validation, or
      response shaping.

    Used by:
    - ``_read_excel_sheet`` and ``unified_file_preview``.

    Why:
    - Depending on Polars version/options, ``read_excel`` may return either a
      DataFrame or a dict[str, DataFrame]. Preview rendering expects a
      DataFrame, so this helper removes return-shape ambiguity.
    """
    if isinstance(result, pl.DataFrame):
        return result

    if isinstance(result, dict):
        if (
            preferred_sheet
            and preferred_sheet in result
            and isinstance(result[preferred_sheet], pl.DataFrame)
        ):
            return result[preferred_sheet]

        for value in result.values():
            if isinstance(value, pl.DataFrame):
                return value

    raise TypeError(f"Unexpected Excel read result type: {type(result)!r}")


def _list_excel_sheet_names(file_path: Path) -> list[str]:
    """Return workbook sheet names in a Polars-version-tolerant way.

    Steps:
    - Normalize caller input into the representation this module expects.
    - Delegate stateful, expensive, or validating work to the owning
      manager/helper when needed.
    - Return the compact value the caller uses for artifacts, validation, or
      response shaping.

    Used by:
    - ``unified_file_preview``.

    Why:
    - Some Polars versions return a dict for ``sheet_id=None``, while others may
      return a single DataFrame (e.g., single-sheet workbooks). This helper
      normalizes both behaviors to avoid ``.keys()`` attribute errors.
    """
    if fastexcel is not None:
        try:
            reader = fastexcel.read_excel(str(file_path))
            names = getattr(reader, "sheet_names", None)
            if names:
                return [str(name) for name in names]
        except Exception:
            pass

    workbook = pl.read_excel(file_path, sheet_id=None)
    if isinstance(workbook, dict):
        return [str(name) for name in workbook.keys()]
    if isinstance(workbook, pl.DataFrame):
        pass

    keys = getattr(workbook, "keys", None)
    if callable(keys):
        try:
            return [str(name) for name in keys()]
        except Exception:
            pass

    try:
        with zipfile.ZipFile(file_path) as zf:
            with zf.open("xl/workbook.xml") as workbook_xml:
                root = ET.parse(workbook_xml).getroot()
                names: list[str] = []
                for sheet in root.iter():
                    tag = sheet.tag.rsplit("}", 1)[-1]
                    if tag != "sheet":
                        continue
                    name = sheet.attrib.get("name")
                    if name:
                        names.append(str(name))
                if names:
                    return names
    except Exception:
        pass

    return []


# ── Preview ────────────────────────────────────────────────────────────────


@router.post("/preview", response_model=FilePreviewResponse)
async def unified_file_preview(
    req: FilePreviewRequest, current_user: dict = Depends(get_current_user)
):
    """Unified file preview endpoint.

    - Returns supported types based on extension.
    - Provides preview data (first few rows or page slice).
    - For Excel files, returns sheet_names and supports selecting sheet via
      payload.sheet_name.

    Flow:
    - Resolve authentication and request parameters from FastAPI dependencies.
    - Delegate validation, manager calls, artifacts, or state changes to the
      owning helper.
    - Shape the response payload or raise the HTTP error the client should see.

    Used by:
    - frontend file preview modal/table.

    Why:
    - Provides one format-aware preview API for heterogeneous file types.
    """
    user_id = current_user["id"]
    data_folder = get_user_data_folder(user_id)
    file_path = data_folder / req.filename

    if not validate_file_path(file_path, data_folder):
        raise AccessDeniedError("Access denied: file outside allowed directory")
    if not file_path.exists():
        raise FileNotFoundError(f"File {req.filename} not found")
    file_type = detect_file_type(file_path.name)
    supported_types = _get_supported_types_by_extension(file_type)

    page = max(0, int(req.page))
    page_size = max(1, min(500, int(req.page_size)))
    offset = page * page_size

    columns: list[str] = []
    preview: list[dict[str, Any]] = []
    total_rows = 0
    sheet_names: list[str] | None = None
    selected_sheet: str | None = None

    try:
        if file_type == "excel":
            try:
                sheet_names = _list_excel_sheet_names(file_path)
            except Exception as exc:
                raise InternalServiceError(str(exc)) from exc
            payload = req.payload or {}
            requested_sheet = payload.get("sheet_name")
            selected_sheet = requested_sheet or (
                sheet_names[0] if sheet_names else None
            )

            try:
                if selected_sheet is not None:
                    base_df = _read_excel_sheet(file_path, selected_sheet)
                else:
                    base_df = _coerce_excel_result_to_dataframe(
                        pl.read_excel(file_path, sheet_id=0)
                    )
            except Exception as exc:
                raise InternalServiceError(str(exc)) from exc
            total_rows = int(base_df.height)
            df = base_df.slice(offset, page_size)

            columns = list(df.columns)
            preview = df.fill_null("None").to_dicts()

        elif file_type == "zip":
            df = read_zip_file(file_path)
            total_rows = int(df.height)
            if offset or page_size:
                df = df.slice(offset, page_size)
            columns = list(df.columns)
            preview = df.fill_null("None").to_dicts()
        elif file_type == "text":
            df = read_text_file(file_path)
            total_rows = int(df.height)
            if offset or page_size:
                df = df.slice(offset, page_size)
            columns = list(df.columns)
            preview = df.fill_null("None").to_dicts()
        else:
            lf = _lazy_scan(file_path, file_type).slice(offset, page_size)
            df = cast(pl.DataFrame, lf.collect())
            columns = list(df.columns)
            preview = df.fill_null("None").to_dicts()
            total_rows = 0

        return FilePreviewResponse(
            filename=req.filename,
            file_type=file_type,
            supported_types=supported_types,
            columns=columns,
            preview=preview,
            total_rows=total_rows,
            sheet_names=sheet_names,
            selected_sheet=selected_sheet,
        )
    except Exception as e:
        raise InvalidInputError(f"Error generating preview: {str(e)}")
# ── File info ──────────────────────────────────────────────────────────────


@router.get("/{filename:path}/info", response_model=FileInfoResponse)
async def get_file_info(filename: str, current_user: dict = Depends(get_current_user)):
    """Get detailed file information.

    Flow:
    - Resolve authentication and request parameters from FastAPI dependencies.
    - Delegate validation, manager calls, artifacts, or state changes to the
      owning helper.
    - Shape the response payload or raise the HTTP error the client should see.

    Used by:
    - Frontend and API clients through the FastAPI GET /{filename:path}/info route.
    """
    user_id = current_user["id"]
    data_folder = get_user_data_folder(user_id)
    file_path = data_folder / filename

    if not validate_file_path(file_path, data_folder):
        raise AccessDeniedError("Access denied: file outside allowed directory")
    if not file_path.exists():
        raise FileNotFoundError(f"File {filename} not found")
    stat = file_path.stat()
    file_type = detect_file_type(filename)

    return {
        "filename": filename,
        "size_Byte": stat.st_size,
        "created_at": stat.st_ctime,
        "modified_at": stat.st_mtime,
        "file_type": file_type,
    }


# ── Raw file ───────────────────────────────────────────────────────────────


@router.get("/raw")
async def get_raw_file(
    path: str = Query(..., description="Path relative to the user's data directory"),
    current_user: dict = Depends(get_current_user),
):
    """Return raw UTF-8 text content for a file inside the user's data folder.

    Flow:
    - Resolve authentication and request parameters from FastAPI dependencies.
    - Delegate validation, manager calls, artifacts, or state changes to the
      owning helper.
    - Shape the response payload or raise the HTTP error the client should see.

    Used by:
    - Frontend and API clients through the FastAPI GET /raw route.
    """
    user_id = current_user["id"]
    data_folder = get_user_data_folder(user_id)
    file_path = _resolve_user_file_path(path, data_folder)

    if not file_path.exists() or not file_path.is_file():
        raise FileNotFoundError(f"File {path} not found")
    try:
        content = file_path.read_text(encoding="utf-8")
    except UnicodeDecodeError as exc:
        raise InvalidInputError("File is not valid UTF-8 text") from exc
    except OSError as exc:
        raise InternalServiceError(f"Error reading file: {str(exc)}") from exc
    media_type = "text/markdown" if file_path.suffix.lower() == ".md" else "text/plain"
    return Response(content=content, media_type=media_type)


# ── Download (catch-all, must be last) ─────────────────────────────────────


@router.get("/{filename:path}")
async def download_file(filename: str, current_user: dict = Depends(get_current_user)):
    """Download user's file.

    Flow:
    - Resolve authentication and request parameters from FastAPI dependencies.
    - Delegate validation, manager calls, artifacts, or state changes to the
      owning helper.
    - Shape the response payload or raise the HTTP error the client should see.

    Used by:
    - Frontend and API clients through the FastAPI GET /{filename:path} route.
    """
    user_id = current_user["id"]
    data_folder = get_user_data_folder(user_id)
    file_path = data_folder / filename

    if not validate_file_path(file_path, data_folder):
        raise AccessDeniedError("Access denied: file outside allowed directory")
    if not file_path.exists():
        raise FileNotFoundError(f"File {filename} not found")
    def iterfile():
        """Stream file content in chunks for download.

        Called by:
        - The ``download_file`` local workflow.
        """

        with open(file_path, mode="rb") as file_like:
            yield from file_like

    download_filename = file_path.name

    return StreamingResponse(
        iterfile(),
        media_type="application/octet-stream",
        headers={"Content-Disposition": f"attachment; filename={download_filename}"},
    )
