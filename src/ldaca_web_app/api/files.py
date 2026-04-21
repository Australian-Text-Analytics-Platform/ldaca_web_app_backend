"""File management endpoints."""

import logging
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional, cast

import polars as pl
from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile
from fastapi.responses import Response, StreamingResponse

try:
    import fastexcel
except Exception:  # pragma: no cover - optional import hardening
    fastexcel: Any | None = None

from ..core.auth import get_current_user
from ..core.utils import (
    detect_file_type,
    get_user_data_folder,
    import_sample_data_for_user,
    read_text_file,
    read_zip_file,
    validate_file_path,
    validate_workspace_name,
)
from ..core.workspace import workspace_manager
from ..models import (
    CreateFolderRequest,
    CreateFolderResponse,
    FileInfoResponse,
    FilePreviewRequest,
    FilePreviewResponse,
    FilesImportTaskStartResponse,
    FilesTaskActionResponse,
    FilesTasksListResponse,
    FileTreeNodeResponse,
    FileUploadResponse,
    ImportSampleDataResponse,
    LDaCAImportRequest,
    MessageResponse,
    MoveFileRequest,
)

router = APIRouter(prefix="/files", tags=["file_management"])
logger = logging.getLogger(__name__)

README_FILENAME = "README.md"


def _delete_parent_folder_if_redundant(file_path: Path, data_folder: Path) -> None:
    """Delete the file's parent folder when it becomes empty or README-only.

    Used by:
    - `delete_file`

    Why:
    - Imported datasets often live in their own wrapper folder with a data file
      plus `README.md`. When the data file is deleted, removing the now-empty
      wrapper avoids leaving behind dead folders in the file browser.
    """
    parent = file_path.parent
    if parent == data_folder or not parent.exists() or not parent.is_dir():
        return

    if not validate_file_path(parent, data_folder):
        return

    remaining_entries = list(parent.iterdir())
    if not remaining_entries:
        parent.rmdir()
        return

    if len(remaining_entries) != 1:
        return

    remaining_entry = remaining_entries[0]
    if (
        remaining_entry.is_file()
        and remaining_entry.name.lower() == README_FILENAME.lower()
    ):
        remaining_entry.unlink()
        parent.rmdir()


def _relative_path_for_api(path: Path) -> str:
    """Normalize relative paths for API responses using forward slashes."""
    return path.as_posix()


def _visible_entry_names(names: list[str]) -> list[str]:
    """Return sorted non-hidden directory entries."""
    return sorted(name for name in names if not name.startswith("."))


def _build_file_tree(data_folder: Path) -> list[dict[str, Any]]:
    """Build a nested file tree rooted at the user's data directory."""
    root_children: list[dict[str, Any]] = []
    directory_nodes: dict[str, dict[str, Any]] = {"": {"children": root_children}}

    for current_dir, dirnames, filenames in data_folder.walk(top_down=True):
        dirnames[:] = _visible_entry_names(dirnames)
        visible_filenames = _visible_entry_names(filenames)

        relative_dir = current_dir.relative_to(data_folder)
        relative_dir_str = (
            "" if relative_dir == Path(".") else _relative_path_for_api(relative_dir)
        )
        current_children = directory_nodes[relative_dir_str]["children"]

        for dirname in dirnames:
            directory_path = (
                Path(relative_dir_str, dirname) if relative_dir_str else Path(dirname)
            )
            directory_rel = _relative_path_for_api(directory_path)
            directory_node = {
                "name": dirname,
                "path": directory_rel,
                "type": "directory",
                "children": [],
            }
            current_children.append(directory_node)
            directory_nodes[directory_rel] = directory_node

        for filename in visible_filenames:
            file_rel_path = (
                Path(relative_dir_str, filename) if relative_dir_str else Path(filename)
            )
            absolute_file_path = current_dir / filename
            current_children.append(
                {
                    "name": filename,
                    "path": _relative_path_for_api(file_rel_path),
                    "type": "file",
                    "size": absolute_file_path.stat().st_size,
                }
            )

    return root_children


def _resolve_user_file_path(relative_path: str, data_folder: Path) -> Path:
    """Resolve and validate an API-supplied path inside the user's data folder."""
    file_path = data_folder / relative_path
    if not validate_file_path(file_path, data_folder):
        raise HTTPException(status_code=400, detail="Invalid file path")
    return file_path


def _lazy_scan(file_path, file_type: str) -> pl.LazyFrame:
    """Return a Polars LazyFrame for the given file if possible.

    Prefers scan_* readers to avoid loading the whole file into memory.
    Falls back to eager read + .lazy() for formats without a native scanner.

    Used by:
    - `unified_file_preview`

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
        # Prefer scan_ndjson when available
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
        # Polars may not have scan_excel; fall back to read_excel first sheet then lazy
        try:
            df = pl.read_excel(file_path, sheet_id=0)
            return df.lazy()
        except Exception:
            return pl.DataFrame().lazy()
    return pl.DataFrame().lazy()


def _get_supported_types_by_extension(file_type: str) -> List[str]:
    """Return supported backend data representations by file extension.

    Used by:
    - `unified_file_preview`

    Why:
    - Exposes deterministic frontend capability hints per file type.
    """

    ft = (file_type or "").lower()
    mapping: Dict[str, List[str]] = {
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
    - `unified_file_preview`

    Why:
    - Isolates sheet-level reads for preview pagination.
    """
    result = pl.read_excel(file_path, sheet_name=sheet_name)
    return _coerce_excel_result_to_dataframe(result, preferred_sheet=sheet_name)


def _coerce_excel_result_to_dataframe(
    result: Any,
    preferred_sheet: Optional[str] = None,
) -> pl.DataFrame:
    """Normalize Polars Excel reads into a single DataFrame.

    Used by:
    - `_read_excel_sheet`
    - `unified_file_preview`

    Why:
    - Depending on Polars version/options, `read_excel` may return either a
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


def _list_excel_sheet_names(file_path: Path) -> List[str]:
    """Return workbook sheet names in a Polars-version-tolerant way.

    Used by:
    - `unified_file_preview`

    Why:
    - Some Polars versions return a dict for `sheet_id=None`, while others may
      return a single DataFrame (e.g., single-sheet workbooks). This helper
      normalizes both behaviors to avoid `.keys()` attribute errors.
    """
    if fastexcel is not None:
        try:
            reader = fastexcel.read_excel(str(file_path))
            names = getattr(reader, "sheet_names", None)
            if names:
                return [str(name) for name in names]
        except Exception:
            # Fall back to Polars-based detection below.
            pass

    workbook = pl.read_excel(file_path, sheet_id=None)
    if isinstance(workbook, dict):
        return [str(name) for name in workbook.keys()]
    if isinstance(workbook, pl.DataFrame):
        # Continue to XML fallback below to recover names when possible.
        pass

    # Defensive fallback for uncommon return types.
    keys = getattr(workbook, "keys", None)
    if callable(keys):
        try:
            return [str(name) for name in keys()]
        except Exception:
            pass

    # Final fallback for .xlsx/.xlsm containers: parse workbook.xml directly.
    try:
        with zipfile.ZipFile(file_path) as zf:
            with zf.open("xl/workbook.xml") as workbook_xml:
                root = ET.parse(workbook_xml).getroot()
                names: List[str] = []
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


@router.get(
    "/", response_model=list[FileTreeNodeResponse], response_model_exclude_none=True
)
async def get_user_files(current_user: dict = Depends(get_current_user)):
    """List user-visible files as a nested tree.

    Used by:
    - frontend file browser panel

    Why:
    - The frontend consumes the directory structure directly instead of
      reconstructing it from a flat file listing.
    """
    user_id = current_user["id"]
    data_folder = get_user_data_folder(user_id)
    return _build_file_tree(data_folder)


@router.post("/folders", response_model=CreateFolderResponse)
async def create_folder(
    request: CreateFolderRequest,
    current_user: dict = Depends(get_current_user),
):
    """Create a folder inside the user's data directory."""
    user_id = current_user["id"]
    data_folder = get_user_data_folder(user_id)

    is_valid, reason = validate_workspace_name(request.name)
    if not is_valid:
        raise HTTPException(status_code=400, detail=f"Invalid folder name: {reason}")

    parent_path = request.parent_path.strip()
    parent_folder = (
        data_folder
        if not parent_path
        else _resolve_user_file_path(parent_path, data_folder)
    )

    if not parent_folder.exists() or not parent_folder.is_dir():
        raise HTTPException(
            status_code=404, detail=f"Folder {parent_path or '.'} not found"
        )

    folder_path = parent_folder / request.name.strip()
    if not validate_file_path(folder_path, data_folder):
        raise HTTPException(status_code=400, detail="Invalid file path")
    if folder_path.exists():
        raise HTTPException(
            status_code=400, detail=f"Folder {request.name.strip()} already exists"
        )

    try:
        folder_path.mkdir(parents=False, exist_ok=False)
    except OSError as exc:
        raise HTTPException(
            status_code=500, detail=f"Failed to create folder: {str(exc)}"
        ) from exc

    relative_path = folder_path.relative_to(data_folder)
    return {
        "message": "Folder created",
        "path": _relative_path_for_api(relative_path),
    }


@router.post("/move", response_model=CreateFolderResponse)
async def move_file(
    request: MoveFileRequest,
    current_user: dict = Depends(get_current_user),
):
    """Move a file into another directory inside the user's data folder."""
    user_id = current_user["id"]
    data_folder = get_user_data_folder(user_id)

    source_path = _resolve_user_file_path(request.source_path, data_folder)
    target_directory = (
        data_folder
        if not request.target_directory_path.strip()
        else _resolve_user_file_path(request.target_directory_path, data_folder)
    )

    if not source_path.exists() or not source_path.is_file():
        raise HTTPException(
            status_code=404, detail=f"File {request.source_path} not found"
        )
    if not target_directory.exists() or not target_directory.is_dir():
        raise HTTPException(
            status_code=404,
            detail=f"Folder {request.target_directory_path} not found",
        )

    destination_path = target_directory / source_path.name
    if not validate_file_path(destination_path, data_folder):
        raise HTTPException(status_code=400, detail="Invalid file path")
    if destination_path.exists():
        raise HTTPException(
            status_code=400,
            detail=f"File {destination_path.name} already exists in destination",
        )

    original_source_path = source_path
    try:
        source_path.rename(destination_path)
        _delete_parent_folder_if_redundant(original_source_path, data_folder)
    except OSError as exc:
        raise HTTPException(
            status_code=500, detail=f"Failed to move file: {str(exc)}"
        ) from exc

    return {
        "message": "File moved",
        "path": _relative_path_for_api(destination_path.relative_to(data_folder)),
    }


@router.post("/upload", response_model=FileUploadResponse)
async def upload_file(file: UploadFile, current_user: dict = Depends(get_current_user)):
    """Upload file to user's data folder"""
    user_id = current_user["id"]
    data_folder = get_user_data_folder(user_id)

    if not file.filename:
        raise HTTPException(status_code=400, detail="No filename provided")

    file_path = data_folder / file.filename

    # Check if file already exists
    if file_path.exists():
        raise HTTPException(
            status_code=409, detail=f"File {file.filename} already exists"
        )

    # Save file
    with open(file_path, "wb") as buffer:
        content = await file.read()
        buffer.write(content)

    file_type = detect_file_type(file.filename)

    return {
        "filename": file.filename,
        "size": len(content),
        "upload_time": str(file_path.stat().st_ctime),
        "file_type": file_type,
        "preview_available": file_type in ["csv", "json", "parquet"],
    }


@router.delete("/{filename:path}", response_model=MessageResponse)
async def delete_file(filename: str, current_user: dict = Depends(get_current_user)):
    """Delete user's file"""
    user_id = current_user["id"]
    data_folder = get_user_data_folder(user_id)
    file_path = data_folder / filename

    # Security check
    if not validate_file_path(file_path, data_folder):
        raise HTTPException(
            status_code=403, detail="Access denied: file outside allowed directory"
        )

    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"File {filename} not found")

    file_path.unlink()
    _delete_parent_folder_if_redundant(file_path, data_folder)
    return {"message": f"File {filename} deleted successfully"}


@router.post("/import-sample-data", response_model=ImportSampleDataResponse)
async def import_sample_data(current_user: dict = Depends(get_current_user)):
    """Import (or re-import) sample data for the current user on demand."""
    user_id = current_user["id"]
    try:
        summary = import_sample_data_for_user(user_id)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    return {
        "status": "ok",
        "removed_existing": summary["removed_existing"],
        "file_count": summary["file_count"],
        "bytes_copied": summary["bytes_copied"],
        "sample_dir": summary["sample_dir"],
        "message": "Sample data imported successfully",
    }


@router.post("/import-ldaca", response_model=FilesImportTaskStartResponse)
async def import_ldaca_dataset(
    request: LDaCAImportRequest,
    current_user: dict = Depends(get_current_user),
):
    """Submit background task to import LDaCA dataset from URL.

    Used by:
    - frontend dataset import action

    Why:
    - Runs network/download/import pipeline outside request-response lifecycle.
    """
    user_id = current_user["id"]
    # LDaCA import is independent of a specific workspace.
    workspace_id = workspace_manager.get_current_workspace_id(user_id) or "global"
    tm = workspace_manager.get_task_manager(user_id)
    task_info = await tm.submit_task(
        user_id=user_id,
        workspace_id=workspace_id,
        task_type="ldaca_import",
        task_args={"url": request.url, "filename": request.filename},
    )

    return {
        "state": "running",
        "message": "LDaCA import started",
        "metadata": {
            "task_id": task_info.id,
        },
    }


@router.get("/tasks", response_model=FilesTasksListResponse)
async def list_files_tasks(current_user: dict = Depends(get_current_user)):
    """List file-import worker tasks exposed via the files API.

    Used by:
    - frontend import-task status polling

    Why:
        - Exposes file-import task status via explicit task types.
    """
    user_id = current_user["id"]
    tm = workspace_manager.get_task_manager(user_id)
    all_tasks = await tm.list(user_id=user_id)
    data = [
        task
        for task in all_tasks
        if isinstance(task, dict) and task.get("task_type") == "ldaca_import"
    ]
    return {
        "state": "successful",
        "data": data,
        "message": "Tasks retrieved successfully.",
    }


@router.post("/tasks/clear", response_model=FilesTaskActionResponse)
async def clear_files_tasks(
    task_type: Optional[str] = None,
    task_id: Optional[str] = None,
    current_user: dict = Depends(get_current_user),
):
    """Clear persisted file-import task records.

    Used by:
    - frontend task-list cleanup actions

    Why:
    - Removes completed/failed import task clutter while keeping artifacts.
    """
    user_id = current_user["id"]
    tm = workspace_manager.get_task_manager(user_id)
    if task_id:
        task = await tm.get_task(task_id)
        cleared = bool(task and task.task_type == "ldaca_import")
        if cleared:
            cleared = await tm.clear_task(task_id)
        return {
            "state": "successful",
            "data": {"cleared_count": 1 if cleared else 0},
            "message": "Task cleared successfully.",
        }
    effective_task_type = task_type or "ldaca_import"
    count = await tm.clear_tasks(task_type=effective_task_type, user_id=user_id)
    return {
        "state": "successful",
        "data": {"cleared_count": count},
        "message": "All tasks cleared successfully.",
    }


@router.post("/preview", response_model=FilePreviewResponse)
async def unified_file_preview(
    req: FilePreviewRequest, current_user: dict = Depends(get_current_user)
):
    """Unified file preview endpoint.

    - Returns supported types based on extension.
    - Provides preview data (first few rows or page slice).
    - For Excel files, returns sheet_names and supports selecting sheet via payload.sheet_name.

    Used by:
    - frontend file preview modal/table

    Why:
    - Provides one format-aware preview API for heterogeneous file types.
    """
    user_id = current_user["id"]
    data_folder = get_user_data_folder(user_id)
    file_path = data_folder / req.filename

    if not validate_file_path(file_path, data_folder):
        raise HTTPException(
            status_code=403, detail="Access denied: file outside allowed directory"
        )
    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"File {req.filename} not found")

    file_type = detect_file_type(file_path.name)
    supported_types = _get_supported_types_by_extension(file_type)

    # Pagination normalization
    page = max(0, int(req.page))
    page_size = max(1, min(500, int(req.page_size)))
    offset = page * page_size

    columns: List[str] = []
    preview: List[Dict[str, Any]] = []
    total_rows = 0
    sheet_names: Optional[List[str]] = None
    selected_sheet: Optional[str] = None

    try:
        if file_type == "excel":
            try:
                sheet_names = _list_excel_sheet_names(file_path)
            except Exception as exc:
                raise HTTPException(status_code=500, detail=str(exc)) from exc

            # Choose sheet: payload.sheet_name or first sheet
            payload = req.payload or {}
            requested_sheet = payload.get("sheet_name")
            selected_sheet = requested_sheet or (
                sheet_names[0] if sheet_names else None
            )

            try:
                if selected_sheet is not None:
                    base_df = _read_excel_sheet(file_path, selected_sheet)
                else:
                    # Sheet names unavailable (e.g., single-sheet workbook on some
                    # Polars versions): preview the first sheet by index.
                    base_df = _coerce_excel_result_to_dataframe(
                        pl.read_excel(file_path, sheet_id=0)
                    )
            except Exception as exc:
                raise HTTPException(status_code=500, detail=str(exc)) from exc

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
            # Non-Excel: prefer lazy scan where available
            lf = _lazy_scan(file_path, file_type).slice(offset, page_size)
            df = cast(pl.DataFrame, lf.collect())
            columns = list(df.columns)
            preview = df.fill_null("None").to_dicts()
            total_rows = 0  # unknown unless we count eagerly

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
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=400, detail=f"Error generating preview: {str(e)}"
        )


@router.get("/{filename:path}/info", response_model=FileInfoResponse)
async def get_file_info(filename: str, current_user: dict = Depends(get_current_user)):
    """Get detailed file information"""
    user_id = current_user["id"]
    data_folder = get_user_data_folder(user_id)
    file_path = data_folder / filename

    # Security check
    if not validate_file_path(file_path, data_folder):
        raise HTTPException(
            status_code=403, detail="Access denied: file outside allowed directory"
        )

    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"File {filename} not found")

    stat = file_path.stat()
    file_type = detect_file_type(filename)

    return {
        "filename": filename,
        "size_Byte": stat.st_size,
        "created_at": stat.st_ctime,
        "modified_at": stat.st_mtime,
        "file_type": file_type,
    }


@router.get("/raw")
async def get_raw_file(
    path: str = Query(..., description="Path relative to the user's data directory"),
    current_user: dict = Depends(get_current_user),
):
    """Return raw UTF-8 text content for a file inside the user's data folder."""
    user_id = current_user["id"]
    data_folder = get_user_data_folder(user_id)
    file_path = _resolve_user_file_path(path, data_folder)

    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404, detail=f"File {path} not found")

    try:
        content = file_path.read_text(encoding="utf-8")
    except UnicodeDecodeError as exc:
        raise HTTPException(
            status_code=400, detail="File is not valid UTF-8 text"
        ) from exc
    except OSError as exc:
        raise HTTPException(
            status_code=500, detail=f"Error reading file: {str(exc)}"
        ) from exc

    media_type = "text/markdown" if file_path.suffix.lower() == ".md" else "text/plain"
    return Response(content=content, media_type=media_type)


# Keep the catch-all download route LAST so that more specific routes like
# "/{filename:path}/preview" and "/{filename:path}/info" are matched first.
@router.get("/{filename:path}")
async def download_file(filename: str, current_user: dict = Depends(get_current_user)):
    """Download user's file"""
    user_id = current_user["id"]
    data_folder = get_user_data_folder(user_id)
    file_path = data_folder / filename

    # Security check
    if not validate_file_path(file_path, data_folder):
        raise HTTPException(
            status_code=403, detail="Access denied: file outside allowed directory"
        )

    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"File {filename} not found")

    def iterfile():
        with open(file_path, mode="rb") as file_like:
            yield from file_like

    # Get just the filename for the download header
    download_filename = file_path.name

    return StreamingResponse(
        iterfile(),
        media_type="application/octet-stream",
        headers={"Content-Disposition": f"attachment; filename={download_filename}"},
    )
