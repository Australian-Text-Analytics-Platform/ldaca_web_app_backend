"""File-type detection, data loading, dtype normalization.

Used by:
- backend API routes, backend tests, core workspace and worker services because they need
  a backend boundary that validates inputs before delegating to workspace or worker state.

Flow: detect file type from extension, load through the appropriate reader (Polars for
    tabular, text/zip for documents), normalize column dtypes, and return dataframes.
"""

import logging
import warnings
import zipfile
from pathlib import Path
from typing import Any, Iterable

import polars as pl

logger = logging.getLogger(__name__)

_DEFAULT_TEXT_FILE_EXTENSIONS: set[str] = {
    ".txt",
    ".md",
    ".rst",
    ".log",
    ".csv",
    ".tsv",
    ".json",
    ".jsonl",
    ".ndjson",
    ".xml",
    ".html",
    ".htm",
    ".yaml",
    ".yml",
    ".toml",
    ".ini",
    ".cfg",
    ".text",
}


def detect_file_type(filename: str) -> str:
    """Detect file type from extension.

    Used by:
    - backend API routes, backend tests, core workspace and worker services because they
      need a backend boundary that validates inputs before delegating to workspace or worker
      state.
    """
    ext = Path(filename).suffix.lower()
    type_map = {
        ".csv": "csv",
        ".json": "json",
        ".jsonl": "jsonl",
        ".parquet": "parquet",
        ".xlsx": "excel",
        ".xls": "excel",
        ".xlsm": "excel",
        ".xlsb": "excel",
        ".ods": "excel",
        ".txt": "text",
        ".text": "text",
        ".md": "text",
        ".rst": "text",
        ".log": "text",
        ".tsv": "tsv",
        ".zip": "zip",
    }
    return type_map.get(ext, "unknown")


def _coerce_excel_result_to_dataframe(
    result: Any, sheet_name: str | None = None
) -> pl.DataFrame:
    """Coerce excel result to dataframe values into the shape expected by workspace data loading utilities.

    Called by:
    - ``load_data_file`` in this module because the local file and dataframe normalization
      flow needs this step kept close to the code that consumes it.
    """
    if isinstance(result, pl.DataFrame):
        return result
    if isinstance(result, dict):
        if (
            sheet_name
            and sheet_name in result
            and isinstance(result[sheet_name], pl.DataFrame)
        ):
            return result[sheet_name]
        for value in result.values():
            if isinstance(value, pl.DataFrame):
                return value
    raise RuntimeError(
        f"Unexpected Excel read result type: {type(result).__name__}"
    )


def load_data_file(
    file_path: Path,
    sheet_name: str | None = None,
) -> Any:
    """Load data file into appropriate DataFrame type — defaults to polars LazyFrame for efficiency.

    Used by:
    - backend API routes, backend tests because they need a backend boundary that validates
      inputs before delegating to workspace or worker state.
    """
    file_type = detect_file_type(file_path.name)

    if file_type == "csv":
        return pl.scan_csv(file_path)
    elif file_type == "parquet":
        return pl.scan_parquet(file_path)
    elif file_type == "json":
        return pl.read_json(file_path)
    elif file_type == "tsv":
        return pl.scan_csv(file_path, separator="\t")
    elif file_type == "excel":
        try:
            if sheet_name:
                return _coerce_excel_result_to_dataframe(
                    pl.read_excel(file_path, sheet_name=sheet_name), sheet_name
                )
            return _coerce_excel_result_to_dataframe(pl.read_excel(file_path), sheet_name)
        except Exception as ex:
            try:
                return _coerce_excel_result_to_dataframe(
                    pl.read_excel(file_path, sheet_id=0), sheet_name
                )
            except Exception as ex2:
                logger.error("Failed to read Excel file %s: %s", file_path, ex2)
                raise RuntimeError(f"Failed to read Excel via polars: {ex2}") from ex
    elif file_type == "zip":
        return read_zip_file(file_path)
    elif file_type == "text":
        return read_text_file(file_path)
    else:
        raise ValueError(f"Unsupported file type: {file_type}")


def read_text_file(file_path: Path) -> pl.DataFrame:
    """Read a plain text file into a Polars DataFrame with a single text column.

    Used by:
    - backend API routes, core workspace and worker services because they need a backend
      boundary that validates inputs before delegating to workspace or worker state.
    """
    content = file_path.read_text(encoding="utf-8", errors="replace")
    lines = content.splitlines()
    if not lines:
        return pl.DataFrame({"text": []})
    return pl.DataFrame({"text": lines})


_JS_MAX_SAFE_INTEGER = 2**53 - 1

_CANONICAL_DATETIME = pl.Datetime(time_unit="us", time_zone="UTC")
_INTEGERS_TO_PROMOTE = {
    pl.Int8,
    pl.Int16,
    pl.Int32,
    pl.UInt8,
    pl.UInt16,
    pl.UInt32,
    pl.UInt64,
}


def normalize_dtypes(
    df: pl.DataFrame,
) -> tuple[pl.DataFrame, list[dict[str, str]]]:
    """Coerce columns to the project's canonical dtype profile.

    Returns the normalized frame plus a per-column change log
    ``[{"column", "from_dtype", "to_dtype", "reason"}, ...]`` so callers can
    surface a consolidated warning to the user. The change log is empty when
    nothing needed casting.

    Used by:
    - backend API routes, backend tests because they need a backend boundary that validates
      inputs before delegating to workspace or worker state.
    """
    if df.width == 0:
        return df, []

    changes: list[dict[str, str]] = []
    casts: list[pl.Expr] = []

    for col, dtype in df.schema.items():
        if isinstance(dtype, pl.Datetime):
            time_unit = getattr(dtype, "time_unit", "us")
            time_zone = getattr(dtype, "time_zone", None)
            if time_unit == "us" and time_zone == "UTC":
                continue
            expr = pl.col(col)
            reason_parts: list[str] = []
            if time_zone is None:
                expr = expr.dt.replace_time_zone("UTC")
                reason_parts.append("naive datetime assumed UTC")
            elif time_zone != "UTC":
                expr = expr.dt.convert_time_zone("UTC")
                reason_parts.append(f"converted from {time_zone} to UTC")
            if time_unit != "us":
                expr = expr.dt.cast_time_unit("us")
                reason_parts.append(
                    f"precision {time_unit}->us "
                    "(text analytics does not need sub-microsecond resolution)"
                )
            casts.append(expr.alias(col))
            changes.append(
                {
                    "column": col,
                    "from_dtype": str(dtype),
                    "to_dtype": str(_CANONICAL_DATETIME),
                    "reason": "; ".join(reason_parts),
                }
            )
        elif dtype in _INTEGERS_TO_PROMOTE:
            casts.append(pl.col(col).cast(pl.Int64).alias(col))
            kind = "unsigned" if str(dtype).startswith("UInt") else "narrower signed"
            changes.append(
                {
                    "column": col,
                    "from_dtype": str(dtype),
                    "to_dtype": "Int64",
                    "reason": (
                        f"{kind} integer promoted to Int64 so joins/stacks "
                        "across heterogeneous sources align"
                    ),
                }
            )
        elif dtype == pl.Float32:
            casts.append(pl.col(col).cast(pl.Float64).alias(col))
            changes.append(
                {
                    "column": col,
                    "from_dtype": "Float32",
                    "to_dtype": "Float64",
                    "reason": "Float32 widened to Float64 for cross-source alignment",
                }
            )

    if casts:
        df = df.with_columns(casts)
    return df, changes


def read_zip_file(
    file_path: str | Path,
    *,
    encoding: str = "utf-8",
    errors: str = "ignore",
    text_extensions: Iterable[str] | None = None,
    include_extensionless: bool = True,
) -> pl.DataFrame:
    """Read text files from a ZIP archive into a Polars DataFrame.

    Returns a deterministic, path-sorted table with columns:
    ``file_path``, ``base_name``, ``extension``, and ``document``.

    Used by:
    - backend API routes, backend tests, core workspace and worker services because they
      need a backend boundary that validates inputs before delegating to workspace or worker
      state.
    """
    archive_path = Path(file_path)
    if not archive_path.exists():
        raise FileNotFoundError(f"ZIP archive not found: {archive_path}")

    if text_extensions is None:
        allowed_extensions = _DEFAULT_TEXT_FILE_EXTENSIONS
    else:
        allowed_extensions = {
            (ext if ext.startswith(".") else f".{ext}").lower()
            for ext in text_extensions
        }

    records: list[dict[str, str]] = []

    with zipfile.ZipFile(archive_path) as archive:
        for info in archive.infolist():
            if info.is_dir():
                continue

            inner_path = info.filename
            path_obj = Path(inner_path)
            file_name = path_obj.name
            base_name = path_obj.stem
            extension = path_obj.suffix

            if inner_path.startswith("__MACOSX/") or file_name.startswith("._"):
                continue

            suffix = extension.lower()
            if suffix:
                if suffix not in allowed_extensions:
                    continue
            elif not include_extensionless:
                continue

            with archive.open(info, "r") as file_obj:
                data = file_obj.read()

            try:
                text_content = data.decode(encoding, errors=errors)
            except UnicodeDecodeError:
                warnings.warn(
                    (
                        f"Skipping '{inner_path}' — unable to decode with "
                        f"encoding {encoding!r}"
                    ),
                    UserWarning,
                    stacklevel=2,
                )
                continue

            records.append(
                {
                    "file_path": inner_path,
                    "base_name": base_name,
                    "extension": extension,
                    "document": text_content,
                }
            )

    records.sort(key=lambda entry: entry["file_path"])

    return pl.DataFrame(
        records,
        schema={
            "file_path": pl.String,
            "base_name": pl.String,
            "extension": pl.String,
            "document": pl.String,
        },
    )
