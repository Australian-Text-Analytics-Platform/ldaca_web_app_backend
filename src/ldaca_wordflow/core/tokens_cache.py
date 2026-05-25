"""Per-user DuckDB cache for tokenisation results.

The cache is performance state, not workspace state. Nodes store a versioned
tokenisation spec in ``Node.derived``; analyses call
``hydrate_tokens_lazyframe`` to temporarily attach the requested token column.
The hydrated LazyFrame is never assigned back to ``Node.data``.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

import polars as pl

from .utils import get_user_cache_folder

logger = logging.getLogger(__name__)

CACHE_ROOT_ENV = "LDACA_TOKENS_CACHE_DIR"
TOKENS_CACHE_FILENAME = "tokens.duckdb"
TOKENS_CACHE_SCHEMA_VERSION = 1
CONTENT_HASH_COLUMN = "__ldaca_content_hash__"


def tokens_cache_path(user_id: str) -> Path:
    """Return the user-specific DuckDB token cache path without creating it."""
    override = os.environ.get(CACHE_ROOT_ENV)
    if override:
        root = Path(override).expanduser() / user_id
    else:
        root = get_user_cache_folder(user_id)
    root.mkdir(parents=True, exist_ok=True)
    return root / TOKENS_CACHE_FILENAME


def tokens_cache_dir(user_id: str) -> Path:
    """Return the parent directory for the user-specific token cache."""
    return tokens_cache_path(user_id).parent


def _params_hash(params: dict[str, Any]) -> str:
    blob = json.dumps(params, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(blob).hexdigest()


def _hash_text(value: Any) -> str:
    text = "" if value is None else str(value)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def content_hash_expr(source_column: str) -> pl.Expr:
    """Stable text-content fingerprint expression for cache lookup keys."""
    return (
        pl.col(source_column)
        .cast(pl.Utf8, strict=False)
        .map_elements(_hash_text, return_dtype=pl.String)
        .alias(CONTENT_HASH_COLUMN)
    )


def _connect(user_id: str):
    import duckdb

    path = tokens_cache_path(user_id)
    conn = duckdb.connect(str(path))
    _ensure_schema(conn)
    return conn


def ensure_tokens_cache(user_id: str) -> Path:
    """Create the DuckDB file and schema if they are missing."""
    conn = _connect(user_id)
    conn.close()
    return tokens_cache_path(user_id)


def _ensure_schema(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS token_cache (
            model VARCHAR NOT NULL,
            params_hash VARCHAR NOT NULL,
            content_hash VARCHAR NOT NULL,
            tokens VARCHAR[] NOT NULL,
            input_ids INTEGER[] NOT NULL,
            start_offsets BIGINT[] NOT NULL,
            end_offsets BIGINT[] NOT NULL,
            pos_tags VARCHAR[] NOT NULL,
            schema_version INTEGER NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
            last_accessed_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
            PRIMARY KEY (model, params_hash, content_hash)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS token_cache_lookup_idx
        ON token_cache (model, params_hash, content_hash)
        """
    )


def read_cached_hashes(user_id: str, model: str, params: dict[str, Any]) -> set[str]:
    """Return content hashes already cached for ``(model, params)``."""
    params_key = _params_hash(params)
    conn = _connect(user_id)
    try:
        rows = conn.execute(
            """
            SELECT content_hash
            FROM token_cache
            WHERE model = ? AND params_hash = ?
            """,
            [model, params_key],
        ).fetchall()
    finally:
        conn.close()
    return {str(row[0]) for row in rows}


def _unique_source_rows(base_lf: pl.LazyFrame, source_column: str) -> pl.LazyFrame:
    return base_lf.select(
        content_hash_expr(source_column),
        pl.col(source_column).cast(pl.Utf8, strict=False).alias("__src__"),
    ).unique(subset=[CONTENT_HASH_COLUMN])


def _exclude_cached_hashes(
    source_rows_lf: pl.LazyFrame, cached_hashes: set[str]
) -> pl.LazyFrame:
    if not cached_hashes:
        return source_rows_lf
    return source_rows_lf.filter(
        ~pl.col(CONTENT_HASH_COLUMN).is_in(list(cached_hashes))
    )


def _tokenize_source_rows(
    rows_lf: pl.LazyFrame,
    *,
    model: str,
    params: dict[str, Any],
) -> pl.DataFrame:
    import polars_text as pt

    return cast(
        pl.DataFrame,
        rows_lf.select(
            pl.col(CONTENT_HASH_COLUMN),
            pt.tokenize_with_offsets(
                pl.col("__src__"),
                model=model,
                lowercase=bool(params.get("lowercase", True)),
                remove_punct=bool(params.get("remove_punct", True)),
            ).alias("tokens"),
        ).collect(),
    )


def _token_rows_to_storage_frame(
    rows: pl.DataFrame,
    *,
    model: str,
    params: dict[str, Any],
) -> pl.DataFrame:
    params_key = _params_hash(params)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    records: list[dict[str, Any]] = []
    for row in rows.to_dicts():
        token_structs = row.get("tokens") or []
        token_texts: list[str] = []
        start_offsets: list[int] = []
        end_offsets: list[int] = []
        for token_struct in token_structs:
            if token_struct is None:
                continue
            token_texts.append(str(token_struct.get("token") or ""))
            start_offsets.append(int(token_struct.get("start") or 0))
            end_offsets.append(int(token_struct.get("end") or 0))
        records.append(
            {
                "model": model,
                "params_hash": params_key,
                "content_hash": str(row[CONTENT_HASH_COLUMN]),
                "tokens": token_texts,
                "input_ids": [],
                "start_offsets": start_offsets,
                "end_offsets": end_offsets,
                "pos_tags": [],
                "schema_version": TOKENS_CACHE_SCHEMA_VERSION,
                "created_at": now,
                "last_accessed_at": now,
            }
        )
    return pl.DataFrame(records) if records else pl.DataFrame()


def write_or_append_cache(
    user_id: str,
    model: str,
    params: dict[str, Any],
    new_rows: pl.DataFrame,
) -> Path:
    """Persist freshly tokenised rows into the user-specific DuckDB cache."""
    expected_cols = {CONTENT_HASH_COLUMN, "tokens"}
    missing = expected_cols - set(new_rows.columns)
    if missing:
        raise ValueError(
            f"write_or_append_cache: new_rows missing columns {sorted(missing)}; "
            f"got {new_rows.columns}"
        )
    if new_rows.height == 0:
        ensure_tokens_cache(user_id)
        return tokens_cache_path(user_id)

    storage_frame = _token_rows_to_storage_frame(new_rows, model=model, params=params)
    conn = _connect(user_id)
    try:
        conn.register("incoming_token_rows", storage_frame)
        conn.execute(
            """
            INSERT OR IGNORE INTO token_cache
            SELECT * FROM incoming_token_rows
            """
        )
        conn.unregister("incoming_token_rows")
    finally:
        conn.close()
    return tokens_cache_path(user_id)


def _empty_tokens_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            CONTENT_HASH_COLUMN: pl.String,
            "tokens": pl.List(
                pl.Struct(
                    [
                        pl.Field("token", pl.String),
                        pl.Field("start", pl.Int64),
                        pl.Field("end", pl.Int64),
                    ]
                )
            ),
        }
    )


def read_tokens_for_hashes(
    user_id: str,
    model: str,
    params: dict[str, Any],
    content_hashes: list[str],
) -> pl.DataFrame:
    """Return cached token structs for a set of content hashes."""
    if not content_hashes:
        return _empty_tokens_frame()

    requested = pl.DataFrame({CONTENT_HASH_COLUMN: list(dict.fromkeys(content_hashes))})
    params_key = _params_hash(params)
    conn = _connect(user_id)
    try:
        conn.register("requested_token_hashes", requested)
        rows = conn.execute(
            """
            SELECT c.content_hash, c.tokens, c.start_offsets, c.end_offsets
            FROM token_cache c
            JOIN requested_token_hashes r
              ON c.content_hash = r.__ldaca_content_hash__
            WHERE c.model = ? AND c.params_hash = ?
            """,
            [model, params_key],
        ).fetchall()
        conn.unregister("requested_token_hashes")
    finally:
        conn.close()

    records: list[dict[str, Any]] = []
    for content_hash, tokens, starts, ends in rows:
        token_values = list(tokens or [])
        start_values = list(starts or [])
        end_values = list(ends or [])
        token_structs = [
            {
                "token": str(token),
                "start": int(start_values[index]),
                "end": int(end_values[index]),
            }
            for index, token in enumerate(token_values)
            if index < len(start_values) and index < len(end_values)
        ]
        records.append(
            {
                CONTENT_HASH_COLUMN: str(content_hash),
                "tokens": token_structs,
            }
        )
    if not records:
        return _empty_tokens_frame()
    return pl.DataFrame(
        records,
        schema=_empty_tokens_frame().schema,
    )


def hydrate_tokens_lazyframe(
    base_lf: pl.LazyFrame,
    *,
    source_column: str,
    model: str,
    params: dict[str, Any],
    user_id: str,
    derived_name: str,
) -> pl.LazyFrame:
    """Return a temporary LazyFrame with ``derived_name`` joined from cache."""
    source_rows_lf = _unique_source_rows(base_lf, source_column)
    source_rows_df = cast(pl.DataFrame, source_rows_lf.collect())
    all_hashes = [str(value) for value in source_rows_df[CONTENT_HASH_COLUMN].to_list()]
    cached_hashes = read_cached_hashes(user_id, model, params)
    missing_rows_lf = _exclude_cached_hashes(source_rows_df.lazy(), cached_hashes)
    missing_rows_df = cast(pl.DataFrame, missing_rows_lf.collect())
    if missing_rows_df.height > 0:
        new_tokens_df = _tokenize_source_rows(
            missing_rows_df.lazy(), model=model, params=params
        )
        write_or_append_cache(user_id, model, params, new_tokens_df)

    cache_df = read_tokens_for_hashes(user_id, model, params, all_hashes)
    cache_lf = cache_df.lazy().rename({"tokens": derived_name})
    return (
        base_lf.drop(derived_name, strict=False)
        .with_columns(content_hash_expr(source_column))
        .join(cache_lf, on=CONTENT_HASH_COLUMN, how="left")
        .drop(CONTENT_HASH_COLUMN)
    )


def hydrate_derived_tokens_lazyframe(
    base_lf: pl.LazyFrame,
    *,
    node: Any,
    source_column: str,
    user_id: str,
    derived_name: str,
) -> pl.LazyFrame:
    """Hydrate a registered tokens column unless it already exists physically."""
    if derived_name in base_lf.collect_schema().names():
        return base_lf

    derived_registry = getattr(node, "derived", {})
    derived_meta = (
        derived_registry.get(derived_name)
        if isinstance(derived_registry, dict)
        else None
    )
    if not isinstance(derived_meta, dict):
        return base_lf

    model = derived_meta.get("model")
    params = derived_meta.get("params")
    if not isinstance(model, str) or not isinstance(params, dict):
        return base_lf

    return hydrate_tokens_lazyframe(
        base_lf,
        source_column=source_column,
        model=model,
        params=params,
        user_id=user_id,
        derived_name=derived_name,
    )


def cache_exists(user_id: str, model: str, params: dict[str, Any]) -> bool:
    return bool(read_cached_hashes(user_id, model, params))


def sweep_unreferenced(
    user_id: str | None = None,
    *,
    grace_period_days: int = 7,
    now: datetime | None = None,
) -> dict[str, list[str]]:
    """Compatibility maintenance hook; DuckDB token rows are recomputable."""
    if user_id is None:
        return {}
    ensure_tokens_cache(user_id)
    return {user_id: []}


def _reset_for_tests(user_id: str) -> None:
    path = tokens_cache_path(user_id)
    if path.exists():
        path.unlink()


__all__ = [
    "CACHE_ROOT_ENV",
    "CONTENT_HASH_COLUMN",
    "TOKENS_CACHE_FILENAME",
    "cache_exists",
    "content_hash_expr",
    "ensure_tokens_cache",
    "hydrate_derived_tokens_lazyframe",
    "hydrate_tokens_lazyframe",
    "read_cached_hashes",
    "read_tokens_for_hashes",
    "sweep_unreferenced",
    "tokens_cache_dir",
    "tokens_cache_path",
    "write_or_append_cache",
]
