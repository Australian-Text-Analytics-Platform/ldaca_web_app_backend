"""Global DuckDB cache for tokenisation results, exposed as an elementwise expr.

Nodes store a versioned tokenisation spec in ``Node.derived``. Analyses
attach the cache-aware tokenization to a LazyFrame with
``hydrate_derived_tokens_lazyframe`` (or build the expression directly via
``cached_tokens_expr``). Both require ``user_id`` so each user's tokens land
in their own ``user_cache/tokens.duckdb``. Polars treats the expression as
elementwise, so ``filter`` / ``slice`` on base columns pushes below it and
only the rows that survive the predicate are ever tokenized.

Per chunk, the expression: hashes the input → looks up cached tokens by
content hash → tokenizes only the misses through the raw
``polars_text.tokenize_with_offsets`` plugin → persists them with
``INSERT OR IGNORE`` → emits the per-row token structs in input order.
"""

from __future__ import annotations

import functools
import hashlib
import json
import logging
import threading
from pathlib import Path
from typing import Any, cast

import duckdb
import polars as pl

from .utils import get_user_cache_folder

logger = logging.getLogger(__name__)

TOKENS_CACHE_FILENAME = "tokens.duckdb"


def tokens_cache_path(user_id: str) -> Path:
    """Return the per-user DuckDB token cache path, creating its dir if needed."""
    return get_user_cache_folder(user_id) / TOKENS_CACHE_FILENAME


_TOKEN_STRUCT_DTYPE: pl.DataType = pl.Struct(
    [
        pl.Field("token", pl.String),
        pl.Field("start", pl.Int64),
        pl.Field("end", pl.Int64),
    ]
)
_TOKENS_DTYPE: pl.DataType = pl.List(_TOKEN_STRUCT_DTYPE)

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS token_cache (
    model VARCHAR NOT NULL,
    params_hash VARCHAR NOT NULL,
    content_hash VARCHAR NOT NULL,
    tokens VARCHAR[] NOT NULL,
    start_offsets BIGINT[] NOT NULL,
    end_offsets BIGINT[] NOT NULL,
    PRIMARY KEY (model, params_hash, content_hash)
)
"""

_DB_LOCK = threading.Lock()


def _params_hash(params: dict[str, Any]) -> str:
    blob = json.dumps(params, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(blob).hexdigest()


def _hash_text(value: Any) -> str:
    text = "" if value is None else str(value)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _connect(user_id: str) -> duckdb.DuckDBPyConnection:
    path = tokens_cache_path(user_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(path))
    conn.execute(_SCHEMA_SQL)
    return conn


def _fetch_cached(
    conn: duckdb.DuckDBPyConnection,
    *,
    model: str,
    params_hash: str,
    hashes: list[str],
) -> dict[str, list[dict[str, Any]]]:
    if not hashes:
        return {}
    requested = pl.DataFrame({"h": list(dict.fromkeys(hashes))})
    conn.register("__requested_hashes", requested)
    try:
        rows = conn.execute(
            """
            SELECT c.content_hash, c.tokens, c.start_offsets, c.end_offsets
            FROM token_cache c
            JOIN __requested_hashes r ON c.content_hash = r.h
            WHERE c.model = ? AND c.params_hash = ?
            """,
            [model, params_hash],
        ).fetchall()
    finally:
        conn.unregister("__requested_hashes")
    out: dict[str, list[dict[str, Any]]] = {}
    for content_hash, toks, starts, ends in rows:
        toks_list = list(toks or [])
        starts_list = list(starts or [])
        ends_list = list(ends or [])
        n = min(len(toks_list), len(starts_list), len(ends_list))
        out[str(content_hash)] = [
            {
                "token": str(toks_list[i]),
                "start": int(starts_list[i]),
                "end": int(ends_list[i]),
            }
            for i in range(n)
        ]
    return out


def _persist_new(
    conn: duckdb.DuckDBPyConnection,
    *,
    model: str,
    params_hash: str,
    new_entries: list[tuple[str, list[dict[str, Any]]]],
) -> None:
    if not new_entries:
        return
    records: list[dict[str, Any]] = []
    for content_hash, tokens in new_entries:
        toks: list[str] = []
        starts: list[int] = []
        ends: list[int] = []
        for entry in tokens or []:
            if entry is None:
                continue
            toks.append(str(entry.get("token") or ""))
            starts.append(int(entry.get("start") or 0))
            ends.append(int(entry.get("end") or 0))
        records.append(
            {
                "model": model,
                "params_hash": params_hash,
                "content_hash": content_hash,
                "tokens": toks,
                "start_offsets": starts,
                "end_offsets": ends,
            }
        )
    incoming = pl.DataFrame(records)
    conn.register("__incoming_tokens", incoming)
    try:
        conn.execute(
            "INSERT OR IGNORE INTO token_cache SELECT * FROM __incoming_tokens"
        )
    finally:
        conn.unregister("__incoming_tokens")


def _tokenize_misses(
    texts: list[str],
    *,
    model: str,
    lowercase: bool,
    remove_punct: bool,
) -> list[list[dict[str, Any]]]:
    """Tokenize a batch of miss texts via the raw plugin. Spy point for tests."""
    if not texts:
        return []
    import polars_text as pt

    miss_df = cast(
        pl.DataFrame,
        pl.DataFrame({"__src__": texts})
        .lazy()
        .select(
            pt.tokenize_with_offsets(
                pl.col("__src__"),
                model=model,
                lowercase=lowercase,
                remove_punct=remove_punct,
            ).alias("__tokens__")
        )
        .collect(),
    )
    return miss_df["__tokens__"].to_list()


def _tokenize_chunk(
    s: pl.Series,
    *,
    user_id: str,
    model: str,
    params_hash: str,
    lowercase: bool,
    remove_punct: bool,
) -> pl.Series:
    values = s.to_list()
    if not values:
        return pl.Series(name=s.name, values=[], dtype=_TOKENS_DTYPE)
    hashes = [_hash_text(v) for v in values]
    texts = ["" if v is None else str(v) for v in values]

    with _DB_LOCK:
        conn = _connect(user_id)
        try:
            cached = _fetch_cached(
                conn, model=model, params_hash=params_hash, hashes=hashes
            )
            # Deduplicate miss texts so we don't re-tokenize repeated rows in
            # the same chunk.
            unique_misses: dict[str, str] = {}
            for h, t in zip(hashes, texts):
                if h not in cached and h not in unique_misses:
                    unique_misses[h] = t
            if unique_misses:
                miss_hashes = list(unique_misses.keys())
                miss_texts = list(unique_misses.values())
                computed = _tokenize_misses(
                    miss_texts,
                    model=model,
                    lowercase=lowercase,
                    remove_punct=remove_punct,
                )
                new_entries = list(zip(miss_hashes, computed))
                _persist_new(
                    conn,
                    model=model,
                    params_hash=params_hash,
                    new_entries=new_entries,
                )
                for h, tokens in new_entries:
                    cached[h] = tokens or []
        finally:
            conn.close()

    out = [cached.get(h, []) for h in hashes]
    return pl.Series(name=s.name, values=out, dtype=_TOKENS_DTYPE)


def cached_tokens_expr(
    source_expr: pl.Expr,
    *,
    user_id: str,
    model: str,
    lowercase: bool = True,
    remove_punct: bool = True,
) -> pl.Expr:
    """Elementwise expression producing a per-row tokens list, cache-backed.

    The expression hashes each row, fetches cached tokens for known hashes,
    tokenizes only the misses through ``polars_text.tokenize_with_offsets``,
    and persists them. ``is_elementwise=True`` lets Polars push ``filter``
    and ``slice`` on base columns below this node so the cache is consulted
    only for surviving rows.
    """
    params = {"lowercase": lowercase, "remove_punct": remove_punct}
    ph = _params_hash(params)
    fn = functools.partial(
        _tokenize_chunk,
        user_id=user_id,
        model=model,
        params_hash=ph,
        lowercase=lowercase,
        remove_punct=remove_punct,
    )
    return source_expr.cast(pl.Utf8, strict=False).map_batches(
        fn,
        return_dtype=_TOKENS_DTYPE,
        is_elementwise=True,
    )


def hydrate_derived_tokens_lazyframe(
    base_lf: pl.LazyFrame,
    *,
    node: Any,
    source_column: str,
    derived_name: str,
    user_id: str,
) -> pl.LazyFrame:
    """Lazily attach a derived tokens column registered on ``node``.

    Short-circuits if the column is already physically present. Otherwise
    reads the model and tokenisation params from ``node.derived[derived_name]``
    and attaches a cache-backed elementwise expression keyed on ``user_id``.
    """
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
    params = derived_meta.get("params") or {}
    if not isinstance(model, str):
        return base_lf

    return base_lf.with_columns(
        cached_tokens_expr(
            pl.col(source_column),
            user_id=user_id,
            model=model,
            lowercase=bool(params.get("lowercase", True)),
            remove_punct=bool(params.get("remove_punct", True)),
        ).alias(derived_name)
    )


__all__ = [
    "TOKENS_CACHE_FILENAME",
    "cached_tokens_expr",
    "hydrate_derived_tokens_lazyframe",
    "tokens_cache_path",
]
