"""Utilities for searching and importing LDaCA records through the Oni API."""

from __future__ import annotations

import asyncio
from enum import StrEnum
from typing import Any
from urllib.parse import parse_qs, urlparse

import httpx


class OniSearchMethod(StrEnum):
    """Supported LDaCA Data Portal search modes."""

    IDENTIFIER = "identifier"
    KEYWORD = "keyword"
    ID = "id"
    STRING = "string"
    COLLECTION = "collection"
    FILE_FORMAT = "file_format"
    ALL = "all"


DEFAULT_SOURCE_FIELDS = [
    "@id",
    "@type",
    "_crateId",
    "_memberOf",
    "_root",
    "_mainCollection",
    "name",
    "description",
    "encodingFormat",
    "license",
    "conformsTo",
    "_access",
    "error",
]

DEFAULT_SEARCH_FIELDS = ["name.@value", "description.@value", "_text", "@id"]


def extract_ldaca_identifier(value: str) -> str | None:
    """Extract an ARCP identifier from a raw id or LDaCA portal URL."""
    candidate = value.strip()
    if not candidate:
        return None
    if candidate.startswith("arcp://"):
        return candidate

    parsed = urlparse(candidate)
    query_values = parse_qs(parsed.query)
    for key in ("id", "_crateId"):
        values = query_values.get(key)
        if values and values[0].strip():
            return values[0].strip()
    return None


def jsonld_value(value: Any) -> Any:
    """Normalize common JSON-LD value/id containers from Oni search results."""
    if isinstance(value, list):
        normalized = [jsonld_value(item) for item in value]
        return normalized[0] if len(normalized) == 1 else normalized
    if isinstance(value, dict):
        if "@value" in value:
            return value["@value"]
        if "@id" in value:
            return value["@id"]
    return value


def _bounded_page_size(limit: int) -> int:
    return min(max(limit, 1), 100)


def _bounded_offset(offset: int) -> int:
    return max(offset, 0)


def _is_identifier_method(method: OniSearchMethod) -> bool:
    return method in {OniSearchMethod.IDENTIFIER, OniSearchMethod.ID}


def _is_keyword_method(method: OniSearchMethod) -> bool:
    return method in {OniSearchMethod.KEYWORD, OniSearchMethod.STRING}


def build_search_body(
    *,
    method: OniSearchMethod,
    query: str,
    limit: int,
    offset: int,
) -> dict[str, Any]:
    """Build the OpenSearch request body used by the Oni items index."""
    search_query = query.strip()
    body: dict[str, Any] = {
        "size": _bounded_page_size(limit),
        "from": _bounded_offset(offset),
        "_source": DEFAULT_SOURCE_FIELDS,
    }

    if _is_identifier_method(method):
        identifier = extract_ldaca_identifier(search_query) or search_query
        body["query"] = {
            "bool": {
                "should": [
                    {"term": {"@id.keyword": identifier}},
                    {"term": {"_crateId.@value.keyword": identifier}},
                    {"term": {"_crateId.keyword": identifier}},
                ],
                "minimum_should_match": 1,
            }
        }
        return body

    if method is OniSearchMethod.COLLECTION:
        body["query"] = {
            "bool": {
                "filter": [
                    {"terms": {"@type.keyword": ["Dataset", "RepositoryCollection"]}},
                    {"terms": {"_isTopLevel.@value.keyword": ["true"]}},
                ]
            }
        }
        return body

    if method is OniSearchMethod.FILE_FORMAT:
        filters: list[dict[str, Any]] = [{"terms": {"@type.keyword": ["File"]}}]
        if search_query:
            filters.append({"terms": {"encodingFormat.@value.keyword": [search_query]}})
        body["query"] = {"bool": {"filter": filters}}
        return body

    if method is OniSearchMethod.ALL or not search_query:
        body["query"] = {"match_all": {}}
        return body

    if not _is_keyword_method(method):
        body["query"] = {"match_all": {}}
        return body

    body["query"] = {
        "multi_match": {
            "query": search_query,
            "fields": DEFAULT_SEARCH_FIELDS,
        }
    }
    return body


def _string_list(value: Any) -> list[str]:
    normalized = jsonld_value(value)
    if normalized is None:
        return []
    if isinstance(normalized, list):
        return [str(item) for item in normalized if item is not None]
    return [str(normalized)]


def _first_string(value: Any) -> str | None:
    values = _string_list(value)
    return values[0] if values else None


def _unique_strings(*values: Any) -> list[str]:
    seen: set[str] = set()
    results: list[str] = []
    for value in values:
        for item in _string_list(value):
            if item not in seen:
                seen.add(item)
                results.append(item)
    return results


def _summary_to_result(summary: dict[str, Any]) -> dict[str, Any]:
    crate_id = summary.get("crateId") or _first_string(summary.get("_crateId"))
    result_id = summary.get("@id") or crate_id or summary.get("id")
    title = (
        _first_string(summary.get("name"))
        or _first_string(summary.get("record", {}).get("name"))
        or str(result_id)
    )
    return {
        "id": str(result_id),
        "crate_id": str(crate_id) if crate_id else None,
        "title": title,
        "description": _first_string(summary.get("description"))
        or _first_string(summary.get("record", {}).get("description")),
        "types": _string_list(summary.get("recordType") or summary.get("@type")),
        "license": _first_string(summary.get("license")),
        "importable": summary.get("error") != "not_authorized",
        "access": summary.get("_access"),
        "collections": _unique_strings(
            summary.get("_memberOf"),
            summary.get("_mainCollection"),
            summary.get("_root"),
            summary.get("_crateId"),
            summary.get("crateId"),
        ),
        "file_formats": _unique_strings(summary.get("encodingFormat")),
        "stats": {},
    }


def _hit_to_result(hit: dict[str, Any]) -> dict[str, Any]:
    source = hit.get("_source", {})
    return _summary_to_result({"@id": source.get("@id"), **source})


class OniClient:
    """Small async client for the LDaCA Data Portal Oni API."""

    def __init__(
        self,
        *,
        base_url: str,
        token: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout = timeout

    @classmethod
    def from_settings(
        cls, app_settings: Any, *, token: str | None = None
    ) -> "OniClient":
        return cls(
            base_url=app_settings.ldaca_oni_api_base_url,
            token=token if token is not None else app_settings.ldaca_oni_api_token,
            timeout=app_settings.ldaca_oni_timeout,
        )

    def _headers(self) -> dict[str, str]:
        if not self.token:
            return {}
        return {"Authorization": f"Bearer {self.token}"}

    async def _request_json(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        async with httpx.AsyncClient(
            base_url=self.base_url,
            headers=self._headers(),
            timeout=self.timeout,
            follow_redirects=True,
        ) as client:
            response = await client.request(
                method,
                path,
                params=params,
                json=json_body,
            )
        response.raise_for_status()
        return response.json()

    async def get_object(self, identifier: str) -> dict[str, Any] | None:
        try:
            data = await self._request_json("GET", "/object", params={"id": identifier})
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                return None
            raise
        if data.get("message") == "Not Found":
            return None
        return data

    async def get_metadata(self, identifier: str) -> dict[str, Any]:
        return await self._request_json(
            "GET", "/object/meta", params={"id": identifier}
        )

    async def download_object_texts(
        self,
        identifier: str,
        paths: list[str],
        *,
        concurrency: int = 8,
    ) -> dict[str, str]:
        semaphore = asyncio.Semaphore(concurrency)

        async with httpx.AsyncClient(
            base_url=self.base_url,
            headers=self._headers(),
            timeout=self.timeout,
            follow_redirects=True,
        ) as client:

            async def fetch(path: str) -> tuple[str, str]:
                async with semaphore:
                    response = await client.get(
                        "/object/open",
                        params={"id": identifier, "path": path},
                    )
                    response.raise_for_status()
                    return path, response.text

            pairs = await asyncio.gather(*(fetch(path) for path in paths))
        return dict(pairs)

    async def search(
        self,
        *,
        method: OniSearchMethod,
        query: str,
        limit: int,
        offset: int,
    ) -> list[dict[str, Any]]:
        if _is_identifier_method(method):
            identifier = extract_ldaca_identifier(query)
            if identifier:
                summary = await self.get_object(identifier)
                if summary:
                    return [_summary_to_result(summary)]

        body = build_search_body(method=method, query=query, limit=limit, offset=offset)
        data = await self._request_json("POST", "/search/index/items", json_body=body)
        return [_hit_to_result(hit) for hit in data.get("hits", {}).get("hits", [])]

    async def featured_collections(
        self, collection_ids: list[str]
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for collection_id in collection_ids:
            summary = await self.get_object(collection_id)
            if summary:
                results.append(_summary_to_result(summary))
        return results
