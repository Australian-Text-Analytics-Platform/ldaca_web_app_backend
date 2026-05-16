"""Integration tests for /users/me/snapshots endpoints.

Covers the storage clerk responsibilities defined in
``docs/snapshot-view/plan.md`` §2.5 — list, upload, download,
description, single delete, batch delete (with and without the
``incompatible_with`` filter).
"""

from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path
from typing import Any

import pytest


def _build_bundle(manifest: dict[str, Any]) -> bytes:
    """Build an in-memory ``.ldaca-snapshot`` zip with the given
    manifest plus a trivial result payload."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("manifest.json", json.dumps(manifest))
        zf.writestr("tables/result.parquet", b"\x00fake-parquet-bytes")
    return buf.getvalue()


def _manifest(
    *,
    tool: str = "concordance",
    tool_version: str = "v0.4.4",
    title: str = "test snapshot",
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "mode": "demo",
        "tool": tool,
        "tool_version": tool_version,
        "captured_at": "2026-05-16T08:00:00Z",
        "title": title,
        "source": {
            "workspace_id": "ws-1",
            "workspace_name": "Tutorial workspace",
            "node_ids": ["n1"],
            "node_labels": ["Node 1"],
            "total_source_rows": 100,
        },
        "capabilities": {
            "canPaginate": True,
            "canSortAndFilterResult": True,
            "canExport": True,
            "canFilterSourceRows": False,
            "canCrossJump": False,
        },
        "preview": {
            "tool": tool,
            "searchTerm": "love",
            "totalHits": 42,
            "materialised": True,
            "displayColumns": ["doc_id", "matched_text"],
        },
        "payloads": [{"kind": "result", "path": "tables/result.parquet"}],
        "node_colors": {"n1": "#aabbcc"},
    }


async def _upload(client, filename: str, manifest: dict[str, Any]) -> dict[str, Any]:
    bundle_bytes = _build_bundle(manifest)
    response = await client.post(
        "/api/users/me/snapshots",
        files={"file": (filename, bundle_bytes, "application/zip")},
        data={"filename": filename},
    )
    assert response.status_code == 200, response.text
    return response.json()


@pytest.mark.asyncio
async def test_list_empty(authenticated_client) -> None:
    response = await authenticated_client.get("/api/users/me/snapshots")
    assert response.status_code == 200
    assert response.json() == {"items": []}


@pytest.mark.asyncio
async def test_upload_and_list_round_trip(authenticated_client) -> None:
    body = await _upload(
        authenticated_client, "concordance-demo-1.ldaca-snapshot", _manifest()
    )
    assert body["filename"] == "concordance-demo-1.ldaca-snapshot"
    assert body["manifest"]["tool"] == "concordance"

    response = await authenticated_client.get("/api/users/me/snapshots")
    items = response.json()["items"]
    assert len(items) == 1
    assert items[0]["filename"] == "concordance-demo-1.ldaca-snapshot"
    assert items[0]["manifest"]["title"] == "test snapshot"


@pytest.mark.asyncio
async def test_upload_writes_sidecars(authenticated_client) -> None:
    """The sidecar manifest + .md exist on disk after upload — that's
    what makes the list endpoint cheap (no zip decode)."""
    await _upload(
        authenticated_client, "concordance-foo.ldaca-snapshot", _manifest()
    )
    # Description endpoint serves the .md sidecar; fetching it asserts
    # the sidecar exists (or was lazily regenerated).
    response = await authenticated_client.get(
        "/api/users/me/snapshots/concordance-foo.ldaca-snapshot/description"
    )
    assert response.status_code == 200
    assert "test snapshot" in response.text
    assert response.headers["content-type"].startswith("text/markdown")


@pytest.mark.asyncio
async def test_upload_rejects_invalid_filename_suffix(authenticated_client) -> None:
    response = await authenticated_client.post(
        "/api/users/me/snapshots",
        files={"file": ("x.txt", b"hi", "text/plain")},
        data={"filename": "x.txt"},
    )
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_upload_rejects_path_traversal(authenticated_client) -> None:
    response = await authenticated_client.post(
        "/api/users/me/snapshots",
        files={
            "file": (
                "x.ldaca-snapshot",
                _build_bundle(_manifest()),
                "application/zip",
            )
        },
        data={"filename": "../etc/passwd.ldaca-snapshot"},
    )
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_upload_rejects_collision(authenticated_client) -> None:
    name = "concordance-dup.ldaca-snapshot"
    await _upload(authenticated_client, name, _manifest())
    bundle_bytes = _build_bundle(_manifest(title="other"))
    response = await authenticated_client.post(
        "/api/users/me/snapshots",
        files={"file": (name, bundle_bytes, "application/zip")},
        data={"filename": name},
    )
    assert response.status_code == 409


@pytest.mark.asyncio
async def test_upload_rejects_bundle_without_manifest(authenticated_client) -> None:
    # A zip with no manifest.json — must fail and leave nothing behind.
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w") as zf:
        zf.writestr("other.txt", b"hi")
    name = "concordance-bad.ldaca-snapshot"
    response = await authenticated_client.post(
        "/api/users/me/snapshots",
        files={"file": (name, buf.getvalue(), "application/zip")},
        data={"filename": name},
    )
    assert response.status_code == 400

    # The list endpoint should not show the broken upload.
    listing = await authenticated_client.get("/api/users/me/snapshots")
    assert all(it["filename"] != name for it in listing.json()["items"])


@pytest.mark.asyncio
async def test_download_returns_bundle_bytes(authenticated_client) -> None:
    bundle = _build_bundle(_manifest())
    name = "concordance-dl.ldaca-snapshot"
    await authenticated_client.post(
        "/api/users/me/snapshots",
        files={"file": (name, bundle, "application/zip")},
        data={"filename": name},
    )
    response = await authenticated_client.get(f"/api/users/me/snapshots/{name}")
    assert response.status_code == 200
    assert response.content == bundle
    assert response.headers["content-type"] == "application/zip"


@pytest.mark.asyncio
async def test_download_missing_returns_404(authenticated_client) -> None:
    response = await authenticated_client.get(
        "/api/users/me/snapshots/concordance-nope.ldaca-snapshot"
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_delete_one_removes_all_sidecars(
    authenticated_client, settings_override
) -> None:
    name = "concordance-del.ldaca-snapshot"
    await _upload(authenticated_client, name, _manifest())

    # Sanity-check the sidecars are on disk before delete.
    snapshots_dir: Path = (
        settings_override.get_data_root()
        / settings_override.user_data_folder
        / "user_root"
        / "user_cache"
        / "snapshots"
    )
    basename = name.removesuffix(".ldaca-snapshot")
    bundle = snapshots_dir / name
    manifest_sidecar = snapshots_dir / f"{basename}.manifest.json"
    md_sidecar = snapshots_dir / f"{basename}.md"
    assert bundle.exists()
    assert manifest_sidecar.exists()
    assert md_sidecar.exists()

    response = await authenticated_client.delete(f"/api/users/me/snapshots/{name}")
    assert response.status_code == 200
    assert response.json() == {"deleted": [name]}

    assert not bundle.exists()
    assert not manifest_sidecar.exists()
    assert not md_sidecar.exists()


@pytest.mark.asyncio
async def test_list_filters_by_tool(authenticated_client) -> None:
    await _upload(
        authenticated_client, "concordance-a.ldaca-snapshot", _manifest(tool="concordance")
    )
    await _upload(
        authenticated_client, "quotation-a.ldaca-snapshot", _manifest(tool="quotation")
    )

    conc = await authenticated_client.get("/api/users/me/snapshots?tool=concordance")
    assert len(conc.json()["items"]) == 1
    assert conc.json()["items"][0]["filename"] == "concordance-a.ldaca-snapshot"

    quot = await authenticated_client.get("/api/users/me/snapshots?tool=quotation")
    assert len(quot.json()["items"]) == 1


@pytest.mark.asyncio
async def test_lazy_sidecar_extraction(authenticated_client, settings_override) -> None:
    """Drop a bundle on disk WITHOUT sidecars and confirm the list
    endpoint extracts the manifest sidecar lazily on first list.
    This guards against hand-imported bundles."""
    # Compute the snapshots dir and write a bundle directly.
    snapshots_dir: Path = (
        settings_override.get_data_root()
        / settings_override.user_data_folder
        / "user_root"
        / "user_cache"
        / "snapshots"
    )
    snapshots_dir.mkdir(parents=True, exist_ok=True)
    name = "concordance-hand-dropped.ldaca-snapshot"
    bundle_path = snapshots_dir / name
    bundle_path.write_bytes(_build_bundle(_manifest(title="hand dropped")))

    response = await authenticated_client.get("/api/users/me/snapshots")
    items = response.json()["items"]
    assert any(it["filename"] == name for it in items)

    # The sidecar should have been written on first list.
    basename = name.removesuffix(".ldaca-snapshot")
    assert (snapshots_dir / f"{basename}.manifest.json").exists()


@pytest.mark.asyncio
async def test_batch_delete_all_for_tool(authenticated_client) -> None:
    await _upload(
        authenticated_client,
        "concordance-1.ldaca-snapshot",
        _manifest(tool="concordance"),
    )
    await _upload(
        authenticated_client,
        "concordance-2.ldaca-snapshot",
        _manifest(tool="concordance"),
    )
    await _upload(
        authenticated_client,
        "quotation-1.ldaca-snapshot",
        _manifest(tool="quotation"),
    )

    response = await authenticated_client.delete(
        "/api/users/me/snapshots?tool=concordance"
    )
    assert response.status_code == 200
    deleted = set(response.json()["deleted"])
    assert deleted == {"concordance-1.ldaca-snapshot", "concordance-2.ldaca-snapshot"}

    # Quotation snapshot untouched.
    listing = await authenticated_client.get("/api/users/me/snapshots")
    assert [it["filename"] for it in listing.json()["items"]] == [
        "quotation-1.ldaca-snapshot"
    ]


@pytest.mark.asyncio
async def test_batch_delete_incompatible_only(authenticated_client) -> None:
    """Mixed-version list: only different-MAJOR.MINOR snapshots are
    deleted when ``incompatible_with`` is supplied."""
    await _upload(
        authenticated_client,
        "concordance-current.ldaca-snapshot",
        _manifest(tool_version="v0.4.2"),
    )
    await _upload(
        authenticated_client,
        "concordance-old.ldaca-snapshot",
        _manifest(tool_version="v0.3.5"),
    )
    await _upload(
        authenticated_client,
        "concordance-newer.ldaca-snapshot",
        _manifest(tool_version="v0.5.0"),
    )

    response = await authenticated_client.delete(
        "/api/users/me/snapshots",
        params={"tool": "concordance", "incompatible_with": "v0.4.4"},
    )
    assert response.status_code == 200
    deleted = set(response.json()["deleted"])
    assert deleted == {
        "concordance-old.ldaca-snapshot",
        "concordance-newer.ldaca-snapshot",
    }

    listing = await authenticated_client.get("/api/users/me/snapshots")
    remaining = [it["filename"] for it in listing.json()["items"]]
    assert remaining == ["concordance-current.ldaca-snapshot"]


@pytest.mark.asyncio
async def test_description_works_for_missing_md(
    authenticated_client, settings_override
) -> None:
    """If the .md sidecar is accidentally deleted, the description
    endpoint regenerates it from the manifest instead of 404'ing."""
    name = "concordance-no-md.ldaca-snapshot"
    await _upload(authenticated_client, name, _manifest(title="resurrect me"))

    snapshots_dir: Path = (
        settings_override.get_data_root()
        / settings_override.user_data_folder
        / "user_root"
        / "user_cache"
        / "snapshots"
    )
    basename = name.removesuffix(".ldaca-snapshot")
    md_path = snapshots_dir / f"{basename}.md"
    md_path.unlink()
    assert not md_path.exists()

    response = await authenticated_client.get(
        f"/api/users/me/snapshots/{name}/description"
    )
    assert response.status_code == 200
    assert "resurrect me" in response.text
    assert md_path.exists()
