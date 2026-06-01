"""Tests for the background docs mirror (core/docs_sync.py)."""

from __future__ import annotations

import json

import httpx
import pytest
from ldaca_wordflow.core import docs_sync
from ldaca_wordflow.settings import settings

BASE = "https://docs.example.test/v0.5"

REGISTRY = {
    "meta": {"version": "9.9.9"},
    "tutorial": {
        "concordance.overview": {"file": "tutorials/concordance.md", "anchor": "c"},
    },
    "info": {},
    "reference": {},
}

# Remote "site": registry + markdown (with image refs) + image assets.
REMOTE_FILES: dict[str, bytes] = {
    "registry.json": json.dumps(REGISTRY).encode(),
    "tutorials/index.md": b"# Index\n\n![pic](tutorials/assets/a.png)\n",
    "tutorials/concordance.md": b'# Conc\n\n<img src="tutorials/assets/b.png">\n',
    "information/index.md": b"# Info\n",
    "references/index.md": b"# Ref\n",
    "tutorials/assets/a.png": b"PNG-A",
    "tutorials/assets/b.png": b"PNG-B",
}


class _FakeResp:
    def __init__(self, content: bytes, status: int = 200):
        self.content = content
        self.status_code = status

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return json.loads(self.content)


class _FakeClient:
    """Stand-in for httpx.Client that serves REMOTE_FILES and counts GETs."""

    def __init__(self, files: dict[str, bytes], calls: list[str], **_kwargs):
        self._files = files
        self._calls = calls

    def __enter__(self):
        return self

    def __exit__(self, *_a):
        return False

    def get(self, url: str) -> _FakeResp:
        self._calls.append(url)
        prefix = BASE + "/"
        rel = url[len(prefix):] if url.startswith(prefix) else url
        if rel in self._files:
            return _FakeResp(self._files[rel])
        return _FakeResp(b"", status=404)


@pytest.fixture
def docs_cache(tmp_path, monkeypatch):
    """Point the docs cache at a tmpdir and the remote at the fake site."""
    monkeypatch.setattr(settings, "docs_cache_dir", str(tmp_path / "docscache"))
    monkeypatch.setattr(settings, "docs_remote_base_url", BASE)
    calls: list[str] = []
    monkeypatch.setattr(
        httpx, "Client", lambda **kw: _FakeClient(REMOTE_FILES, calls, **kw)
    )
    return tmp_path / "docscache", calls


def test_sync_mirrors_markdown_and_assets(docs_cache):
    cache_dir, _calls = docs_cache
    docs_sync.sync_docs()

    content = cache_dir / "content"
    # Markdown (registry entry + the three index files) + parsed image assets.
    for rel in [
        "registry.json",
        "tutorials/index.md",
        "tutorials/concordance.md",
        "information/index.md",
        "references/index.md",
        "tutorials/assets/a.png",
        "tutorials/assets/b.png",
    ]:
        assert (content / rel).is_file(), f"missing mirrored file: {rel}"
    assert (content / "tutorials/assets/a.png").read_bytes() == b"PNG-A"
    assert (cache_dir / "VERSION").read_text() == "9.9.9"


def test_sync_is_skipped_when_version_unchanged(docs_cache):
    cache_dir, calls = docs_cache
    docs_sync.sync_docs()
    first_round = len(calls)
    assert first_round > 1  # registry + files

    # Second run at the same version only re-fetches registry.json, then bails.
    docs_sync.sync_docs()
    assert calls[first_round:] == [f"{BASE}/registry.json"]


def test_sync_resyncs_when_version_changes(docs_cache, monkeypatch):
    cache_dir, _calls = docs_cache
    docs_sync.sync_docs()
    assert (cache_dir / "VERSION").read_text() == "9.9.9"

    bumped = dict(REGISTRY, meta={"version": "9.9.10"})
    monkeypatch.setitem(REMOTE_FILES, "registry.json", json.dumps(bumped).encode())
    try:
        docs_sync.sync_docs()
        assert (cache_dir / "VERSION").read_text() == "9.9.10"
    finally:
        REMOTE_FILES["registry.json"] = json.dumps(REGISTRY).encode()


def test_resolve_doc_file_prefers_cache(docs_cache):
    cache_dir, _calls = docs_cache
    docs_sync.sync_docs()

    resolved = docs_sync.resolve_doc_file("tutorials/index.md")
    assert resolved is not None
    assert resolved == (cache_dir / "content" / "tutorials/index.md")


def test_resolve_doc_file_rejects_traversal(docs_cache):
    docs_sync.sync_docs()
    assert docs_sync.resolve_doc_file("../../etc/passwd") is None
    assert docs_sync.resolve_doc_file("") is None


def test_warning_section_mirrored_and_absent_warning_index_is_quiet(
    docs_cache, monkeypatch
):
    cache_dir, _calls = docs_cache
    # warnings/index.md is intentionally NOT in REMOTE_FILES -> the optional
    # placeholder fetch 404s and is skipped without crashing. A populated
    # `warning` registry section, however, must be mirrored by the section loop.
    reg = dict(
        REGISTRY,
        meta={"version": "8.8.8"},
        warning={"x.warn": {"file": "warnings/x.md", "anchor": "w"}},
    )
    monkeypatch.setitem(REMOTE_FILES, "registry.json", json.dumps(reg).encode())
    monkeypatch.setitem(REMOTE_FILES, "warnings/x.md", b"# Warn\n")

    docs_sync.sync_docs()

    content = cache_dir / "content"
    assert (content / "warnings/x.md").is_file()  # warning section enumerated
    assert not (content / "warnings/index.md").exists()  # absent placeholder skipped


def test_sync_noop_when_disabled(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "docs_cache_dir", str(tmp_path / "docscache"))
    monkeypatch.setattr(settings, "docs_remote_base_url", "")
    called = {"n": 0}
    monkeypatch.setattr(
        httpx, "Client", lambda **kw: called.__setitem__("n", called["n"] + 1)
    )
    docs_sync.sync_docs()
    assert called["n"] == 0
    assert not (tmp_path / "docscache" / "content").exists()
