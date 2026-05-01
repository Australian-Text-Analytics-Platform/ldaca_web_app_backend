"""Tests for packaged backend resources."""

from __future__ import annotations

from importlib import resources
from pathlib import Path

import pytest
from ldaca_web_app.core.utils import get_user_data_folder, import_sample_data_for_user
from ldaca_web_app.settings import settings


def _resource_path(relative: str) -> Path:
    """Resolve a resource path to a filesystem path."""
    target = resources.files("ldaca_web_app.resources").joinpath(relative)
    with resources.as_file(target) as resolved:
        return resolved


def test_sample_data_resources_exist():
    sample_root = _resource_path("sample_data")
    assert sample_root.exists() and sample_root.is_dir()
    # Require at least one regular (non-hidden) file somewhere under the
    # packaged sample_data tree so that the shipped resources are usable.
    files = [
        p for p in sample_root.rglob("*") if p.is_file() and not p.name.startswith(".")
    ]
    assert files, f"No packaged sample data files found under {sample_root}"


def test_stopwords_resources():
    stopwords_dir = resources.files("ldaca_web_app.resources")
    languages = ["en", "es", "fr", "de"]
    for lang in languages:
        path = stopwords_dir.joinpath(f"stopwords_{lang}.txt")
        with resources.as_file(path) as resolved:
            assert resolved.is_file()
            content = resolved.read_text(encoding="utf-8")
            assert content.strip(), f"Stopwords file {path} should not be empty"


def test_import_sample_data_uses_packaged_resources(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "data_root", tmp_path)
    monkeypatch.setattr(settings, "user_data_folder", "users")
    monkeypatch.setattr(settings, "sample_data", None)

    # Enumerate the packaged sample data tree so we can verify the copy
    # mirrors it without hard-coding any particular filenames.
    source_root = _resource_path("sample_data")
    source_files = {
        p.relative_to(source_root)
        for p in source_root.rglob("*")
        if p.is_file() and not p.name.startswith(".")
    }
    assert source_files, "Packaged sample_data should contain at least one file"

    summary = import_sample_data_for_user("test")

    user_data_dir = get_user_data_folder("test")
    target_sample = user_data_dir / "sample_data"
    assert target_sample.exists()
    copied_files = {
        p.relative_to(target_sample)
        for p in target_sample.rglob("*")
        if p.is_file() and not p.name.startswith(".")
    }
    assert source_files.issubset(copied_files)
    assert summary["file_count"] > 0
    assert summary["bytes_copied"] > 0
    assert summary["bytes_copied"] > 0


def test_import_sample_data_raises_when_source_missing(tmp_path, monkeypatch):
    """If the configured sample_data override does not exist, import raises."""
    monkeypatch.setattr(settings, "data_root", tmp_path)
    monkeypatch.setattr(settings, "user_data_folder", "users")
    monkeypatch.setattr(settings, "sample_data", tmp_path / "does_not_exist")

    with pytest.raises(FileNotFoundError):
        import_sample_data_for_user("test")
        import_sample_data_for_user("test")
