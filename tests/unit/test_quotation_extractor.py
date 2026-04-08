from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest
from ldaca_web_app.core import quotation_extractor as qe


def test_load_spacy_model_downloads_to_cache_when_package_missing(
    monkeypatch, tmp_path
):
    cache_root = tmp_path / "spacy-cache"
    downloaded_model_dir = cache_root / "en_core_web_md"
    load_calls: list[object] = []

    class FakeSpacyModule:
        def load(self, target):
            load_calls.append(target)
            if target == qe._SPACY_MODEL:
                raise OSError(
                    "[E050] Can't find model 'en_core_web_md'. It doesn't seem "
                    "to be a Python package or a valid path to a data directory."
                )
            return SimpleNamespace(name="downloaded-model", target=target)

    def fake_download() -> Path:
        downloaded_model_dir.mkdir(parents=True, exist_ok=True)
        (downloaded_model_dir / "config.cfg").write_text(
            '[nlp]\nlang = "en"\n',
            encoding="utf-8",
        )
        return downloaded_model_dir

    monkeypatch.setattr(qe, "_SPACY_MODEL_CACHE_ROOT", cache_root)
    monkeypatch.setattr(qe, "_download_spacy_model_to_cache", fake_download)
    monkeypatch.setitem(__import__("sys").modules, "spacy", FakeSpacyModule())

    model = qe._load_spacy_model()

    assert model.name == "downloaded-model"
    assert load_calls == [qe._SPACY_MODEL, downloaded_model_dir]


def test_load_spacy_model_prefers_cached_directory(monkeypatch, tmp_path):
    cache_root = tmp_path / "spacy-cache"
    cached_model_dir = cache_root / "en_core_web_md"
    cached_model_dir.mkdir(parents=True, exist_ok=True)
    (cached_model_dir / "config.cfg").write_text(
        '[nlp]\nlang = "en"\n',
        encoding="utf-8",
    )
    load_calls: list[object] = []

    class FakeSpacyModule:
        def load(self, target):
            load_calls.append(target)
            return SimpleNamespace(name="cached-model", target=target)

    monkeypatch.setattr(qe, "_SPACY_MODEL_CACHE_ROOT", cache_root)
    monkeypatch.setitem(__import__("sys").modules, "spacy", FakeSpacyModule())

    model = qe._load_spacy_model()

    assert model.name == "cached-model"
    assert load_calls == [cached_model_dir]


def test_download_spacy_model_handles_nested_archive_filename(monkeypatch, tmp_path):
    cache_root = tmp_path / "spacy-cache"
    extracted_model_dir = tmp_path / "extracted-model"
    extracted_model_dir.mkdir(parents=True, exist_ok=True)
    (extracted_model_dir / "config.cfg").write_text(
        '[nlp]\nlang = "en"\n',
        encoding="utf-8",
    )
    (extracted_model_dir / "meta.json").write_text("{}", encoding="utf-8")
    (extracted_model_dir / "vocab").mkdir(exist_ok=True)

    spacy_module = ModuleType("spacy")
    about_module = ModuleType("spacy.about")
    cli_module = ModuleType("spacy.cli")
    download_module = ModuleType("spacy.cli.download")
    setattr(about_module, "__download_url__", "https://example.invalid/download")

    def fake_get_compatibility():
        return {qe._SPACY_MODEL: ["3.8.0"]}

    def fake_get_version(_model: str, _compatibility: dict):
        return "3.8.0"

    def fake_get_model_filename(_model: str, _version: str, sdist: bool = False):
        assert sdist is True
        return "en_core_web_md-3.8.0/en_core_web_md-3.8.0.tar.gz"

    setattr(download_module, "get_compatibility", fake_get_compatibility)
    setattr(download_module, "get_version", fake_get_version)
    setattr(download_module, "get_model_filename", fake_get_model_filename)
    setattr(cli_module, "download", download_module)
    setattr(spacy_module, "about", about_module)
    setattr(spacy_module, "cli", cli_module)

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def raise_for_status(self):
            return None

        def iter_bytes(self):
            yield b"fake-archive-bytes"

    class FakeTarFile:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def getmembers(self):
            return []

        def extractall(self, destination):
            return None

    monkeypatch.setattr(qe, "_SPACY_MODEL_CACHE_ROOT", cache_root)
    monkeypatch.setattr(qe, "_find_spacy_data_dir", lambda root: extracted_model_dir)
    monkeypatch.setattr(qe.httpx, "stream", lambda *args, **kwargs: FakeResponse())
    monkeypatch.setattr(qe.tarfile, "open", lambda *args, **kwargs: FakeTarFile())
    monkeypatch.setitem(__import__("sys").modules, "spacy", spacy_module)
    monkeypatch.setitem(__import__("sys").modules, "spacy.about", about_module)
    monkeypatch.setitem(__import__("sys").modules, "spacy.cli", cli_module)
    monkeypatch.setitem(
        __import__("sys").modules,
        "spacy.cli.download",
        download_module,
    )

    cache_dir = qe._download_spacy_model_to_cache()

    assert cache_dir == cache_root / qe._SPACY_MODEL
    assert (cache_dir / "config.cfg").exists()
