from pathlib import Path
from types import SimpleNamespace

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
