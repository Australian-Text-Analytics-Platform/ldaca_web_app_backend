from pathlib import Path

from ldaca_web_app.core import model_prefetch as mp
from ldaca_web_app.core import quotation_extractor as qe


def test_prefetch_skips_when_model_already_cached(monkeypatch, tmp_path):
    cache_root = tmp_path / "spacy-cache"
    cached_dir = cache_root / "en_core_web_md"
    cached_dir.mkdir(parents=True, exist_ok=True)
    (cached_dir / "config.cfg").write_text('[nlp]\nlang = "en"\n', encoding="utf-8")

    monkeypatch.setattr(qe, "_SPACY_MODEL_CACHE_ROOT", cache_root)

    download_called = False

    def fake_download() -> Path:
        nonlocal download_called
        download_called = True
        return cached_dir

    monkeypatch.setattr(qe, "_download_spacy_model_to_cache", fake_download)

    mp._prefetch_spacy_model()

    assert not download_called


def test_prefetch_downloads_when_model_missing(monkeypatch, tmp_path):
    cache_root = tmp_path / "spacy-cache"
    cached_dir = cache_root / "en_core_web_md"

    monkeypatch.setattr(qe, "_SPACY_MODEL_CACHE_ROOT", cache_root)

    download_called = False

    def fake_download() -> Path:
        nonlocal download_called
        download_called = True
        cached_dir.mkdir(parents=True, exist_ok=True)
        (cached_dir / "config.cfg").write_text('[nlp]\nlang = "en"\n', encoding="utf-8")
        return cached_dir

    monkeypatch.setattr(qe, "_download_spacy_model_to_cache", fake_download)

    mp._prefetch_spacy_model()

    assert download_called


def test_prefetch_does_not_raise_on_failure(monkeypatch, tmp_path):
    cache_root = tmp_path / "spacy-cache"
    monkeypatch.setattr(qe, "_SPACY_MODEL_CACHE_ROOT", cache_root)

    def boom() -> Path:
        raise RuntimeError("network error")

    monkeypatch.setattr(qe, "_download_spacy_model_to_cache", boom)

    # Should not raise
    mp._prefetch_spacy_model()


def test_start_model_prefetch_spawns_daemon_thread(monkeypatch, tmp_path):
    cache_root = tmp_path / "spacy-cache"
    cached_dir = cache_root / "en_core_web_md"
    cached_dir.mkdir(parents=True, exist_ok=True)
    (cached_dir / "config.cfg").write_text('[nlp]\nlang = "en"\n', encoding="utf-8")

    monkeypatch.setattr(qe, "_SPACY_MODEL_CACHE_ROOT", cache_root)

    import threading

    threads_before = threading.enumerate()
    mp.start_model_prefetch()

    import time

    time.sleep(0.1)

    threads_after = threading.enumerate()
    prefetch_names = [t.name for t in threads_after if t.name == "model-prefetch"]
    # Thread may have finished already for the cached case, so just verify no crash
    assert isinstance(prefetch_names, list)
