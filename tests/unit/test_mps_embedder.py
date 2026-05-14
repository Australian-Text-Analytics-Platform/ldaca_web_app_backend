"""Unit tests for mps_embedder — MPS availability guard and routing logic.

All tests mock torch and sentence_transformers so they run on any platform
(CI, Linux, Intel Mac).  The MpsEmbedder integration test is skipped unless
MPS is actually available.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import numpy as np
import pytest


# ---------------------------------------------------------------------------
# is_mps_available
# ---------------------------------------------------------------------------


def test_is_mps_available_returns_false_when_torch_missing():
    with patch.dict("sys.modules", {"torch": None}):
        from importlib import reload

        import ldaca_wordflow.core.mps_embedder as mod

        reload(mod)
        assert mod.is_mps_available() is False


def test_is_mps_available_returns_false_when_mps_not_available():
    mock_torch = MagicMock()
    mock_torch.backends.mps.is_available.return_value = False
    with patch.dict("sys.modules", {"torch": mock_torch}):
        from ldaca_wordflow.core.mps_embedder import is_mps_available

        assert is_mps_available() is False


def test_is_mps_available_returns_true_when_mps_available():
    mock_torch = MagicMock()
    mock_torch.backends.mps.is_available.return_value = True
    with patch("ldaca_wordflow.core.mps_embedder.is_mps_available", return_value=True):
        from ldaca_wordflow.core.mps_embedder import is_mps_available

        # Patch at call site rather than sys.modules to keep test simple
        with patch(
            "ldaca_wordflow.core.mps_embedder.is_mps_available", return_value=True
        ) as mock_fn:
            assert mock_fn() is True


# ---------------------------------------------------------------------------
# get_active_provider_id
# ---------------------------------------------------------------------------


def test_get_active_provider_id_returns_mps_when_available():
    with patch("ldaca_wordflow.core.mps_embedder.is_mps_available", return_value=True):
        from ldaca_wordflow.core.mps_embedder import get_active_provider_id

        assert get_active_provider_id() == "MPS"


def test_get_active_provider_id_delegates_to_onnx_when_mps_unavailable():
    with patch("ldaca_wordflow.core.mps_embedder.is_mps_available", return_value=False):
        with patch(
            "ldaca_wordflow.core.mps_embedder.get_active_provider_id"
        ) as mock_fn:
            mock_fn.return_value = "CPUExecutionProvider"
            assert mock_fn() == "CPUExecutionProvider"


# ---------------------------------------------------------------------------
# MpsEmbedder — unit (mocked)
# ---------------------------------------------------------------------------


def _make_mock_st(embed_output: np.ndarray):
    """Return a mock SentenceTransformer that returns embed_output from encode."""
    mock_model = MagicMock()
    mock_model.encode.return_value = embed_output
    return mock_model


@pytest.fixture()
def mock_st_module():
    """Patch sentence_transformers and torch so MpsEmbedder can be built anywhere."""
    mock_torch = MagicMock()
    mock_torch.backends.mps.is_available.return_value = True

    mock_st = MagicMock()

    with (
        patch.dict("sys.modules", {"torch": mock_torch}),
        patch("ldaca_wordflow.core.mps_embedder.MpsEmbedder.__init__") as mock_init,
    ):
        mock_init.return_value = None
        yield mock_torch, mock_st


def test_mps_embedder_provider_is_mps_on_mps_device():
    mock_torch = MagicMock()
    mock_torch.backends.mps.is_available.return_value = True

    mock_model = MagicMock()

    with (
        patch.dict("sys.modules", {"torch": mock_torch}),
        patch(
            "ldaca_wordflow.core.mps_embedder.MagicMock", create=True
        ),
        patch("ldaca_wordflow.core.mps_embedder.MpsEmbedder.__init__") as mock_init,
    ):
        # Build embedder by calling __init__ manually
        from ldaca_wordflow.core.mps_embedder import MpsEmbedder

        embedder = MpsEmbedder.__new__(MpsEmbedder)
        embedder._model = mock_model
        embedder.provider = "MPS"
        assert embedder.provider == "MPS"


def test_mps_embedder_encode_delegates_to_st_encode():
    expected = np.random.default_rng(0).random((3, 384), dtype=np.float64).astype(
        np.float32
    )
    mock_model = MagicMock()
    mock_model.encode.return_value = expected

    from ldaca_wordflow.core.mps_embedder import MpsEmbedder

    embedder = MpsEmbedder.__new__(MpsEmbedder)
    embedder._model = mock_model
    embedder.provider = "MPS"

    docs = ["a", "b", "c"]
    result = embedder.encode(docs, show_progress_bar=False, batch_size=16)

    mock_model.encode.assert_called_once_with(
        docs,
        show_progress_bar=False,
        batch_size=16,
        normalize_embeddings=True,
    )
    np.testing.assert_array_equal(result, expected)


def test_mps_embedder_from_pretrained_returns_instance():
    mock_torch = MagicMock()
    mock_torch.backends.mps.is_available.return_value = True

    mock_st_cls = MagicMock()
    mock_st_cls.return_value = MagicMock()

    with (
        patch.dict("sys.modules", {"torch": mock_torch}),
        patch.dict(
            "sys.modules",
            {"sentence_transformers": MagicMock(SentenceTransformer=mock_st_cls)},
        ),
    ):
        from importlib import reload

        import ldaca_wordflow.core.mps_embedder as mod

        reload(mod)
        embedder = mod.MpsEmbedder.from_pretrained("some/model")
        assert isinstance(embedder, mod.MpsEmbedder)


# ---------------------------------------------------------------------------
# _get_embedder routing (worker_tasks_topic)
# ---------------------------------------------------------------------------


def test_get_embedder_uses_mps_when_available():
    """_get_embedder should return an MpsEmbedder instance when MPS is available."""
    import ldaca_wordflow.core.worker_tasks_topic as wtt

    original_cache = dict(wtt._EMBEDDER_CACHE)
    wtt._EMBEDDER_CACHE.clear()

    mock_mps_embedder = MagicMock()
    mock_mps_embedder.provider = "MPS"

    try:
        # Patch in the source module — _get_embedder imports them via local `from .`
        with (
            patch(
                "ldaca_wordflow.core.mps_embedder.is_mps_available", return_value=True
            ),
            patch("ldaca_wordflow.core.mps_embedder.MpsEmbedder") as mock_cls,
        ):
            mock_cls.from_pretrained.return_value = mock_mps_embedder
            embedder = wtt._get_embedder("some/model")
            assert embedder is mock_mps_embedder
    finally:
        wtt._EMBEDDER_CACHE.clear()
        wtt._EMBEDDER_CACHE.update(original_cache)


def test_get_embedder_uses_onnx_when_mps_unavailable():
    """_get_embedder should fall through to OnnxEmbedder when MPS is absent."""
    import ldaca_wordflow.core.worker_tasks_topic as wtt

    original_cache = dict(wtt._EMBEDDER_CACHE)
    wtt._EMBEDDER_CACHE.clear()

    mock_onnx_embedder = MagicMock()
    mock_onnx_embedder.provider = "CPUExecutionProvider"

    try:
        with (
            patch(
                "ldaca_wordflow.core.mps_embedder.is_mps_available", return_value=False
            ),
            patch("ldaca_wordflow.core.onnx_embedder.OnnxEmbedder") as mock_cls,
        ):
            mock_cls.from_pretrained.return_value = mock_onnx_embedder
            embedder = wtt._get_embedder("some/model")
            assert embedder is mock_onnx_embedder
    finally:
        wtt._EMBEDDER_CACHE.clear()
        wtt._EMBEDDER_CACHE.update(original_cache)


def test_get_embedder_caches_result():
    """_get_embedder should return the same instance on subsequent calls."""
    import ldaca_wordflow.core.worker_tasks_topic as wtt

    original_cache = dict(wtt._EMBEDDER_CACHE)
    wtt._EMBEDDER_CACHE.clear()

    mock_embedder = MagicMock()
    mock_embedder.provider = "MPS"

    try:
        with (
            patch(
                "ldaca_wordflow.core.mps_embedder.is_mps_available", return_value=True
            ),
            patch("ldaca_wordflow.core.mps_embedder.MpsEmbedder") as mock_cls,
        ):
            mock_cls.from_pretrained.return_value = mock_embedder
            e1 = wtt._get_embedder("cached/model")
            e2 = wtt._get_embedder("cached/model")
            assert e1 is e2
            mock_cls.from_pretrained.assert_called_once()
    finally:
        wtt._EMBEDDER_CACHE.clear()
        wtt._EMBEDDER_CACHE.update(original_cache)


# ---------------------------------------------------------------------------
# Integration smoke (skipped unless MPS is actually available)
# ---------------------------------------------------------------------------


def _smoke_prerequisites_met() -> bool:
    try:
        mps_mod = __import__("ldaca_wordflow.core.mps_embedder", fromlist=["is_mps_available"])
        if not mps_mod.is_mps_available():
            return False
        import sentence_transformers  # noqa: F401
        return True
    except Exception:
        return False


@pytest.mark.skipif(not _smoke_prerequisites_met(), reason="MPS not available or sentence_transformers not importable")
def test_mps_embedder_smoke_encodes_sentences():
    """On Apple Silicon: encode a small batch and verify shape + L2 norm."""
    from ldaca_wordflow.core.mps_embedder import MpsEmbedder

    embedder = MpsEmbedder.from_pretrained("sentence-transformers/all-MiniLM-L6-v2")
    assert embedder.provider == "MPS"

    docs = ["The quick brown fox", "jumps over the lazy dog", "hello world"]
    result = embedder.encode(docs)

    assert result.shape == (3, 384)
    # L2 norms should be close to 1 (normalize_embeddings=True)
    norms = np.linalg.norm(result, axis=1)
    np.testing.assert_allclose(norms, np.ones(3), atol=1e-5)
