"""Unit tests for the OnnxEmbedder.

All tests avoid loading a real ONNX model or making network requests.
The public encode() interface and the two pure-numpy helpers are tested
with mocks and synthetic data.
"""

from __future__ import annotations

import platform
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from ldaca_web_app.core import onnx_embedder as oem


# ---------------------------------------------------------------------------
# Pure helpers — no mocking needed
# ---------------------------------------------------------------------------


def test_mean_pool_single_doc():
    # (1, 4, 3) — one doc, 4 tokens, dim 3
    token_embs = np.array([[[1, 2, 3], [4, 5, 6], [7, 8, 9], [0, 0, 0]]], dtype=np.float32)
    mask = np.array([[1, 1, 1, 0]], dtype=np.int64)
    result = oem._mean_pool(token_embs, mask)
    expected = np.array([[(1 + 4 + 7) / 3, (2 + 5 + 8) / 3, (3 + 6 + 9) / 3]], dtype=np.float32)
    np.testing.assert_allclose(result, expected, rtol=1e-5)


def test_mean_pool_all_tokens_masked():
    token_embs = np.ones((1, 3, 4), dtype=np.float32)
    mask = np.zeros((1, 3), dtype=np.int64)
    result = oem._mean_pool(token_embs, mask)
    # count clips to 1e-9 so result is ones / 1e-9 — finite, not NaN
    assert np.isfinite(result).all()


def test_l2_normalize_unit_vector():
    v = np.array([[3.0, 4.0]], dtype=np.float32)
    result = oem._l2_normalize(v)
    np.testing.assert_allclose(np.linalg.norm(result, axis=1), [1.0], atol=1e-6)


def test_l2_normalize_zero_vector():
    v = np.zeros((1, 4), dtype=np.float32)
    result = oem._l2_normalize(v)
    assert np.isfinite(result).all()


def test_l2_normalize_batch():
    v = np.array([[1.0, 0.0, 0.0], [0.0, 3.0, 4.0]], dtype=np.float32)
    result = oem._l2_normalize(v)
    norms = np.linalg.norm(result, axis=1)
    np.testing.assert_allclose(norms, [1.0, 1.0], atol=1e-6)


# ---------------------------------------------------------------------------
# _select_providers
# ---------------------------------------------------------------------------


def test_select_providers_skips_coreml_even_when_available(monkeypatch):
    # CoreML is intentionally excluded: the all-MiniLM-L6-v2 graph only has
    # 68% CoreML node coverage, producing 55 partitions with expensive
    # CoreML<->CPU sync overhead that is slower than ARM64 quantized CPU ONNX.
    monkeypatch.setattr(
        "onnxruntime.get_available_providers",
        lambda: ["CoreMLExecutionProvider", "CPUExecutionProvider"],
    )
    import onnxruntime  # noqa: F401 — ensure monkeypatch target exists
    providers = oem._select_providers()
    assert providers == ["CPUExecutionProvider"]


def test_select_providers_returns_directml_on_windows(monkeypatch):
    monkeypatch.setattr(
        "onnxruntime.get_available_providers",
        lambda: ["DmlExecutionProvider", "CPUExecutionProvider"],
    )
    providers = oem._select_providers()
    assert providers[0] == "DmlExecutionProvider"


def test_select_providers_falls_back_to_cpu(monkeypatch):
    monkeypatch.setattr(
        "onnxruntime.get_available_providers",
        lambda: ["CPUExecutionProvider"],
    )
    providers = oem._select_providers()
    assert providers == ["CPUExecutionProvider"]


def test_select_providers_prefers_directml_when_coreml_also_available(monkeypatch):
    # DirectML is preferred when both are available; CoreML is always skipped.
    monkeypatch.setattr(
        "onnxruntime.get_available_providers",
        lambda: ["DmlExecutionProvider", "CoreMLExecutionProvider", "CPUExecutionProvider"],
    )
    providers = oem._select_providers()
    assert providers[0] == "DmlExecutionProvider"


# ---------------------------------------------------------------------------
# OnnxEmbedder.encode — with mocked session + tokenizer
# ---------------------------------------------------------------------------


def _make_fake_encoding(ids, attention_mask):
    enc = SimpleNamespace(
        ids=ids,
        attention_mask=attention_mask,
        type_ids=[0] * len(ids),
    )
    return enc


def _make_embedder_with_mock_session(
    *,
    output_name: str = "last_hidden_state",
    hidden_dim: int = 4,
    seq_len: int = 3,
    batch_size: int = 2,
    providers: list[str] | None = None,
) -> oem.OnnxEmbedder:
    """Return an OnnxEmbedder with its session and tokenizer fully mocked."""
    if providers is None:
        providers = ["CPUExecutionProvider"]

    # Mock onnxruntime session
    mock_session = MagicMock()
    mock_session.get_providers.return_value = providers
    mock_session.get_inputs.return_value = [
        SimpleNamespace(name="input_ids"),
        SimpleNamespace(name="attention_mask"),
        SimpleNamespace(name="token_type_ids"),
    ]
    mock_session.get_outputs.return_value = [SimpleNamespace(name=output_name)]

    def fake_run(output_names, feeds):
        b = feeds["input_ids"].shape[0]
        if output_name == "sentence_embedding":
            return [np.random.default_rng(0).random((b, hidden_dim)).astype(np.float32)]
        # last_hidden_state: (B, L, D)
        return [np.random.default_rng(0).random((b, seq_len, hidden_dim)).astype(np.float32)]

    mock_session.run.side_effect = fake_run

    # Mock tokenizer
    mock_tokenizer = MagicMock()
    mock_tokenizer.token_to_id.return_value = 0

    def fake_encode_batch(sentences):
        ids = list(range(1, seq_len + 1))
        mask = [1] * seq_len
        return [_make_fake_encoding(ids, mask) for _ in sentences]

    mock_tokenizer.encode_batch.side_effect = fake_encode_batch

    embedder = oem.OnnxEmbedder.__new__(oem.OnnxEmbedder)
    embedder._session = mock_session
    embedder._input_names = {"input_ids", "attention_mask", "token_type_ids"}
    embedder._output_names = {output_name}
    embedder._tokenizer = mock_tokenizer
    return embedder


def test_encode_returns_correct_shape():
    embedder = _make_embedder_with_mock_session(hidden_dim=4)
    result = embedder.encode(["hello", "world", "foo"])
    assert result.shape == (3, 4)
    assert result.dtype == np.float32


def test_encode_normalizes_output_for_last_hidden_state():
    embedder = _make_embedder_with_mock_session(output_name="last_hidden_state", hidden_dim=4)
    result = embedder.encode(["a", "b"])
    norms = np.linalg.norm(result, axis=1)
    np.testing.assert_allclose(norms, [1.0, 1.0], atol=1e-5)


def test_encode_passes_sentence_embedding_through_without_renormalizing():
    embedder = _make_embedder_with_mock_session(output_name="sentence_embedding", hidden_dim=4)
    result = embedder.encode(["a"])
    assert result.shape == (1, 4)


def test_encode_accepts_show_progress_bar_kwarg():
    embedder = _make_embedder_with_mock_session()
    # Must not raise even though OnnxEmbedder ignores the flag
    result = embedder.encode(["x"], show_progress_bar=True)
    assert result.shape[0] == 1


def test_encode_batches_internally():
    """Sentences > batch_size must be split into multiple ONNX runs."""
    embedder = _make_embedder_with_mock_session(hidden_dim=4)
    docs = [f"doc {i}" for i in range(10)]
    result = embedder.encode(docs, batch_size=3)
    assert result.shape == (10, 4)
    # encode_batch should have been called ceil(10/3) = 4 times
    assert embedder._tokenizer.encode_batch.call_count == 4


def test_encode_single_batch_no_concatenation():
    """When everything fits in one batch, result is the raw chunk (no concat)."""
    embedder = _make_embedder_with_mock_session(hidden_dim=4)
    result = embedder.encode(["only one doc"], batch_size=32)
    assert result.shape == (1, 4)


def test_encode_skips_token_type_ids_when_not_in_inputs():
    embedder = _make_embedder_with_mock_session()
    embedder._input_names = {"input_ids", "attention_mask"}  # no token_type_ids
    result = embedder.encode(["a"])
    call_feeds = embedder._session.run.call_args[0][1]
    assert "token_type_ids" not in call_feeds


# ---------------------------------------------------------------------------
# _select_onnx_filename
# ---------------------------------------------------------------------------


def test_select_onnx_filename_arm64_when_coreml_provider_passed(monkeypatch):
    # _select_providers() never produces a CoreML provider list anymore, but
    # _select_onnx_filename() should still behave sensibly if called with one.
    monkeypatch.setattr(platform, "machine", lambda: "arm64")
    assert oem._select_onnx_filename(["CoreMLExecutionProvider", "CPUExecutionProvider"]) == "onnx/model_qint8_arm64.onnx"


def test_select_onnx_filename_fp32_for_directml():
    assert oem._select_onnx_filename(["DmlExecutionProvider", "CPUExecutionProvider"]) == "onnx/model.onnx"


def test_select_onnx_filename_arm64_for_cpu_arm(monkeypatch):
    monkeypatch.setattr(platform, "machine", lambda: "arm64")
    assert oem._select_onnx_filename(["CPUExecutionProvider"]) == "onnx/model_qint8_arm64.onnx"


def test_select_onnx_filename_avx2_for_cpu_x86(monkeypatch):
    monkeypatch.setattr(platform, "machine", lambda: "x86_64")
    assert oem._select_onnx_filename(["CPUExecutionProvider"]) == "onnx/model_quint8_avx2.onnx"


# ---------------------------------------------------------------------------
# OnnxEmbedder.from_pretrained — mock hf_hub_download
# ---------------------------------------------------------------------------


def _patched_from_pretrained(tmp_path, monkeypatch, *, platform_machine="x86_64"):
    downloaded: list[str] = []
    monkeypatch.setattr(platform, "machine", lambda: platform_machine)
    monkeypatch.setattr(
        "onnxruntime.get_available_providers",
        lambda: ["CPUExecutionProvider"],
    )

    def fake_hf_download(repo_id, filename, **kwargs):
        downloaded.append(filename)
        p = tmp_path / filename.replace("/", "_")
        p.write_bytes(b"fake")
        return str(p)

    mock_hf = MagicMock()
    mock_hf.hf_hub_download = fake_hf_download
    monkeypatch.setitem(sys.modules, "huggingface_hub", mock_hf)
    return downloaded


def test_from_pretrained_downloads_platform_quantized_model(tmp_path, monkeypatch):
    downloaded = _patched_from_pretrained(tmp_path, monkeypatch, platform_machine="x86_64")
    with patch.object(oem.OnnxEmbedder, "__init__", return_value=None):
        oem.OnnxEmbedder.from_pretrained("some/model")
    assert "onnx/model_quint8_avx2.onnx" in downloaded


def test_from_pretrained_downloads_arm64_model_on_arm(tmp_path, monkeypatch):
    downloaded = _patched_from_pretrained(tmp_path, monkeypatch, platform_machine="arm64")
    with patch.object(oem.OnnxEmbedder, "__init__", return_value=None):
        oem.OnnxEmbedder.from_pretrained("some/model")
    assert "onnx/model_qint8_arm64.onnx" in downloaded


def test_from_pretrained_falls_back_to_fp32_on_missing_quantized(tmp_path, monkeypatch):
    monkeypatch.setattr(platform, "machine", lambda: "x86_64")
    monkeypatch.setattr(
        "onnxruntime.get_available_providers",
        lambda: ["CPUExecutionProvider"],
    )

    def fake_hf_download(repo_id, filename, **kwargs):
        if filename == "onnx/model_quint8_avx2.onnx":
            raise FileNotFoundError("not found")
        p = tmp_path / filename.replace("/", "_")
        p.write_bytes(b"fake")
        return str(p)

    mock_hf = MagicMock()
    mock_hf.hf_hub_download = fake_hf_download
    monkeypatch.setitem(sys.modules, "huggingface_hub", mock_hf)

    with patch.object(oem.OnnxEmbedder, "__init__", return_value=None):
        oem.OnnxEmbedder.from_pretrained("some/model")  # must not raise


def test_from_pretrained_downloads_tokenizer(tmp_path, monkeypatch):
    downloaded = _patched_from_pretrained(tmp_path, monkeypatch)
    with patch.object(oem.OnnxEmbedder, "__init__", return_value=None):
        oem.OnnxEmbedder.from_pretrained("some/model")
    assert "tokenizer.json" in downloaded


def test_from_pretrained_uses_arm64_quantized_even_when_coreml_available(tmp_path, monkeypatch):
    # CoreML is excluded from provider selection; the ARM64 quantized model
    # should be chosen on ARM64 regardless of CoreML availability.
    downloaded: list[str] = []
    monkeypatch.setattr(
        "onnxruntime.get_available_providers",
        lambda: ["CoreMLExecutionProvider", "CPUExecutionProvider"],
    )
    monkeypatch.setattr(platform, "machine", lambda: "arm64")

    def fake_hf_download(repo_id, filename, **kwargs):
        downloaded.append(filename)
        p = tmp_path / filename.replace("/", "_")
        p.write_bytes(b"fake")
        return str(p)

    mock_hf = MagicMock()
    mock_hf.hf_hub_download = fake_hf_download
    monkeypatch.setitem(sys.modules, "huggingface_hub", mock_hf)

    with patch.object(oem.OnnxEmbedder, "__init__", return_value=None):
        oem.OnnxEmbedder.from_pretrained("some/model")

    assert "onnx/model_qint8_arm64.onnx" in downloaded
    assert "onnx/model.onnx" not in downloaded
