"""ONNX Runtime embedding backend for sentence-level text encoding.

Drop-in replacement for SentenceTransformer("all-MiniLM-L6-v2") that uses
ONNX Runtime instead of PyTorch.  Platform-aware execution provider selection:
  - DmlExecutionProvider  — Windows (DirectML, works on any GPU/iGPU)
  - CPUExecutionProvider  — universal fallback (ARM64 quantized model on Mac)

Note: CoreMLExecutionProvider is intentionally skipped.  The all-MiniLM-L6-v2
graph has only 68% CoreML node coverage (285/418), producing 55 separate
CoreML↔CPU partitions that add heavy synchronisation overhead on every batch.
Running the ARM64-quantized model on CPUExecutionProvider is significantly
faster on Apple Silicon.

The class implements the same `.encode(sentences)` interface as
SentenceTransformer so it can be passed directly as BERTopic's
`embedding_model=` argument.
"""

from __future__ import annotations

import logging
import platform
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import numpy as np

logger = logging.getLogger(__name__)

_MAX_SEQ_LEN = 256  # all-MiniLM-L6-v2 uses 256-token windows


def _select_providers() -> list[str]:
    """Return the best available ONNX execution provider list for this platform.

    CoreMLExecutionProvider is intentionally excluded: the all-MiniLM-L6-v2
    ONNX graph is only 68% CoreML-compatible (285/418 nodes), leaving 55
    separate CoreML↔CPU partitions that must sync data on every inference
    call.  The per-batch overhead is worse than running the ARM64 quantized
    model entirely on CPU.  DirectML on Windows covers the full graph, so
    it remains a preferred provider.
    """
    import onnxruntime as ort

    available = set(ort.get_available_providers())
    if "DmlExecutionProvider" in available:
        return ["DmlExecutionProvider", "CPUExecutionProvider"]
    return ["CPUExecutionProvider"]


def _select_onnx_filename(providers: list[str]) -> str:
    """Choose the best ONNX model file given the active provider list.

    DirectML runs its own compilation pass on the fp32 model and doesn't
    benefit from CPU-level weight quantisation.  For CPUExecutionProvider
    we pick the architecture-specific quantized variant for the speed win.

    Available quantized variants in sentence-transformers/all-MiniLM-L6-v2:
      onnx/model_qint8_arm64.onnx   — ARM64 (Apple Silicon)
      onnx/model_quint8_avx2.onnx   — x86_64 with AVX2 (2013+, ubiquitous)
      onnx/model.onnx                — fp32 fallback, works everywhere
    """
    active = providers[0] if providers else "CPUExecutionProvider"
    if active == "DmlExecutionProvider":
        return "onnx/model.onnx"

    machine = platform.machine().lower()
    if machine in ("arm64", "aarch64"):
        return "onnx/model_qint8_arm64.onnx"
    return "onnx/model_quint8_avx2.onnx"


def _mean_pool(
    token_embeddings: "np.ndarray",
    attention_mask: "np.ndarray",
) -> "np.ndarray":
    """Attention-mask-weighted mean of token embeddings — (B, L, D) → (B, D)."""
    import numpy as np

    mask = attention_mask[:, :, np.newaxis].astype(np.float32)  # (B, L, 1)
    summed = (token_embeddings * mask).sum(axis=1)               # (B, D)
    count = mask.sum(axis=1).clip(min=1e-9)                      # (B, 1)
    return summed / count


def _l2_normalize(embeddings: "np.ndarray") -> "np.ndarray":
    """Row-wise L2 normalization."""
    import numpy as np

    norms = np.linalg.norm(embeddings, axis=1, keepdims=True).clip(min=1e-9)
    return (embeddings / norms).astype(np.float32)


class OnnxEmbedder:
    """Sentence embedder backed by ONNX Runtime.

    Produces L2-normalised mean-pooled embeddings matching the output of
    SentenceTransformer("all-MiniLM-L6-v2") to within float32 tolerance.
    """

    def __init__(self, model_path: Path, tokenizer_path: Path) -> None:
        import onnxruntime as ort
        from tokenizers import Tokenizer

        providers = _select_providers()
        self._session = ort.InferenceSession(str(model_path), providers=providers)
        self._input_names = {inp.name for inp in self._session.get_inputs()}
        self._output_names = {out.name for out in self._session.get_outputs()}

        self._tokenizer = Tokenizer.from_file(str(tokenizer_path))
        pad_id = self._tokenizer.token_to_id("[PAD]") or 0
        self._tokenizer.enable_padding(pad_id=pad_id, pad_token="[PAD]")
        self._tokenizer.enable_truncation(max_length=_MAX_SEQ_LEN)

        self.provider: str = self._session.get_providers()[0]
        logger.info(
            "[OnnxEmbedder] provider=%s model=%s",
            self.provider,
            model_path.name,
        )

    def encode(
        self,
        sentences: list[str],
        *,
        show_progress_bar: bool = False,
        batch_size: int = 32,
    ) -> "np.ndarray":
        """Encode sentences to L2-normalised embeddings.

        `show_progress_bar` is accepted for API compatibility with
        SentenceTransformer.encode but is always ignored.
        """
        import numpy as np

        chunks: list[np.ndarray] = []

        for start in range(0, len(sentences), batch_size):
            batch = sentences[start : start + batch_size]
            encoded = self._tokenizer.encode_batch(batch)

            input_ids = np.array([e.ids for e in encoded], dtype=np.int64)
            attention_mask = np.array(
                [e.attention_mask for e in encoded], dtype=np.int64
            )

            feeds: dict[str, np.ndarray] = {
                "input_ids": input_ids,
                "attention_mask": attention_mask,
            }
            if "token_type_ids" in self._input_names:
                feeds["token_type_ids"] = np.array(
                    [e.type_ids for e in encoded], dtype=np.int64
                )

            if "sentence_embedding" in self._output_names:
                # Some ONNX exports include the pooling + normalisation layer.
                outputs = self._session.run(["sentence_embedding"], feeds)
                chunk = outputs[0].astype(np.float32)
            else:
                outputs = self._session.run(["last_hidden_state"], feeds)
                chunk = _l2_normalize(_mean_pool(outputs[0], attention_mask))

            chunks.append(chunk)

        return np.concatenate(chunks, axis=0) if len(chunks) > 1 else chunks[0]

    @classmethod
    def from_pretrained(cls, model_id: str) -> "OnnxEmbedder":
        """Download (or load from HF cache) an ONNX model and return an embedder.

        Provider selection and model-file choice are both done here so the
        caller only needs to pass the HuggingFace repo ID.
        """
        from huggingface_hub import hf_hub_download

        providers = _select_providers()
        onnx_filename = _select_onnx_filename(providers)
        try:
            model_path = Path(
                hf_hub_download(repo_id=model_id, filename=onnx_filename)
            )
        except Exception:
            logger.warning(
                "[OnnxEmbedder] %s not found in %s, falling back to onnx/model.onnx",
                onnx_filename,
                model_id,
            )
            model_path = Path(
                hf_hub_download(repo_id=model_id, filename="onnx/model.onnx")
            )

        tokenizer_path = Path(
            hf_hub_download(repo_id=model_id, filename="tokenizer.json")
        )

        return cls(model_path=model_path, tokenizer_path=tokenizer_path)
