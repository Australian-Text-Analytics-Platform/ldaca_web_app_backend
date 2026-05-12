"""Topic modeling worker task implementation."""

from __future__ import annotations

import logging
import os
import pickle
import random
import re
from pathlib import Path
from typing import Any, Callable, Dict, Optional, cast
from uuid import uuid4

from ..api.workspaces.analyses.generated_columns import (
    TOPIC_COLUMN,
    TOPIC_MEANING_COLUMN,
)


logger = logging.getLogger(__name__)

_EMBEDDER_CACHE: dict[tuple[str, str], Any] = {}
_EMBEDDING_CHUNK_SIZE = 512

# Phase 3.1: language → (repo_id, revision) for the topic-modeling embedder.
# English keeps the pinned MiniLM-L6 the topic-modeling team has been
# validating against (existing flows are byte-identical when language
# resolves to "en"). Anything else routes to the multilingual MiniLM-L12,
# which is same 384-dim embedding space, ~110 MB ONNX, 50+ languages
# including ZH / JA / KO / ES / FR / DE per decision 3.
#
# Revision pinning for the multilingual model is deferred until the ZH
# workflow is validated end-to-end. ``scripts/check_model_updates.py``
# is the release-time deliberate bump point.
_TOPIC_EMBEDDERS_BY_LANGUAGE: dict[str, tuple[str, "str | None"]] = {
    "en": (
        "sentence-transformers/all-MiniLM-L6-v2",
        "c9745ed1d9f207416be6d2e6f8de32d1f16199bf",
    ),
    "multi": (
        "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
        None,
    ),
}

# Back-compat alias used by the result payload and existing telemetry — the
# English pair is what English callers (the previous default) still see.
_TOPIC_EMBEDDER_REPO_ID, _TOPIC_EMBEDDER_REVISION = _TOPIC_EMBEDDERS_BY_LANGUAGE["en"]


def _select_embedder(language: "str | None") -> tuple[str, "str | None"]:
    """Return ``(repo_id, revision)`` for ``language``. English keeps the
    pinned MiniLM-L6 (back-compat); everything else routes to the
    multilingual fallback so ZH / JA topic modeling produces non-degenerate
    clusters.
    """
    code = (language or "en").strip().lower()
    if code == "en":
        return _TOPIC_EMBEDDERS_BY_LANGUAGE["en"]
    return _TOPIC_EMBEDDERS_BY_LANGUAGE["multi"]


def _embedder_cache_label(repo_id: str, revision: "str | None") -> str:
    """Format the embedder identifier used for on-disk cache keying so the
    same revision string format as before lands in the cache filename."""
    suffix = revision[:8] if revision else "latest"
    return f"{repo_id}@{suffix}"

# Auto-engagement of the online pipeline is disabled: sampling handles large
# corpora by default.  These thresholds are set to effectively unreachable
# values so the classic UMAP+HDBSCAN pipeline is always used unless the caller
# explicitly passes force_mode="online".
_ONLINE_THRESHOLD_DOCS = 10_000_000
_ONLINE_THRESHOLD_BYTES = 10 * 1024 * 1024 * 1024  # 10 GB


def _make_reagg_path(old_path: Path) -> Path:
    """Return a fresh unique path beside `old_path` for a re-aggregation rewrite.

    Re-aggregation must not overwrite `old_path` because previously-detached
    workspace nodes hold lazy `scan_parquet(old_path)` references. We keep
    the original directory and the original "base" stem (stripping any prior
    `_r<hex>` suffix from earlier re-aggregations to keep names compact) and
    append a fresh short hex suffix.
    """
    base_stem = re.sub(r"(_r[0-9a-f]+)+$", "", old_path.stem)
    return old_path.parent / f"{base_stem}_r{uuid4().hex[:8]}{old_path.suffix}"


def _sample_corpus(
    docs: list[str], fraction: float, seed: int
) -> tuple[list[str], list[int]]:
    """Return a reproducible random sample of docs and their original indices.

    Uses the same Polars expression as the preprocessing slice tool
    (`pl.int_range(...).sample(fraction=..., seed=...)`) so identical
    `(seed, fraction)` parameters select identical rows across tools.
    Operates on an in-memory integer Series; no parquet artifact is created.
    """
    import polars as pl
    if fraction >= 1.0:
        return docs, list(range(len(docs)))
    indices = (
        pl.int_range(len(docs), eager=True)
        .sample(fraction=fraction, seed=seed)
        .sort()
        .to_list()
    )
    if not indices:
        # Polars floors fraction*N, so a tiny corpus with very small fraction
        # can yield zero rows. Topic modelling needs at least one document.
        return [docs[0]], [0]
    return [docs[i] for i in indices], indices


def _compute_min_topic_size(
    n_eff: int,
    topic_size_mode: str,
    topic_size_value: int,
) -> int:
    """Derive BERTopic min_topic_size from the chosen sizing mode.

    Args:
        n_eff: Effective document count (post-sample total across all corpora).
        topic_size_mode: "target", "min", or "exact".
        topic_size_value: The user-supplied numeric value for the chosen mode.
    """
    if topic_size_mode == "min":
        return max(2, int(topic_size_value))
    if topic_size_mode == "exact":
        # Start from the target-mode heuristic, then reduce it so BERTopic is
        # more likely to produce enough raw topics before exact post-fit merging.
        target_min_topic_size = max(2, n_eff // (int(topic_size_value) * 10))
        return max(5, int(target_min_topic_size * 0.75))
    # "target" (default)
    return max(2, n_eff // (int(topic_size_value) * 10))


def _should_use_online_pipeline(docs: list[str], force_mode: str | None) -> bool:
    """Return True when the online pipeline should be used for this corpus."""
    if force_mode == "online":
        return True
    if force_mode == "classic":
        return False
    if len(docs) > _ONLINE_THRESHOLD_DOCS:
        return True
    return sum(len(d) for d in docs) > _ONLINE_THRESHOLD_BYTES


def _resolve_top_n_words(representative_words_count: "int | None") -> int:
    """Pick BERTopic's ``top_n_words`` from the user-requested display cap.

    BERTopic's default is 10. When the user picks "Words per topic = 35"
    and toggles on the frontend stopword filter, c-TF-IDF would compute
    only 10 raw words; the filter would drop most of them as function
    words (English: by sklearn's stoplist at the vectorizer; non-English:
    by the post-fit filter); and the user would see 1–3 — even though
    they asked for 35.

    We pre-compute a generous headroom so the post-filter slice still
    has enough material:

    - At least 50 candidates, so even a tiny request like 5 has a
      healthy buffer for the stopword filter.
    - Otherwise 2× the requested cap.

    Performance impact is negligible — c-TF-IDF already produces a
    ranked vocabulary per topic; ``top_n_words`` just decides where to
    truncate.
    """
    requested = int(representative_words_count or 0)
    return max(50, requested * 2) if requested > 0 else 50


def _build_classic_pipeline(
    min_topic_size: int,
    random_state: int,
    embedder: Any,
    top_n_words: int = 50,
) -> Any:
    """Build a standard BERTopic pipeline with UMAP + HDBSCAN."""
    from bertopic import BERTopic
    from umap import UMAP

    return BERTopic(
        verbose=False,
        min_topic_size=min_topic_size,
        embedding_model=embedder,
        umap_model=UMAP(
            n_neighbors=15,
            n_components=5,
            min_dist=0.0,
            metric="cosine",
            random_state=random_state,
        ),
        top_n_words=top_n_words,
    )


def _build_online_pipeline(
    n_docs: int,
    n_clusters: int | None,
    random_state: int,
    embedder: Any,
    language: str | None = None,
    top_n_words: int = 50,
) -> tuple[Any, int]:
    """Build a BERTopic pipeline using IncrementalPCA + MiniBatchKMeans.

    Returns (topic_model, k) where k is the actual cluster count selected.
    Suitable for corpora that exceed the online-mode thresholds.

    Phase 3.5: ``language`` routes the per-topic label-stage vectorizer's
    stopword filter. Only ``"en"`` gets sklearn's built-in English list;
    everything else gets ``None`` so e.g. Chinese function words 的/是/了
    aren't English-filtered (i.e. silently kept).
    """
    import math

    from bertopic import BERTopic
    from sklearn.cluster import MiniBatchKMeans
    from sklearn.decomposition import IncrementalPCA

    k = (
        n_clusters
        if (n_clusters and n_clusters > 0)
        else max(10, min(200, int(math.sqrt(n_docs / 2))))
    )

    dim_model = IncrementalPCA(n_components=5)
    cluster_model = MiniBatchKMeans(n_clusters=k, random_state=random_state, n_init="auto")

    kwargs: dict[str, Any] = dict(
        verbose=False,
        embedding_model=embedder,
        umap_model=dim_model,
        hdbscan_model=cluster_model,
        top_n_words=top_n_words,
    )
    try:
        from bertopic.vectorizers import OnlineCountVectorizer

        resolved_language = (language or "en").strip().lower()
        stop_words = "english" if resolved_language == "en" else None
        kwargs["vectorizer_model"] = OnlineCountVectorizer(
            stop_words=stop_words, decay=0.01
        )
    except ImportError:
        logger.debug("[Worker] OnlineCountVectorizer unavailable; using default CountVectorizer")

    return BERTopic(**kwargs), k


def _get_embedder(model_id: str, revision: "str | None" = None):
    """Get or create a cached embedder per worker process.

    On Apple Silicon, prefers MPS (PyTorch Metal) over ONNX CPU — the full
    BERT graph runs on Metal/Neural Engine as a single unit, giving ~3× cold
    throughput vs the ARM64 quantized ONNX path (64s vs 201s on M1 Max,
    26k docs).  Falls through to ONNX on Windows, Linux, and Intel Macs.

    ``revision`` is ``None`` for the multilingual embedder until it gets
    pinned at release time; the cache key uses the empty string as a stable
    sentinel so the per-process cache still works.
    """
    cache_key = (model_id, revision or "")
    embedder = _EMBEDDER_CACHE.get(cache_key)
    if embedder is not None:
        return embedder

    from .mps_embedder import is_mps_available

    if is_mps_available():
        from .mps_embedder import MpsEmbedder

        embedder = MpsEmbedder.from_pretrained(model_id, revision=revision)
    else:
        from .onnx_embedder import OnnxEmbedder

        embedder = OnnxEmbedder.from_pretrained(model_id, revision=revision)

    _EMBEDDER_CACHE[cache_key] = embedder
    return embedder


def _persist_exact_reduction_artifact(
    artifact_path: str,
    *,
    topic_model: Any,
    all_docs: list[str],
    corpus_sizes: list[int],
    active_corpora_indices: list[list[int]],
) -> None:
    artifact_payload: dict[str, Any] = {
        "all_docs": all_docs,
        "corpus_sizes": corpus_sizes,
        "active_corpora_indices": active_corpora_indices,
    }
    if hasattr(topic_model, "save"):
        model_path = f"{artifact_path}.bertopic"
        try:
            topic_model.save(
                model_path,
                serialization="pickle",
                save_embedding_model=False,
            )
        except TypeError:
            topic_model.save(model_path, serialization="pickle")
        artifact_payload["model_path"] = model_path
    else:
        artifact_payload["topic_model"] = topic_model
    with open(artifact_path, "wb") as artifact_file:
        pickle.dump(artifact_payload, artifact_file)


def _load_exact_reduction_artifact(artifact_path: str) -> dict[str, Any]:
    with open(artifact_path, "rb") as artifact_file:
        loaded = pickle.load(artifact_file)
    if not isinstance(loaded, dict):
        raise ValueError("Exact topic reduction artifact is invalid")
    model_path = loaded.get("model_path")
    if isinstance(model_path, str) and model_path:
        from bertopic import BERTopic

        loaded["topic_model"] = BERTopic.load(model_path)
    return loaded


def _count_non_outlier_topics(topic_model: Any) -> int:
    import numpy as np

    topic_freq_pd = topic_model.get_topic_freq()
    if topic_freq_pd is None or "Topic" not in topic_freq_pd:
        return 0
    return sum(
        1
        for topic_id in topic_freq_pd["Topic"].tolist()
        if isinstance(topic_id, (int, np.integer)) and int(topic_id) != -1
    )


def _has_outlier_topic(topic_model: Any) -> bool:
    import numpy as np

    topic_freq_pd = topic_model.get_topic_freq()
    if topic_freq_pd is None or "Topic" not in topic_freq_pd:
        return False
    return any(
        isinstance(topic_id, (int, np.integer)) and int(topic_id) == -1
        for topic_id in topic_freq_pd["Topic"].tolist()
    )


def _resolve_exact_reduce_topics_target(
    topic_model: Any, requested_topic_count: int
) -> int:
    """Map a requested non-outlier topic count to BERTopic's reduction target.

    BERTopic retains the outlier topic (`-1`) in `nr_topics` accounting, but the
    UI and the rest of this worker expose only non-outlier topics. When an
    outlier bucket exists, ask BERTopic for one extra topic so the visible topic
    count matches the user's exact selection.
    """

    has_outlier_topic = _has_outlier_topic(topic_model)
    current_total = _count_non_outlier_topics(topic_model) + int(has_outlier_topic)
    requested_total = int(requested_topic_count) + int(has_outlier_topic)
    return min(current_total, requested_total)


def _build_topic_result_payload(
    *,
    topic_model: Any,
    node_infos: list[Dict[str, Any]],
    all_docs: list[str],
    corpus_sizes: list[int],
    active_corpora_indices: list[list[int]],
    max_representative_words: int,
    random_state: int,
    assigned_topics: list[int] | None = None,
    artifact_prefix: str | None = None,
    artifact_root: Any | None = None,
    existing_artifacts: dict[str, Any] | None = None,
) -> dict[str, Any]:
    from pathlib import Path

    import numpy as np
    import polars as pl
    from bertopic._utils import select_topic_representation

    # Tracks parquet paths that this call is replacing during re-aggregation.
    # Previously-detached workspace nodes still hold lazy references to the
    # superseded paths, so we keep the old files on disk and only record them
    # in the manifest. The existing task-cleanup helper walks the manifest
    # tree to delete every `*_path` / `*_parquet_path` key it finds, so listing
    # them here under `superseded_artifacts` lets cleanup reclaim the disk
    # whenever the task is finally cleared.
    newly_superseded: list[dict[str, str]] = []
    if existing_artifacts is None:
        if artifact_prefix is None or artifact_root is None:
            raise ValueError("artifact_prefix and artifact_root are required")
        artifact_root_path = Path(artifact_root)
        topic_meanings_path = artifact_root_path / f"{artifact_prefix}_topic_meanings.parquet"
        existing_node_artifacts: list[dict[str, Any]] = []
    else:
        old_topic_meanings_path = Path(
            str(existing_artifacts.get("topic_meanings_parquet_path") or "")
        )
        if not str(old_topic_meanings_path):
            raise ValueError("Topic meanings artifact path is missing")
        # Re-aggregation must not overwrite the meanings parquet — the previous
        # detach's `topic_meanings` node scans it lazily.
        topic_meanings_path = _make_reagg_path(old_topic_meanings_path)
        newly_superseded.append(
            {"topic_meanings_parquet_path": str(old_topic_meanings_path)}
        )
        node_payloads = existing_artifacts.get("nodes")
        if not isinstance(node_payloads, list):
            raise ValueError("Topic assignment artifacts are missing")
        existing_node_artifacts = [
            payload for payload in node_payloads if isinstance(payload, dict)
        ]
        if len(existing_node_artifacts) != len(node_infos):
            raise ValueError("Topic assignment artifacts do not match the request")

    if assigned_topics is None:
        assigned_topics = list(getattr(topic_model, "topics_", []))
    if len(assigned_topics) != len(all_docs):
        raise ValueError("Topic assignments do not match the document count")

    assignments: list[list[int]] = []
    node_artifacts: list[dict[str, Any]] = []
    offset = 0
    for idx, size in enumerate(corpus_sizes):
        end = offset + size
        corpus_topics = assigned_topics[offset:end]
        normalized_topics = [
            int(topic_id) if isinstance(topic_id, (int, np.integer)) else -1
            for topic_id in corpus_topics
        ]
        assignments.append(normalized_topics)

        if existing_artifacts is None:
            node_id = str(node_infos[idx]["node_id"])
            node_name = str(node_infos[idx].get("node_name") or node_id)
            text_column = str(node_infos[idx].get("text_column") or "")
            original_columns = list(node_infos[idx].get("original_columns") or [])
            assignments_path = artifact_root_path / f"{artifact_prefix}_topic_assignments_{node_id}.parquet"
        else:
            existing_node = existing_node_artifacts[idx]
            node_id = str(existing_node.get("node_id") or node_infos[idx]["node_id"])
            node_name = str(existing_node.get("node_name") or node_infos[idx].get("node_name") or node_id)
            text_column = str(existing_node.get("text_column") or node_infos[idx].get("text_column") or "")
            original_columns = list(
                existing_node.get("original_columns")
                or node_infos[idx].get("original_columns")
                or []
            )
            old_assignments_path = Path(
                str(existing_node.get("assignments_parquet_path") or "")
            )
            if not str(old_assignments_path):
                raise ValueError("Topic assignment artifact path is missing")
            # Re-aggregation must not overwrite the assignments parquet — the
            # previous detach's lazy plan still scans it.
            assignments_path = _make_reagg_path(old_assignments_path)
            newly_superseded.append(
                {"assignments_parquet_path": str(old_assignments_path)}
            )

        pl.DataFrame(
            {
                "__row_nr__": active_corpora_indices[idx],
                TOPIC_COLUMN: normalized_topics,
            }
        ).with_columns(
            [
                pl.col("__row_nr__").cast(pl.Int64),
                pl.col(TOPIC_COLUMN).cast(pl.Int64),
            ]
        ).lazy().sink_parquet(assignments_path)
        node_artifacts.append(
            {
                "node_id": node_id,
                "node_name": node_name,
                "text_column": text_column,
                "original_columns": original_columns,
                "assignments_parquet_path": str(assignments_path),
            }
        )
        offset = end

    per_corpus_topic_counts: list[dict[int, int]] = []
    for corpus_topics in assignments:
        counts: dict[int, int] = {}
        for topic_id in corpus_topics:
            counts[topic_id] = counts.get(topic_id, 0) + 1
        per_corpus_topic_counts.append(counts)

    topic_freq_pd = topic_model.get_topic_freq()
    topic_freq = (
        cast(pl.DataFrame, pl.from_pandas(topic_freq_pd))
        if topic_freq_pd is not None
        else pl.DataFrame(schema={"Topic": pl.Int64})
    )

    topic_ids: list[int] = []
    if "Topic" in topic_freq.columns and not topic_freq.is_empty():
        topic_series = topic_freq.get_column("Topic")
        topic_ids = [
            int(topic_id)
            for topic_id in topic_series.to_list()
            if isinstance(topic_id, (int, np.integer)) and int(topic_id) != -1
        ]

    topics_by_id = cast(dict[int, list[tuple[str, float]]], topic_model.get_topics())
    representative_words_by_topic: list[list[str]] = []
    labels: list[str] = []
    for topic_id in topic_ids:
        words = topics_by_id.get(topic_id, [])
        top_words = [
            word
            for word, _score in words[:max_representative_words]
            if isinstance(word, str) and word
        ]
        representative_words_by_topic.append(top_words)
        labels.append(" | ".join(top_words) if top_words else f"Topic {topic_id}")

    all_topics_sorted = sorted(topics_by_id.keys())
    indices = (
        np.array([all_topics_sorted.index(topic_id) for topic_id in topic_ids])
        if topic_ids
        else np.array([])
    )

    embeddings, c_tfidf_used = select_topic_representation(
        topic_model.c_tf_idf_,
        topic_model.topic_embeddings_,
        output_ndarray=True,
    )
    if len(indices) > 0:
        embeddings = embeddings[indices]
    else:
        embeddings = np.zeros((0, 2))

    if embeddings.shape[0] == 0:
        coords = embeddings
    elif embeddings.shape[0] == 1:
        coords = np.array([[0.0, 0.0]])
    elif embeddings.shape[0] <= 15:
        from sklearn.decomposition import PCA

        comps = min(2, embeddings.shape[1])
        projected = PCA(n_components=comps, random_state=random_state).fit_transform(
            embeddings
        )
        if comps == 1:
            coords = np.column_stack([projected[:, 0], np.zeros_like(projected[:, 0])])
        else:
            coords = projected
    else:
        try:
            from umap import UMAP

            n_samples = embeddings.shape[0]
            n_neighbors = max(2, min(15, n_samples - 2))
            if c_tfidf_used:
                from sklearn.preprocessing import MinMaxScaler

                normalized = MinMaxScaler().fit_transform(embeddings)
                coords = UMAP(
                    n_neighbors=n_neighbors,
                    n_components=2,
                    metric="hellinger",
                    random_state=random_state,
                ).fit_transform(normalized)
            else:
                coords = UMAP(
                    n_neighbors=n_neighbors,
                    n_components=2,
                    metric="cosine",
                    random_state=random_state,
                ).fit_transform(embeddings)
        except (
            ImportError,
            ModuleNotFoundError,
            TypeError,
            ValueError,
            RuntimeError,
        ) as umap_error:
            logger.warning(
                "[Worker %d] UMAP failed: %s. Falling back to PCA.",
                os.getpid(),
                umap_error,
            )
            from sklearn.decomposition import PCA

            comps = min(2, embeddings.shape[1])
            projected = PCA(n_components=comps, random_state=random_state).fit_transform(
                embeddings
            )
            if comps == 1:
                coords = np.column_stack([projected[:, 0], np.zeros_like(projected[:, 0])])
            else:
                coords = projected

    topic_payloads = []
    for i, topic_id in enumerate(topic_ids):
        per_sizes = [
            per_corpus_topic_counts[j].get(topic_id, 0)
            for j in range(len(per_corpus_topic_counts))
        ]
        topic_payloads.append(
            {
                "id": topic_id,
                "label": labels[i] if i < len(labels) else f"Topic {topic_id}",
                "representative_words": representative_words_by_topic[i]
                if i < len(representative_words_by_topic)
                else [],
                "size": per_sizes,
                "total_size": int(sum(per_sizes)),
                "x": float(coords[i, 0]) if i < len(coords) else 0.0,
                "y": float(coords[i, 1]) if i < len(coords) else 0.0,
            }
        )

    pl.DataFrame(
        {
            TOPIC_COLUMN: topic_ids,
            TOPIC_MEANING_COLUMN: representative_words_by_topic,
        },
        schema={
            TOPIC_COLUMN: pl.Int64,
            TOPIC_MEANING_COLUMN: pl.List(pl.String),
        },
    ).lazy().sink_parquet(topic_meanings_path)

    artifacts: dict[str, Any] = {
        "version": 1,
        "topic_meanings_parquet_path": str(topic_meanings_path),
        "nodes": node_artifacts,
    }
    if existing_artifacts is not None:
        exact_artifact_path = existing_artifacts.get("exact_reduction_artifact_path")
        if isinstance(exact_artifact_path, str) and exact_artifact_path:
            artifacts["exact_reduction_artifact_path"] = exact_artifact_path
            artifacts["version"] = max(2, int(existing_artifacts.get("version") or 1))
        # Carry forward paths superseded by earlier re-aggregations so they
        # remain in the manifest tree and are reclaimed by task cleanup.
        prior_superseded = existing_artifacts.get("superseded_artifacts")
        carried = (
            [item for item in prior_superseded if isinstance(item, dict)]
            if isinstance(prior_superseded, list)
            else []
        )
        combined = carried + newly_superseded
        if combined:
            artifacts["superseded_artifacts"] = combined

    return {
        "topics": topic_payloads,
        "corpus_sizes": corpus_sizes,
        "per_corpus_topic_counts": per_corpus_topic_counts,
        "artifacts": artifacts,
        "meta": {
            "embeddings_from_ctfidf": bool(c_tfidf_used),
            "total_topics_incl_outlier": int(topic_freq.height),
        },
    }


def reaggregate_exact_topic_modeling_result(
    *,
    artifact_path: str,
    existing_artifacts: dict[str, Any],
    node_infos: list[Dict[str, Any]],
    topic_size_value: int,
    representative_words_count: int,
    random_seed: int,
) -> dict[str, Any]:
    stored = _load_exact_reduction_artifact(artifact_path)
    topic_model = stored.get("topic_model")
    all_docs = stored.get("all_docs")
    corpus_sizes = stored.get("corpus_sizes")
    active_corpora_indices = stored.get("active_corpora_indices")

    if topic_model is None or not isinstance(all_docs, list):
        raise ValueError("Exact topic reduction artifact is incomplete")
    if not isinstance(corpus_sizes, list) or not isinstance(active_corpora_indices, list):
        raise ValueError("Exact topic reduction artifact is missing corpus metadata")

    raw_total_topics = _count_non_outlier_topics(topic_model)
    requested_topic_count = int(topic_size_value)
    if raw_total_topics < 2:
        raise ValueError("Exact topic reduction requires at least two raw topics")
    if requested_topic_count < 2 or requested_topic_count > raw_total_topics:
        raise ValueError(
            f"Exact topic count must be between 2 and {raw_total_topics}"
        )

    topic_model.reduce_topics(
        all_docs,
        nr_topics=_resolve_exact_reduce_topics_target(topic_model, requested_topic_count),
    )
    payload = _build_topic_result_payload(
        topic_model=topic_model,
        node_infos=node_infos,
        all_docs=all_docs,
        corpus_sizes=[int(size) for size in corpus_sizes],
        active_corpora_indices=[list(map(int, indices)) for indices in active_corpora_indices],
        max_representative_words=max(1, int(representative_words_count)),
        random_state=int(random_seed),
        existing_artifacts=existing_artifacts,
    )
    payload_meta = payload.get("meta")
    if not isinstance(payload_meta, dict):
        payload_meta = {}
    payload_meta["raw_total_topics"] = raw_total_topics
    payload["meta"] = payload_meta
    return payload


def _encode_embeddings_in_chunks(
    embedder: Any,
    docs: list[str],
    *,
    chunk_size: int = _EMBEDDING_CHUNK_SIZE,
    progress_callback: Optional[Callable[[float, str], None]] = None,
    progress_start: float = 0.08,
    progress_end: float = 0.88,
    docs_offset: int = 0,
    total_docs_for_display: int = 0,
    report_every: int = 10,
):
    effective_chunk_size = max(1, int(chunk_size or 0))
    chunk_embeddings: list[Any] = []
    n_chunks = max(1, (len(docs) + effective_chunk_size - 1) // effective_chunk_size)
    total_display = total_docs_for_display or len(docs)

    for chunk_idx, start in enumerate(range(0, len(docs), effective_chunk_size)):
        chunk = docs[start : start + effective_chunk_size]
        chunk_embeddings.append(embedder.encode(chunk, show_progress_bar=False))

        if progress_callback and (chunk_idx + 1) % report_every == 0:
            done_docs = docs_offset + min(start + effective_chunk_size, len(docs))
            cb_frac = progress_start + ((chunk_idx + 1) / n_chunks) * (progress_end - progress_start)
            pct = int(done_docs / total_display * 100) if total_display > 0 else 0
            progress_callback(
                cb_frac,
                f"Embedding documents... ({done_docs:,} / {total_display:,},  {pct}%)",
            )

    if len(chunk_embeddings) == 1:
        return chunk_embeddings[0]

    import numpy as np

    normalized_chunks = [np.asarray(chunk) for chunk in chunk_embeddings]
    return np.concatenate(normalized_chunks, axis=0)


def _embed_with_cache(
    embedder: Any,
    docs: list[str],
    cache_dir: str | None,
    progress_callback: Optional[Callable[[float, str], None]],
    progress_start: float = 0.08,
    progress_end: float = 0.88,
    cache_model_id: str | None = None,
) -> Any:
    """Encode docs using the embedder, reading from / writing to disk cache.

    When cache_dir is None the cache is bypassed and all docs are encoded
    directly (preserves the pre-Phase-2 behaviour for callers that don't
    pass a cache dir).

    ``cache_model_id`` keys the on-disk cache so two different embedders
    (e.g. EN MiniLM-L6 vs multilingual MiniLM-L12) don't collide on a
    shared cache directory. Defaults to the English embedder label for
    callers that haven't been migrated yet.
    """
    import numpy as np

    if cache_dir is None:
        if progress_callback:
            progress_callback(progress_start, f"Embedding {len(docs):,} documents...")
        return _encode_embeddings_in_chunks(
            embedder,
            docs,
            progress_callback=progress_callback,
            progress_start=progress_start,
            progress_end=progress_end,
            total_docs_for_display=len(docs),
        )

    from pathlib import Path

    from .embedding_cache import EmbeddingCache

    cache = EmbeddingCache(
        cache_dir=Path(cache_dir),
        # Include revision in the cache key so a bumped embedder version
        # writes to a fresh cache file rather than reusing stale embeddings.
        model_id=cache_model_id
        or _embedder_cache_label(_TOPIC_EMBEDDER_REPO_ID, _TOPIC_EMBEDDER_REVISION),
        provider_id=getattr(embedder, "provider", "cpu"),
    )

    cached_embeds, missing_idx = cache.lookup(docs)

    n_cached = len(docs) - len(missing_idx)
    logger.info(
        "[Worker %d] embedding cache: %d/%d hits, %d misses",
        os.getpid(),
        n_cached,
        len(docs),
        len(missing_idx),
    )

    if not missing_idx:
        if progress_callback:
            progress_callback(progress_end, f"All {len(docs):,} embeddings loaded from cache.")
        return cached_embeds

    if progress_callback:
        progress_callback(
            progress_start,
            f"Embedding {len(missing_idx):,} new documents "
            f"({n_cached:,} loaded from cache)...",
        )

    missed_docs = [docs[i] for i in missing_idx]
    new_embeds = _encode_embeddings_in_chunks(
        embedder,
        missed_docs,
        progress_callback=progress_callback,
        progress_start=progress_start,
        progress_end=progress_end,
        docs_offset=n_cached,
        total_docs_for_display=len(docs),
    )

    cache.store(missed_docs, new_embeds)

    # Reassemble: fill newly computed embeddings into the pre-allocated array.
    dim = new_embeds.shape[1]
    if cached_embeds.shape[1] != dim:
        # First ever run — cached_embeds was zero-width placeholder
        result = np.zeros((len(docs), dim), dtype=np.float32)
        for idx, emb_row in zip(missing_idx, new_embeds):
            result[idx] = emb_row
        # Rows NOT in missing_idx were already cached — re-fetch after store
        # since cached_embeds had wrong width on first run.
        hit_idx = [i for i in range(len(docs)) if i not in set(missing_idx)]
        if hit_idx:
            cached_embeds2, _ = cache.lookup(docs)
            for i in hit_idx:
                result[i] = cached_embeds2[i]
    else:
        result = cached_embeds.copy()
        for slot, emb_row in zip(missing_idx, new_embeds):
            result[slot] = emb_row

    if progress_callback:
        progress_callback(progress_end, "Embedding complete.")

    return result


def run_topic_modeling_task(
    configure_worker_environment,
    user_id: str,
    workspace_id: str,
    node_infos: list[Dict[str, Any]],
    artifact_dir: str,
    artifact_prefix: str,
    min_topic_size: int = 5,
    workspace_dir: str | None = None,
    corpora: list[list[str]] | None = None,
    random_seed: int = 42,
    representative_words_count: int = 5,
    progress_callback: Optional[Callable[[float, str], None]] = None,
    embedding_cache_dir: str | None = None,
    force_mode: str | None = None,
    n_clusters: int | None = None,
    sample_fractions: list[float | None] | None = None,
    topic_size_mode: str | None = "target",
    topic_size_value: int | None = 25,
    language: str | None = None,
) -> Dict[str, Any]:
    """Execute topic modeling in a worker process.

    Used by:
    - `core.worker.topic_modeling_task`
    - `TASK_REGISTRY["topic_modeling"]`

        Why:
        - Runs BERTopic embedding/modeling out-of-process and returns an artifact
            manifest (Parquet outputs) for main-process lazy retrieval/finalization.
    """
    configure_worker_environment()

    try:
        if progress_callback:
            progress_callback(
                0.01,
                "Loading topic modeling resources. First runs may download model files...",
            )

        from pathlib import Path

        import numpy as np
        import polars as pl
        from bertopic import BERTopic
        from bertopic._utils import select_topic_representation

        logger.info(
            "[Worker %d] Starting topic modeling task for workspace %s",
            os.getpid(),
            workspace_id,
        )

        def _load_corpora_from_workspace(
            target_workspace_dir: str, node_payloads: list[Dict[str, Any]]
        ) -> list[list[str]]:
            from docworkspace import Workspace

            workspace = Workspace.load(Path(target_workspace_dir))
            loaded_corpora: list[list[str]] = []

            for node_info in node_payloads:
                node_id = str(node_info.get("node_id") or "")
                text_column = str(node_info.get("text_column") or "")
                if not node_id or not text_column:
                    raise ValueError(
                        "Topic modeling requires node_id and text_column for each node"
                    )

                try:
                    node = workspace.nodes[node_id]
                except KeyError as exc:
                    raise ValueError(
                        f"Topic modeling node {node_id} is missing from workspace"
                    ) from exc

                selected = cast(
                    pl.DataFrame,
                    node.data.select(pl.col(text_column).alias("__doc_col__")).collect(),
                )
                loaded_corpora.append(
                    [
                        str(value) if value is not None else ""
                        for value in selected["__doc_col__"].to_list()
                    ]
                )

            return loaded_corpora

        artifact_root = Path(artifact_dir)
        artifact_root.mkdir(parents=True, exist_ok=True)

        if corpora is None:
            if workspace_dir is None:
                raise ValueError(
                    "Topic modeling requires corpora or a workspace_dir to load them"
                )
            if progress_callback:
                progress_callback(0.03, "Loading source documents from workspace...")
            corpora = _load_corpora_from_workspace(workspace_dir, node_infos)

        if len(corpora) != len(node_infos):
            raise ValueError(
                "Topic modeling payload mismatch: corpora and node_infos lengths differ"
            )

        if progress_callback:
            progress_callback(0.05, "Preparing topic modeling payload...")

        node_names = [
            str(info.get("node_name") or info.get("node_id") or "node")
            for info in node_infos
        ]

        if progress_callback:
            progress_callback(0.07, "Loading embedding model...")

        def _compute_topics() -> dict[str, Any]:
            corpus_sizes_before_sample = [len(corpus) for corpus in corpora]
            active_corpora: list[list[str]] = []
            # original row indices for each corpus — used to write correct __row_nr__
            # values in the assignments parquet so detach joins back to the right rows.
            active_corpora_indices: list[list[int]] = []
            if sample_fractions is not None:
                for _i, _corpus in enumerate(corpora):
                    _frac = sample_fractions[_i] if _i < len(sample_fractions) else None
                    if _frac is not None and 0.0 < _frac < 1.0:
                        _sampled_docs, _sampled_idx = _sample_corpus(_corpus, _frac, random_seed + _i)
                        active_corpora.append(_sampled_docs)
                        active_corpora_indices.append(_sampled_idx)
                    else:
                        active_corpora.append(_corpus)
                        active_corpora_indices.append(list(range(len(_corpus))))
            else:
                active_corpora = list(corpora)
                active_corpora_indices = [list(range(len(c))) for c in corpora]

            all_docs = [doc for corpus in active_corpora for doc in corpus]
            corpus_sizes = [len(corpus) for corpus in active_corpora]
            if not all_docs:
                topic_meanings_path = (
                    artifact_root / f"{artifact_prefix}_topic_meanings.parquet"
                )
                pl.DataFrame(
                    schema={
                        TOPIC_COLUMN: pl.Int64,
                        TOPIC_MEANING_COLUMN: pl.List(pl.String),
                    }
                ).lazy().sink_parquet(topic_meanings_path)

                node_artifacts: list[dict[str, Any]] = []
                for idx, corpus in enumerate(active_corpora):
                    node_id = str(node_infos[idx]["node_id"])
                    node_name = str(node_infos[idx].get("node_name") or node_id)
                    text_column = str(node_infos[idx].get("text_column") or "")
                    original_columns = list(
                        node_infos[idx].get("original_columns") or []
                    )
                    assignments_path = (
                        artifact_root
                        / f"{artifact_prefix}_topic_assignments_{node_id}.parquet"
                    )
                    pl.DataFrame(
                        {
                            "__row_nr__": active_corpora_indices[idx],
                            TOPIC_COLUMN: [],
                        }
                    ).with_columns(
                        [
                            pl.col("__row_nr__").cast(pl.Int64),
                            pl.col(TOPIC_COLUMN).cast(pl.Int64),
                        ]
                    ).lazy().sink_parquet(assignments_path)
                    node_artifacts.append(
                        {
                            "node_id": node_id,
                            "node_name": node_name,
                            "text_column": text_column,
                            "original_columns": original_columns,
                            "assignments_parquet_path": str(assignments_path),
                        }
                    )

                return {
                    "topics": [],
                    "corpus_sizes": corpus_sizes,
                    "per_corpus_topic_counts": [],
                    "artifacts": {
                        "version": 1,
                        "topic_meanings_parquet_path": str(topic_meanings_path),
                        "nodes": node_artifacts,
                    },
                    "meta": {},
                }

            if any(size == 0 for size in corpus_sizes):
                raise ValueError("All corpora must contain at least one document.")

            random_state = int(random_seed)
            max_representative_words = max(1, int(representative_words_count))
            # Phase 3.1: the displayed embedder name reflects the language-
            # routed model, so the result panel shows e.g.
            # "paraphrase-multilingual-MiniLM-L12-v2" on a ZH run.
            embedding_model_name = _select_embedder(language)[0].split("/")[-1]

            random.seed(random_state)
            np.random.seed(random_state)

            n_eff = len(all_docs)
            effective_min_topic_size = _compute_min_topic_size(
                n_eff, topic_size_mode or "target", topic_size_value or 25
            )

            # Determine pipeline mode before embedding so progress fractions
            # reflect actual time distribution (embedding dominates for large corpora).
            use_online = _should_use_online_pipeline(all_docs, force_mode)
            pipeline_mode = "online" if use_online else "classic"
            logger.info(
                "[Worker %d] Pipeline mode: %s (%d docs)",
                os.getpid(),
                pipeline_mode,
                len(all_docs),
            )

            # Online: embedding ~92% of wall time → 80% of bar (0.08–0.88)
            # Classic: embedding ~50% of wall time → 55% of bar (0.08–0.63)
            # Cluster stage is opaque (no intra-UMAP callbacks); give it remaining bar before writing.
            embed_start = 0.08
            embed_end = 0.88 if use_online else 0.63
            cluster_frac = 0.89 if use_online else 0.65

            embedder_repo_id, embedder_revision = _select_embedder(language)
            embedder = _get_embedder(embedder_repo_id, embedder_revision)
            embedding_backend = (
                "mps" if getattr(embedder, "provider", "").upper() == "MPS" else "onnx"
            )
            all_embeddings = _embed_with_cache(
                embedder, all_docs, embedding_cache_dir, progress_callback,
                progress_start=embed_start, progress_end=embed_end,
                cache_model_id=_embedder_cache_label(
                    embedder_repo_id, embedder_revision
                ),
            )

            top_n_words = _resolve_top_n_words(representative_words_count)
            if use_online:
                if progress_callback:
                    progress_callback(
                        cluster_frac,
                        f"Running online pipeline for {len(all_docs):,} documents "
                        f"(IncrementalPCA + MiniBatchKMeans)...",
                    )
                topic_model, actual_k = _build_online_pipeline(
                    len(all_docs),
                    n_clusters,
                    random_state,
                    embedder,
                    language=language,
                    top_n_words=top_n_words,
                )
            else:
                if progress_callback:
                    progress_callback(
                        cluster_frac, "Running classic BERTopic pipeline (UMAP + HDBSCAN)..."
                    )
                topic_model = _build_classic_pipeline(
                    effective_min_topic_size,
                    random_state,
                    embedder,
                    top_n_words=top_n_words,
                )
                actual_k = None

            assigned_topics, _ = topic_model.fit_transform(all_docs, all_embeddings)

            raw_total_topics = None
            exact_reduction_artifact_path: str | None = None

            if (topic_size_mode or "target") == "exact" and topic_size_value:
                raw_total_topics = _count_non_outlier_topics(topic_model)
                exact_reduction_artifact_path = str(
                    artifact_root / f"{artifact_prefix}_exact_reduction.pkl"
                )
                _persist_exact_reduction_artifact(
                    exact_reduction_artifact_path,
                    topic_model=topic_model,
                    all_docs=all_docs,
                    corpus_sizes=corpus_sizes,
                    active_corpora_indices=active_corpora_indices,
                )
                if progress_callback:
                    progress_callback(
                        cluster_frac,
                        f"Reducing topics to exactly {topic_size_value}...",
                    )
                topic_model.reduce_topics(
                    all_docs,
                    nr_topics=_resolve_exact_reduce_topics_target(
                        topic_model, int(topic_size_value)
                    ),
                )
                assigned_topics = list(topic_model.topics_)
            payload = _build_topic_result_payload(
                topic_model=topic_model,
                node_infos=node_infos,
                all_docs=all_docs,
                corpus_sizes=corpus_sizes,
                active_corpora_indices=active_corpora_indices,
                max_representative_words=max_representative_words,
                random_state=random_state,
                assigned_topics=assigned_topics,
                artifact_prefix=artifact_prefix,
                artifact_root=artifact_root,
            )
            payload_meta = payload.get("meta")
            if not isinstance(payload_meta, dict):
                payload_meta = {}
            payload_meta.update(
                {
                    "native": True,
                    "engine": "bertopic",
                    "embedding_model": embedding_model_name,
                    "embedding_backend": embedding_backend,
                    "min_topic_size": effective_min_topic_size,
                    "topic_size_mode": topic_size_mode or "target",
                    "topic_size_value": topic_size_value,
                    "representative_words_count": max_representative_words,
                    "random_state": random_state,
                    "pipeline_mode": pipeline_mode,
                    **({"n_clusters": actual_k} if actual_k is not None else {}),
                    **(
                        {
                            "corpus_sizes_before_sample": corpus_sizes_before_sample,
                            "corpus_sizes_after_sample": corpus_sizes,
                        }
                        if sample_fractions is not None
                        else {}
                    ),
                }
            )
            if raw_total_topics is not None:
                payload_meta["raw_total_topics"] = raw_total_topics
            payload["meta"] = payload_meta
            payload_artifacts = payload.get("artifacts")
            if isinstance(payload_artifacts, dict) and exact_reduction_artifact_path:
                payload_artifacts["exact_reduction_artifact_path"] = exact_reduction_artifact_path
                payload_artifacts["version"] = 2
            return payload

        tv = _compute_topics()

        if progress_callback:
            progress_callback(0.9, "Writing topic-modeling results...")

        result = {
            "topics": tv["topics"],
            "corpus_sizes": tv["corpus_sizes"],
            "per_corpus_topic_counts": tv.get("per_corpus_topic_counts"),
            "artifacts": tv.get("artifacts", {"version": 1, "nodes": []}),
            "meta": {**tv.get("meta", {}), "node_names": node_names},
        }

        if progress_callback:
            progress_callback(1.0, "Topic modeling completed")

        logger.info("[Worker %d] Topic modeling completed successfully", os.getpid())
        return result

    except Exception as e:
        logger.error("[Worker %d] Topic modeling failed: %s", os.getpid(), e)
        if progress_callback:
            progress_callback(-1, f"Failed: {str(e)}")
        raise
