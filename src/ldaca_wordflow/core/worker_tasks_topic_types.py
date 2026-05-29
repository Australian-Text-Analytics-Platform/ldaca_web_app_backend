"""Internal type definitions for the topic-modeling worker pipeline.

Used by:
- Other ``worker_tasks_topic_*`` sub-modules and
  ``worker_tasks_topic`` itself.

All types are frozen dataclasses so partial data snapshots are safe to
thread between pipeline stages.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class _PreparedTopicPayload:
    """Internal data bundle used by topic-modeling worker pipeline for prepared topic payload.

    Called by:
    - ``_prepare_payload`` in ``worker_tasks_topic`` builds and returns one.
    - ``run_topic_modeling_task`` destructures it before calling
      ``_compute_topic_payload``.

    Flow: load workspace corpora, choose sampling and embedding settings, reuse embedding
        caches when possible, build topic payloads, and report artifacts back to the task
        manager.
    """

    artifact_root: Path
    corpora: list[list[str]]
    vectorizer_corpora: list[list[str] | None]
    tokens_columns_per_node: list[str | None]
    node_names: list[str]


@dataclass(frozen=True)
class _SampledTopicCorpora:
    """Internal data bundle used by topic-modeling worker pipeline for sampled topic corpora.

    Called by:
    - ``_sample_corpora_for_topic_modeling`` builds and returns one.
    - ``_compute_topic_payload`` and ``_build_empty_topic_payload`` consume it.

    Flow: load workspace corpora, choose sampling and embedding settings, reuse embedding
        caches when possible, build topic payloads, and report artifacts back to the task
        manager.
    """

    corpus_sizes_before_sample: list[int]
    active_corpora: list[list[str]]
    active_corpora_indices: list[list[int]]
    active_vectorizer_corpora: list[list[str] | None]
    all_docs: list[str]
    all_docs_for_vectorizer: list[str]
    any_pretokenised: bool
    corpus_sizes: list[int]


@dataclass(frozen=True)
class _EmbeddedTopicDocuments:
    """Internal data bundle used by topic-modeling worker pipeline for embedded topic documents.

    Called by:
    - ``_embed_documents`` builds and returns one.
    - ``_compute_topic_payload`` reads the embedding arrays and model metadata.

    Flow: load workspace corpora, choose sampling and embedding settings, reuse embedding
        caches when possible, build topic payloads, and report artifacts back to the task
        manager.
    """

    embedder: Any
    all_embeddings: Any
    embedding_model_name: str
    embedding_backend: str


@dataclass(frozen=True)
class _TopicPipelineRun:
    """Internal data bundle used by topic-modeling worker pipeline for topic pipeline run.

    Called by:
    - ``_run_classic_pipeline`` builds and returns one.
    - ``_compute_topic_payload`` destructures it to read the fitted model and
      topic assignments.

    Flow: load workspace corpora, choose sampling and embedding settings, reuse embedding
        caches when possible, build topic payloads, and report artifacts back to the task
        manager.
    """

    topic_model: Any
    assigned_topics: list[int]
