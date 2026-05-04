import sys
from pathlib import Path
from types import ModuleType
from typing import Any, cast

import numpy as np
import pandas as pd
import polars as pl

from ldaca_web_app.core import worker_tasks_topic


def test_run_topic_modeling_task_emits_representative_words_as_list_string(
    tmp_path, monkeypatch
):
    progress_updates: list[tuple[float, str]] = []

    class FakeEmbedder:
        def encode(self, docs, show_progress_bar=False):
            assert show_progress_bar is False
            return np.array([[0.1, 0.2] for _ in docs], dtype=float)

    class FakeBERTopic:
        def __init__(self, *, verbose, min_topic_size, embedding_model, umap_model):
            assert verbose is False
            assert min_topic_size == 2
            assert embedding_model is not None
            assert umap_model is not None
            self.c_tf_idf_ = np.array([[1.0, 0.5]], dtype=float)
            self.topic_embeddings_ = np.array([[0.2, 0.4]], dtype=float)

        def fit_transform(self, docs, embeddings):
            assert docs == ["doc one", "doc two"]
            assert embeddings.shape == (2, 2)
            return [0, 0], None

        def get_topic_freq(self):
            return pd.DataFrame({"Topic": [0, -1], "Count": [2, 0]})

        def get_topic(self, topic_id):
            assert topic_id == 0
            return [("alpha", 0.9), ("beta", 0.8), ("gamma", 0.7)]

        def get_topics(self):
            return {
                -1: [],
                0: self.get_topic(0),
            }

    def fake_select_topic_representation(*_args, **_kwargs):
        return np.array([[0.0, 0.0], [1.0, 2.0]], dtype=float), False

    bertopic_module = cast(Any, ModuleType("bertopic"))
    bertopic_module.BERTopic = FakeBERTopic
    bertopic_utils_module = cast(Any, ModuleType("bertopic._utils"))
    bertopic_utils_module.select_topic_representation = fake_select_topic_representation

    monkeypatch.setattr(
        worker_tasks_topic, "_get_embedder", lambda _name: FakeEmbedder()
    )
    monkeypatch.setitem(sys.modules, "bertopic", bertopic_module)
    monkeypatch.setitem(sys.modules, "bertopic._utils", bertopic_utils_module)

    result = worker_tasks_topic.run_topic_modeling_task(
        configure_worker_environment=lambda: None,
        user_id="test-user",
        workspace_id="test-workspace",
        corpora=[["doc one", "doc two"]],
        node_infos=[
            {
                "node_id": "node-1",
                "node_name": "Node 1",
                "text_column": "document",
                "original_columns": ["document"],
            }
        ],
        artifact_dir=str(tmp_path),
        artifact_prefix="topic_modeling_test",
        min_topic_size=2,
        progress_callback=lambda progress, message: progress_updates.append(
            (
                progress,
                message,
            )
        ),
    )

    meanings = pl.read_parquet(tmp_path / "topic_modeling_test_topic_meanings.parquet")
    assignments = pl.read_parquet(
        tmp_path / "topic_modeling_test_topic_assignments_node-1.parquet"
    )

    assert assignments.columns == ["__row_nr__", "TOPIC_topic"]
    assert meanings.schema["TOPIC_topic_meaning"] == pl.List(pl.String)
    assert meanings.to_dicts() == [
        {
            "TOPIC_topic": 0,
            "TOPIC_topic_meaning": ["alpha", "beta", "gamma"],
        }
    ]
    assert result["topics"][0]["representative_words"] == ["alpha", "beta", "gamma"]
    assert result["topics"][0]["label"] == "alpha | beta | gamma"
    assert progress_updates[0][1].startswith("Loading topic modeling")
    assert any(
        "Embedding" in message or "pipeline" in message.lower()
        for _progress, message in progress_updates
    )
    assert progress_updates[-1] == (1.0, "Topic modeling completed")
    assert progress_updates[-1] == (1.0, "Topic modeling completed")


def test_run_topic_modeling_task_can_load_corpora_from_workspace(
    tmp_path, monkeypatch
):
    class FakeEmbedder:
        def encode(self, docs, show_progress_bar=False):
            assert docs == ["doc one", "doc two"]
            assert show_progress_bar is False
            return np.array([[0.1, 0.2] for _ in docs], dtype=float)

    class FakeBERTopic:
        def __init__(self, *, verbose, min_topic_size, embedding_model, umap_model):
            assert verbose is False
            assert min_topic_size == 2
            assert embedding_model is not None
            assert umap_model is not None
            self.c_tf_idf_ = np.array([[1.0, 0.5]], dtype=float)
            self.topic_embeddings_ = np.array([[0.2, 0.4]], dtype=float)

        def fit_transform(self, docs, embeddings):
            assert docs == ["doc one", "doc two"]
            assert embeddings.shape == (2, 2)
            return [0, 0], None

        def get_topic_freq(self):
            return pd.DataFrame({"Topic": [0, -1], "Count": [2, 0]})

        def get_topic(self, topic_id):
            assert topic_id == 0
            return [("alpha", 0.9), ("beta", 0.8)]

        def get_topics(self):
            return {
                -1: [],
                0: self.get_topic(0),
            }

    class FakeWorkspace:
        def __init__(self):
            self.nodes = {
                "node-1": cast(
                    Any,
                    type(
                        "FakeNode",
                        (),
                        {"data": pl.DataFrame({"document": ["doc one", "doc two"]}).lazy()},
                    )(),
                )
            }

        @classmethod
        def load(cls, path):
            assert Path(path) == tmp_path
            return cls()

    def fake_select_topic_representation(*_args, **_kwargs):
        return np.array([[0.0, 0.0], [1.0, 2.0]], dtype=float), False

    bertopic_module = cast(Any, ModuleType("bertopic"))
    bertopic_module.BERTopic = FakeBERTopic
    bertopic_utils_module = cast(Any, ModuleType("bertopic._utils"))
    bertopic_utils_module.select_topic_representation = fake_select_topic_representation
    docworkspace_module = cast(Any, ModuleType("docworkspace"))
    docworkspace_module.Workspace = FakeWorkspace

    monkeypatch.setattr(
        worker_tasks_topic, "_get_embedder", lambda _name: FakeEmbedder()
    )
    monkeypatch.setitem(sys.modules, "bertopic", bertopic_module)
    monkeypatch.setitem(sys.modules, "bertopic._utils", bertopic_utils_module)
    monkeypatch.setitem(sys.modules, "docworkspace", docworkspace_module)

    result = worker_tasks_topic.run_topic_modeling_task(
        configure_worker_environment=lambda: None,
        user_id="test-user",
        workspace_id="test-workspace",
        workspace_dir=str(tmp_path),
        corpora=None,
        node_infos=[
            {
                "node_id": "node-1",
                "node_name": "Node 1",
                "text_column": "document",
                "original_columns": ["document"],
            }
        ],
        artifact_dir=str(tmp_path),
        artifact_prefix="topic_modeling_workspace_test",
        min_topic_size=2,
    )

    assert result["meta"]["engine"] == "bertopic"
    assert result["topics"][0]["representative_words"] == ["alpha", "beta"]


def test_encode_embeddings_in_chunks_preserves_document_order(monkeypatch):
    encode_calls: list[list[str]] = []

    class FakeEmbedder:
        def encode(self, docs, show_progress_bar=False):
            assert show_progress_bar is False
            encode_calls.append(list(docs))
            return np.array([[len(doc), index] for index, doc in enumerate(docs)], dtype=float)

    monkeypatch.setattr(worker_tasks_topic, "_EMBEDDING_CHUNK_SIZE", 2)

    embeddings = worker_tasks_topic._encode_embeddings_in_chunks(
        FakeEmbedder(),
        ["a", "bb", "ccc", "dddd", "eeeee"],
        chunk_size=worker_tasks_topic._EMBEDDING_CHUNK_SIZE,
    )

    assert encode_calls == [["a", "bb"], ["ccc", "dddd"], ["eeeee"]]
    assert embeddings.tolist() == [
        [1.0, 0.0],
        [2.0, 1.0],
        [3.0, 0.0],
        [4.0, 1.0],
        [5.0, 0.0],
    ]


# ---------------------------------------------------------------------------
# Phase 3 — Online pipeline tests
# ---------------------------------------------------------------------------


def test_should_use_online_pipeline_force_online():
    """force_mode='online' always selects the online pipeline."""
    docs = ["short", "corpus"]
    assert worker_tasks_topic._should_use_online_pipeline(docs, "online") is True


def test_should_use_online_pipeline_force_classic(monkeypatch):
    """force_mode='classic' always selects the classic pipeline, even on large corpora."""
    monkeypatch.setattr(worker_tasks_topic, "_ONLINE_THRESHOLD_DOCS", 1)
    big_docs = ["x"] * 100
    assert worker_tasks_topic._should_use_online_pipeline(big_docs, "classic") is False


def test_should_use_online_pipeline_auto_by_doc_count(monkeypatch):
    """Auto mode engages online pipeline when doc count exceeds threshold."""
    monkeypatch.setattr(worker_tasks_topic, "_ONLINE_THRESHOLD_DOCS", 10)
    monkeypatch.setattr(worker_tasks_topic, "_ONLINE_THRESHOLD_BYTES", 10 * 1024 * 1024)
    docs = ["doc"] * 11
    assert worker_tasks_topic._should_use_online_pipeline(docs, None) is True


def test_should_use_online_pipeline_auto_by_bytes(monkeypatch):
    """Auto mode engages online pipeline when total byte size exceeds threshold."""
    monkeypatch.setattr(worker_tasks_topic, "_ONLINE_THRESHOLD_DOCS", 1_000_000)
    monkeypatch.setattr(worker_tasks_topic, "_ONLINE_THRESHOLD_BYTES", 100)
    # 3 docs × 50 chars = 150 bytes > 100 byte threshold
    docs = ["x" * 50] * 3
    assert worker_tasks_topic._should_use_online_pipeline(docs, None) is True


def test_should_use_online_pipeline_small_corpus_stays_classic():
    """Small corpus below all thresholds stays on the classic pipeline."""
    docs = ["small", "corpus", "stays", "classic"]
    assert worker_tasks_topic._should_use_online_pipeline(docs, None) is False


def test_should_use_online_pipeline_auto_value_falls_through_to_threshold():
    """'auto' is not a special sentinel — threshold logic applies as with None."""
    docs = ["small", "corpus"]
    assert worker_tasks_topic._should_use_online_pipeline(docs, "auto") is False


def _make_online_fake_bertopic_cls():
    """Return a FakeBERTopic class that accepts online-pipeline kwargs."""

    class FakeBERTopicOnline:
        def __init__(self, **kwargs):
            # Online pipeline passes umap_model (IncrementalPCA) and hdbscan_model
            # (MiniBatchKMeans); accept anything via **kwargs.
            assert kwargs.get("verbose") is False
            assert kwargs.get("embedding_model") is not None
            assert "umap_model" in kwargs
            assert "hdbscan_model" in kwargs
            self.c_tf_idf_ = np.array([[1.0, 0.5]], dtype=float)
            self.topic_embeddings_ = np.array([[0.2, 0.4]], dtype=float)

        def fit_transform(self, docs, embeddings):
            return [0] * len(docs), None

        def get_topic_freq(self):
            return pd.DataFrame({"Topic": [0, -1], "Count": [2, 0]})

        def get_topics(self):
            return {-1: [], 0: [("alpha", 0.9), ("beta", 0.8)]}

    return FakeBERTopicOnline


def test_run_topic_modeling_task_online_pipeline_mode(tmp_path, monkeypatch):
    """force_mode='online' produces pipeline_mode='online' and n_clusters in meta."""

    class FakeEmbedder:
        def encode(self, docs, show_progress_bar=False):
            return np.array([[0.1, 0.2] for _ in docs], dtype=float)

    def fake_select_topic_representation(*_args, **_kwargs):
        return np.array([[0.0, 0.0], [1.0, 2.0]], dtype=float), False

    bertopic_module = cast(Any, ModuleType("bertopic"))
    bertopic_module.BERTopic = _make_online_fake_bertopic_cls()
    bertopic_utils_module = cast(Any, ModuleType("bertopic._utils"))
    bertopic_utils_module.select_topic_representation = fake_select_topic_representation

    monkeypatch.setattr(worker_tasks_topic, "_get_embedder", lambda _name: FakeEmbedder())
    monkeypatch.setitem(sys.modules, "bertopic", bertopic_module)
    monkeypatch.setitem(sys.modules, "bertopic._utils", bertopic_utils_module)

    result = worker_tasks_topic.run_topic_modeling_task(
        configure_worker_environment=lambda: None,
        user_id="test-user",
        workspace_id="test-workspace",
        corpora=[["doc one", "doc two"]],
        node_infos=[
            {
                "node_id": "node-1",
                "node_name": "Node 1",
                "text_column": "document",
                "original_columns": ["document"],
            }
        ],
        artifact_dir=str(tmp_path),
        artifact_prefix="topic_online_test",
        min_topic_size=2,
        force_mode="online",
        n_clusters=5,
    )

    assert result["meta"]["pipeline_mode"] == "online"
    assert result["meta"]["n_clusters"] == 5


def test_run_topic_modeling_task_classic_pipeline_meta(tmp_path, monkeypatch):
    """force_mode='classic' produces pipeline_mode='classic' with no n_clusters in meta."""

    class FakeEmbedder:
        def encode(self, docs, show_progress_bar=False):
            return np.array([[0.1, 0.2] for _ in docs], dtype=float)

    class FakeBERTopicClassic:
        def __init__(self, *, verbose, min_topic_size, embedding_model, umap_model):
            self.c_tf_idf_ = np.array([[1.0, 0.5]], dtype=float)
            self.topic_embeddings_ = np.array([[0.2, 0.4]], dtype=float)

        def fit_transform(self, docs, embeddings):
            return [0] * len(docs), None

        def get_topic_freq(self):
            return pd.DataFrame({"Topic": [0, -1], "Count": [2, 0]})

        def get_topics(self):
            return {-1: [], 0: [("alpha", 0.9), ("beta", 0.8)]}

    def fake_select_topic_representation(*_args, **_kwargs):
        return np.array([[0.0, 0.0], [1.0, 2.0]], dtype=float), False

    bertopic_module = cast(Any, ModuleType("bertopic"))
    bertopic_module.BERTopic = FakeBERTopicClassic
    bertopic_utils_module = cast(Any, ModuleType("bertopic._utils"))
    bertopic_utils_module.select_topic_representation = fake_select_topic_representation

    monkeypatch.setattr(worker_tasks_topic, "_get_embedder", lambda _name: FakeEmbedder())
    monkeypatch.setitem(sys.modules, "bertopic", bertopic_module)
    monkeypatch.setitem(sys.modules, "bertopic._utils", bertopic_utils_module)

    result = worker_tasks_topic.run_topic_modeling_task(
        configure_worker_environment=lambda: None,
        user_id="test-user",
        workspace_id="test-workspace",
        corpora=[["doc one", "doc two"]],
        node_infos=[
            {
                "node_id": "node-1",
                "node_name": "Node 1",
                "text_column": "document",
                "original_columns": ["document"],
            }
        ],
        artifact_dir=str(tmp_path),
        artifact_prefix="topic_classic_test",
        min_topic_size=2,
        force_mode="classic",
    )

    assert result["meta"]["pipeline_mode"] == "classic"
    assert "n_clusters" not in result["meta"]
