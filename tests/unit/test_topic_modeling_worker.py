import sys
from pathlib import Path
from types import ModuleType
from typing import Any, cast

import numpy as np
import pandas as pd
import polars as pl
import pytest
from ldaca_wordflow.core import (
    worker_tasks_topic,
    worker_tasks_topic_embedding,
    worker_tasks_topic_result,
)


@pytest.fixture(autouse=True)
def _stub_umap(monkeypatch):
    # Linux Python 3.14 + PyYAML 6.0.3 has a partial-init bug: importing real
    # umap pulls in numba → yaml → AttributeError on yaml.error.  None of these
    # tests exercise UMAP behaviour, so swap in a fake module that only
    # provides a UMAP class with the constructor signature we use.
    fake = cast(Any, ModuleType("umap"))

    class FakeUMAP:
        def __init__(self, **_kwargs):
            pass

        def fit_transform(self, X):
            return X

    fake.UMAP = FakeUMAP
    monkeypatch.setitem(sys.modules, "umap", fake)


def test_run_topic_modeling_task_emits_representative_words_as_list_string(
    tmp_path, monkeypatch
):
    progress_updates: list[tuple[float, str]] = []

    class FakeEmbedder:
        def encode(self, docs, show_progress_bar=False):
            assert show_progress_bar is False
            return np.array([[0.1, 0.2] for _ in docs], dtype=float)

    class FakeBERTopic:
        def __init__(
            self,
            *,
            verbose,
            min_topic_size,
            embedding_model,
            umap_model,
            vectorizer_model=None,
            language=None,
            top_n_words=10,
        ):
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
        worker_tasks_topic_embedding, "_get_embedder", lambda *args, **kwargs: FakeEmbedder()
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


def test_run_topic_modeling_task_payload_carries_full_top_n_words(
    tmp_path, monkeypatch
):
    """Wire payload exposes the full top_n_words buffer, parquet meaning
    column respects the user's requested count.

    Regression: without this, "Words per topic = 15" silently clipped the
    payload to 15, so dialing the frontend slider up to 30 post-fit had
    nothing to reveal — even though BERTopic was fit with top_n_words=50.
    """

    class FakeEmbedder:
        def encode(self, docs, show_progress_bar=False):
            return np.array([[0.1, 0.2] for _ in docs], dtype=float)

    full_words = [(f"w{i}", 1.0 - i * 0.01) for i in range(50)]

    class FakeBERTopic:
        def __init__(
            self,
            *,
            verbose,
            min_topic_size,
            embedding_model,
            umap_model,
            vectorizer_model=None,
            language=None,
            top_n_words=10,
        ):
            # Confirm the fit honoured _resolve_top_n_words(15) == 50.
            assert top_n_words == 50
            self.c_tf_idf_ = np.array([[1.0, 0.5]], dtype=float)
            self.topic_embeddings_ = np.array([[0.2, 0.4]], dtype=float)

        def fit_transform(self, docs, embeddings):
            return [0] * len(docs), None

        def get_topic_freq(self):
            return pd.DataFrame({"Topic": [0, -1], "Count": [2, 0]})

        def get_topics(self):
            return {-1: [], 0: full_words}

    def fake_select_topic_representation(*_args, **_kwargs):
        return np.array([[0.0, 0.0], [1.0, 2.0]], dtype=float), False

    bertopic_module = cast(Any, ModuleType("bertopic"))
    bertopic_module.BERTopic = FakeBERTopic
    bertopic_utils_module = cast(Any, ModuleType("bertopic._utils"))
    bertopic_utils_module.select_topic_representation = fake_select_topic_representation

    monkeypatch.setattr(
        worker_tasks_topic_embedding, "_get_embedder", lambda *args, **kwargs: FakeEmbedder()
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
        artifact_prefix="topic_full_payload_test",
        min_topic_size=2,
        representative_words_count=15,
    )

    # Wire payload carries the full 50-word buffer so the frontend slider
    # can scale up to max(50, 2*15)=50 without re-fitting.
    assert len(result["topics"][0]["representative_words"]) == 50
    assert result["topics"][0]["representative_words"][:3] == ["w0", "w1", "w2"]

    # Meaning column (workspace attach) still respects the user's 15.
    meanings = pl.read_parquet(
        tmp_path / "topic_full_payload_test_topic_meanings.parquet"
    )
    meaning_row = meanings.to_dicts()[0]
    assert len(meaning_row["TOPIC_topic_meaning"]) == 15
    assert meaning_row["TOPIC_topic_meaning"] == [f"w{i}" for i in range(15)]


def test_run_topic_modeling_task_can_load_corpora_from_workspace(tmp_path, monkeypatch):
    class FakeEmbedder:
        def encode(self, docs, show_progress_bar=False):
            assert docs == ["doc one", "doc two"]
            assert show_progress_bar is False
            return np.array([[0.1, 0.2] for _ in docs], dtype=float)

    class FakeBERTopic:
        def __init__(
            self,
            *,
            verbose,
            min_topic_size,
            embedding_model,
            umap_model,
            vectorizer_model=None,
            language=None,
            top_n_words=10,
        ):
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
                        {
                            "data": pl.DataFrame(
                                {"document": ["doc one", "doc two"]}
                            ).lazy(),
                            "find_tokenization_column": lambda self, _source, *, model=None: (
                                None
                            ),
                        },
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
        worker_tasks_topic_embedding, "_get_embedder", lambda *args, **kwargs: FakeEmbedder()
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
            return np.array(
                [[len(doc), index] for index, doc in enumerate(docs)], dtype=float
            )

    monkeypatch.setattr(worker_tasks_topic_embedding, "_EMBEDDING_CHUNK_SIZE", 2)
    embeddings = worker_tasks_topic_embedding._encode_embeddings_in_chunks(
        FakeEmbedder(),
        ["a", "bb", "ccc", "dddd", "eeeee"],
        chunk_size=worker_tasks_topic_embedding._EMBEDDING_CHUNK_SIZE,
    )

    assert encode_calls == [["a", "bb"], ["ccc", "dddd"], ["eeeee"]]
    assert embeddings.tolist() == [
        [1.0, 0.0],
        [2.0, 1.0],
        [3.0, 0.0],
        [4.0, 1.0],
        [5.0, 0.0],
    ]


def test_run_topic_modeling_task_classic_pipeline_meta(tmp_path, monkeypatch):
    """Topic modeling uses the classic BERTopic pipeline."""

    class FakeEmbedder:
        def encode(self, docs, show_progress_bar=False):
            return np.array([[0.1, 0.2] for _ in docs], dtype=float)

    class FakeBERTopicClassic:
        def __init__(
            self,
            *,
            verbose,
            min_topic_size,
            embedding_model,
            umap_model,
            vectorizer_model=None,
            language=None,
            top_n_words=10,
        ):
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

    monkeypatch.setattr(
        worker_tasks_topic_embedding, "_get_embedder", lambda *args, **kwargs: FakeEmbedder()
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
        artifact_prefix="topic_classic_test",
        min_topic_size=2,
    )

    assert result["meta"]["engine"] == "bertopic"


# ---------------------------------------------------------------------------
# Sampling and topic size mode tests
# ---------------------------------------------------------------------------


def test_sample_corpus_reduces_length():
    docs = [f"doc {i}" for i in range(100)]
    sampled_docs, sampled_idx = worker_tasks_topic._sample_corpus(docs, 0.5, seed=42)
    assert len(sampled_docs) == 50
    assert len(sampled_idx) == 50
    # Reproducible
    docs2, idx2 = worker_tasks_topic._sample_corpus(docs, 0.5, seed=42)
    assert docs2 == sampled_docs
    assert idx2 == sampled_idx
    # Different seed → different sample
    docs3, idx3 = worker_tasks_topic._sample_corpus(docs, 0.5, seed=99)
    assert docs3 != sampled_docs


def test_sample_corpus_indices_are_original_positions():
    docs = [f"doc {i}" for i in range(20)]
    sampled_docs, sampled_idx = worker_tasks_topic._sample_corpus(docs, 0.5, seed=7)
    # Each returned doc must match the original at the stored index
    for doc, idx in zip(sampled_docs, sampled_idx):
        assert doc == docs[idx]
    # Indices are sorted
    assert sampled_idx == sorted(sampled_idx)


def test_sample_corpus_fraction_at_or_above_1_returns_original():
    docs = ["a", "b", "c"]
    result_docs, result_idx = worker_tasks_topic._sample_corpus(docs, 1.0, seed=42)
    assert result_docs is docs
    assert result_idx == [0, 1, 2]
    result_docs2, _ = worker_tasks_topic._sample_corpus(docs, 2.0, seed=42)
    assert result_docs2 is docs


def test_sample_corpus_min_k_is_1():
    docs = ["only"]
    result_docs, result_idx = worker_tasks_topic._sample_corpus(docs, 0.01, seed=42)
    assert len(result_docs) == 1
    assert len(result_idx) == 1


def test_compute_min_topic_size_target():
    # max(2, 10000 // (50 * 10)) = max(2, 20) = 20
    assert worker_tasks_topic._compute_min_topic_size(10_000, "target", 50) == 20


def test_compute_min_topic_size_min():
    assert worker_tasks_topic._compute_min_topic_size(10_000, "min", 50) == 50


def test_compute_min_topic_size_exact():
    # target heuristic = 10000 // (50 * 10) = 20, then exact uses max(5, int(20 * 0.75)) = 15
    assert worker_tasks_topic._compute_min_topic_size(10_000, "exact", 50) == 15


def test_compute_min_topic_size_floor_is_2():
    assert worker_tasks_topic._compute_min_topic_size(1, "target", 50) == 2
    assert worker_tasks_topic._compute_min_topic_size(1, "exact", 50) == 5


def _make_classic_fake_bertopic_cls(received_docs: list):
    """FakeBERTopic that records the docs passed to fit_transform."""

    class FakeBERTopicClassic:
        def __init__(
            self,
            *,
            verbose,
            min_topic_size,
            embedding_model,
            umap_model,
            vectorizer_model=None,
            language=None,
            top_n_words=10,
        ):
            self.c_tf_idf_ = np.array([[1.0, 0.5]], dtype=float)
            self.topic_embeddings_ = np.array([[0.2, 0.4]], dtype=float)

        def fit_transform(self, docs, embeddings):
            received_docs.extend(docs)
            return [0] * len(docs), None

        def get_topic_freq(self):
            return pd.DataFrame({"Topic": [0, -1], "Count": [len(received_docs), 0]})

        def get_topics(self):
            return {-1: [], 0: [("alpha", 0.9), ("beta", 0.8)]}

    return FakeBERTopicClassic


def test_run_topic_modeling_task_sampling_reduces_corpus(tmp_path, monkeypatch):
    """sample_fractions trims corpus before fit; meta records before/after sizes."""
    received_docs: list = []

    class FakeEmbedder:
        def encode(self, docs, show_progress_bar=False):
            return np.array([[0.1, 0.2] for _ in docs], dtype=float)

    def fake_select(*_a, **_k):
        return np.array([[0.0, 0.0], [1.0, 2.0]], dtype=float), False

    bertopic_module = cast(Any, ModuleType("bertopic"))
    bertopic_module.BERTopic = _make_classic_fake_bertopic_cls(received_docs)
    bertopic_utils_module = cast(Any, ModuleType("bertopic._utils"))
    bertopic_utils_module.select_topic_representation = fake_select

    monkeypatch.setattr(
        worker_tasks_topic_embedding, "_get_embedder", lambda *args, **kwargs: FakeEmbedder()
    )
    monkeypatch.setitem(sys.modules, "bertopic", bertopic_module)
    monkeypatch.setitem(sys.modules, "bertopic._utils", bertopic_utils_module)

    corpus = [f"doc {i}" for i in range(20)]
    result = worker_tasks_topic.run_topic_modeling_task(
        configure_worker_environment=lambda: None,
        user_id="u",
        workspace_id="w",
        corpora=[corpus],
        node_infos=[
            {
                "node_id": "n1",
                "node_name": "N1",
                "text_column": "t",
                "original_columns": [],
            }
        ],
        artifact_dir=str(tmp_path),
        artifact_prefix="sample_test",
        sample_fractions=[0.5],
    )

    assert len(received_docs) == 10
    assert result["meta"]["corpus_sizes_before_sample"] == [20]
    assert result["meta"]["corpus_sizes_after_sample"] == [10]


def test_run_topic_modeling_task_exact_mode_calls_reduce_topics(tmp_path, monkeypatch):
    """topic_size_mode='exact' calls reduce_topics after fit_transform."""
    reduce_topics_calls: list = []

    class FakeEmbedder:
        def encode(self, docs, show_progress_bar=False):
            return np.array([[0.1, 0.2] for _ in docs], dtype=float)

    class FakeBERTopicExact:
        def __init__(
            self,
            *,
            verbose,
            min_topic_size,
            embedding_model,
            umap_model,
            vectorizer_model=None,
            language=None,
            top_n_words=10,
        ):
            self.c_tf_idf_ = np.array(
                [
                    [1.0, 0.5],
                    [0.9, 0.4],
                    [0.8, 0.3],
                    [0.7, 0.2],
                    [0.6, 0.1],
                    [0.5, 0.2],
                    [0.1, 0.1],
                ],
                dtype=float,
            )
            self.topic_embeddings_ = np.array(
                [
                    [0.2, 0.4],
                    [0.3, 0.5],
                    [0.4, 0.6],
                    [0.5, 0.7],
                    [0.6, 0.8],
                    [0.7, 0.9],
                    [0.1, 0.2],
                ],
                dtype=float,
            )
            self.topics_ = [0, 0]

        def fit_transform(self, docs, embeddings):
            return [0] * len(docs), None

        def reduce_topics(self, docs, nr_topics):
            reduce_topics_calls.append(nr_topics)
            self.topics_ = [0] * len(docs)

        def save(self, path, serialization="pickle", save_embedding_model=False):
            with open(path, "wb") as saved_model:
                saved_model.write(b"fake-bertopic")

        def get_topic_freq(self):
            return pd.DataFrame(
                {"Topic": [0, 1, 2, 3, 4, 5, -1], "Count": [1, 1, 1, 1, 1, 1, 0]}
            )

        def get_topics(self):
            return {
                -1: [],
                0: [("alpha", 0.9), ("beta", 0.8)],
                1: [("bravo", 0.9), ("beta", 0.8)],
                2: [("charlie", 0.9), ("beta", 0.8)],
                3: [("delta", 0.9), ("beta", 0.8)],
                4: [("echo", 0.9), ("beta", 0.8)],
                5: [("foxtrot", 0.9), ("beta", 0.8)],
            }

    def fake_select(*_a, **_k):
        return (
            np.array(
                [
                    [0.0, 0.0],
                    [1.0, 2.0],
                    [2.0, 3.0],
                    [3.0, 4.0],
                    [4.0, 5.0],
                    [5.0, 6.0],
                    [6.0, 7.0],
                ],
                dtype=float,
            ),
            False,
        )

    bertopic_module = cast(Any, ModuleType("bertopic"))
    bertopic_module.BERTopic = FakeBERTopicExact
    bertopic_utils_module = cast(Any, ModuleType("bertopic._utils"))
    bertopic_utils_module.select_topic_representation = fake_select

    monkeypatch.setattr(
        worker_tasks_topic_embedding, "_get_embedder", lambda *args, **kwargs: FakeEmbedder()
    )
    monkeypatch.setitem(sys.modules, "bertopic", bertopic_module)
    monkeypatch.setitem(sys.modules, "bertopic._utils", bertopic_utils_module)

    result = worker_tasks_topic.run_topic_modeling_task(
        configure_worker_environment=lambda: None,
        user_id="u",
        workspace_id="w",
        corpora=[["doc one", "doc two"]],
        node_infos=[
            {
                "node_id": "n1",
                "node_name": "N1",
                "text_column": "t",
                "original_columns": [],
            }
        ],
        artifact_dir=str(tmp_path),
        artifact_prefix="exact_test",
        topic_size_mode="exact",
        topic_size_value=5,
    )

    assert reduce_topics_calls == [6]
    assert result["meta"]["topic_size_mode"] == "exact"
    assert result["meta"]["topic_size_value"] == 5
    assert result["meta"]["raw_total_topics"] == 6
    assert result["artifacts"]["version"] == 2
    assert result["artifacts"]["exact_reduction_artifact_path"].endswith(
        "exact_test_exact_reduction.pkl"
    )


def test_reaggregate_exact_topic_modeling_result_counts_outlier_in_reduce_target(
    monkeypatch,
):
    reduce_topics_calls: list[int] = []

    class FakeTopicModel:
        def __init__(self):
            self.topics_ = [0, 1, 1, -1]
            self.c_tf_idf_ = np.array([[1.0, 0.5], [0.4, 0.8], [0.1, 0.2]], dtype=float)
            self.topic_embeddings_ = np.array(
                [[0.2, 0.4], [0.5, 0.6], [0.1, 0.3]], dtype=float
            )

        def get_topic_freq(self):
            return pd.DataFrame({"Topic": [0, 1, -1], "Count": [1, 2, 1]})

        def get_topics(self):
            return {
                -1: [],
                0: [("alpha", 0.9)],
                1: [("beta", 0.8)],
            }

        def reduce_topics(self, docs, nr_topics):
            assert docs == ["doc one", "doc two", "doc three", "doc four"]
            reduce_topics_calls.append(nr_topics)

    monkeypatch.setattr(
        worker_tasks_topic_result,
        "_load_exact_reduction_artifact",
        lambda _path: {
            "topic_model": FakeTopicModel(),
            "all_docs": ["doc one", "doc two", "doc three", "doc four"],
            "corpus_sizes": [4],
            "active_corpora_indices": [[0, 1, 2, 3]],
        },
    )
    monkeypatch.setattr(
        worker_tasks_topic_result,
        "_build_topic_result_payload",
        lambda **_kwargs: {"topics": [], "corpus_sizes": [4], "meta": {}},
    )

    result = worker_tasks_topic_result.reaggregate_exact_topic_modeling_result(
        artifact_path="/tmp/exact.pkl",
        existing_artifacts={},
        node_infos=[
            {
                "node_id": "n1",
                "node_name": "N1",
                "text_column": "t",
                "original_columns": [],
            }
        ],
        topic_size_value=2,
        representative_words_count=10,
        random_seed=42,
    )

    assert reduce_topics_calls == [3]
    assert result["meta"]["raw_total_topics"] == 2
