"""Token-frequency worker tests for raw text and token-stream inputs."""

from __future__ import annotations

import sys
from pathlib import Path
from types import ModuleType
from typing import Any, cast

import polars as pl
from ldaca_wordflow.core.worker_tasks_token import run_token_frequencies_task


def _stub_polars_text(monkeypatch) -> None:
    """Stub polars_text so this test doesn't depend on the Rust extension."""
    fake = cast(Any, ModuleType("polars_text"))

    def _token_frequencies(series: pl.Series) -> dict[str, int]:
        # Mimic the raw-text path: naive whitespace split for the test.
        counter: dict[str, int] = {}
        for value in series.to_list():
            for token in (value or "").split():
                counter[token] = counter.get(token, 0) + 1
        return counter

    fake.token_frequencies = _token_frequencies
    fake.token_frequency_stats = lambda *_args, **_kwargs: pl.DataFrame()
    monkeypatch.setitem(sys.modules, "polars_text", fake)


def test_worker_raw_text_path_unchanged_when_no_tokens(tmp_path, monkeypatch):
    _stub_polars_text(monkeypatch)

    result = run_token_frequencies_task(
        configure_worker_environment=lambda: None,
        user_id="user-1",
        workspace_id="ws-1",
        node_corpora={"node-1": ["alpha beta alpha", "alpha"]},
        node_display_names={"node-1": "EN Corpus"},
        artifact_dir=str(tmp_path),
        artifact_prefix="token_freq_text",
    )

    assert result["state"] == "successful"
    parquet_path = Path(result["artifacts"]["nodes"][0]["token_parquet_path"])
    counts = pl.read_parquet(parquet_path).to_dicts()
    counts_map = {row["token"]: row["frequency"] for row in counts}
    assert counts_map == {"alpha": 3, "beta": 1}


def test_worker_mixes_token_stream_and_text_paths(tmp_path, monkeypatch):
    """Two-corpus comparison where one side uses a token stream."""
    _stub_polars_text(monkeypatch)

    stream_path = tmp_path / "tokens-side-stream.parquet"
    pl.DataFrame({"token": ["beta", "gamma", "gamma"]}).write_parquet(stream_path)

    result = run_token_frequencies_task(
        configure_worker_environment=lambda: None,
        user_id="user-1",
        workspace_id="ws-1",
        node_corpora={"text-side": ["alpha beta alpha"]},
        node_token_streams={"tokens-side": str(stream_path)},
        node_display_names={"text-side": "EN", "tokens-side": "ZH"},
        artifact_dir=str(tmp_path),
        artifact_prefix="token_freq_mixed",
    )

    assert result["state"] == "successful"
    node_paths = {
        artifact["node_id"]: Path(artifact["token_parquet_path"])
        for artifact in result["artifacts"]["nodes"]
    }
    text_counts = {
        row["token"]: row["frequency"]
        for row in pl.read_parquet(node_paths["text-side"]).to_dicts()
    }
    tokens_counts = {
        row["token"]: row["frequency"]
        for row in pl.read_parquet(node_paths["tokens-side"]).to_dicts()
    }
    assert text_counts == {"alpha": 2, "beta": 1}
    assert tokens_counts == {"beta": 1, "gamma": 2}


def test_worker_uses_node_token_streams_when_provided(tmp_path, monkeypatch):
    """Phase 5 perf path: the API endpoint spills one row per token to a
    parquet via ``sink_parquet``, then hands the path to the worker.
    Worker scans + group_by.len in Polars — no Python list materialisation.
    """
    _stub_polars_text(monkeypatch)

    # Simulate the spill the endpoint produces — one row per token, in
    # the ``token`` column, post-explode + post-null-filter.
    stream_path = tmp_path / "stream.parquet"
    pl.DataFrame(
        {"token": ["alpha", "beta", "alpha", "alpha", "gamma", "gamma"]}
    ).write_parquet(stream_path)

    result = run_token_frequencies_task(
        configure_worker_environment=lambda: None,
        user_id="user-1",
        workspace_id="ws-1",
        node_corpora={},
        node_token_streams={"node-1": str(stream_path)},
        node_display_names={"node-1": "ZH Corpus"},
        artifact_dir=str(tmp_path),
        artifact_prefix="token_freq_stream",
    )

    assert result["state"] == "successful"
    parquet_path = Path(result["artifacts"]["nodes"][0]["token_parquet_path"])
    counts = pl.read_parquet(parquet_path).to_dicts()
    counts_map = {row["token"]: row["frequency"] for row in counts}
    assert counts_map == {"alpha": 3, "beta": 1, "gamma": 2}


def test_worker_token_stream_matches_manual_explode(tmp_path, monkeypatch):
    """The stream path matches the expected explode/group-by frequencies."""
    _stub_polars_text(monkeypatch)

    raw_token_lists = [
        ["alpha", "beta", "alpha", "gamma"],
        ["beta", "beta"],
        ["alpha"],
    ]

    # The polars baseline against an equivalent List[String] column.
    exploded_df = pl.DataFrame({"tokens": raw_token_lists}).explode("tokens")
    baseline_df = exploded_df.group_by("tokens").agg(pl.len().alias("frequency"))
    expected = {row["tokens"]: row["frequency"] for row in baseline_df.to_dicts()}
    stream_path = tmp_path / "stream.parquet"
    exploded_df.rename({"tokens": "token"}).select("token").write_parquet(stream_path)

    result = run_token_frequencies_task(
        configure_worker_environment=lambda: None,
        user_id="user-1",
        workspace_id="ws-1",
        node_corpora={},
        node_token_streams={"node-1": str(stream_path)},
        node_display_names={"node-1": "Corpus"},
        artifact_dir=str(tmp_path),
        artifact_prefix="token_freq_consistency",
    )

    parquet_path = Path(result["artifacts"]["nodes"][0]["token_parquet_path"])
    actual = {
        row["token"]: row["frequency"]
        for row in pl.read_parquet(parquet_path).to_dicts()
    }
    assert actual == expected
