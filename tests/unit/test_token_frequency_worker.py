import sys
from types import ModuleType

import polars as pl

from ldaca_web_app_backend.core.worker_tasks_token import run_token_frequencies_task


def test_token_frequency_worker_emits_early_progress_updates(tmp_path, monkeypatch):
    progress_updates: list[tuple[float, str]] = []

    fake_polars_text = ModuleType("polars_text")
    fake_polars_text.token_frequencies = lambda series: {"alpha": 3, "beta": 1}
    fake_polars_text.token_frequency_stats = lambda left, right: pl.DataFrame({
        "token": ["alpha"],
        "freq_corpus_0": [3],
        "percent_corpus_0": [0.75],
        "freq_corpus_1": [2],
        "percent_corpus_1": [0.5],
        "log_likelihood_llv": [1.2],
        "percent_diff": [0.25],
        "bayes_factor_bic": [0.5],
        "effect_size_ell": [0.1],
        "relative_risk": [1.5],
        "log_ratio": [0.3],
        "odds_ratio": [1.2],
        "significance": ["*"],
    })
    monkeypatch.setitem(sys.modules, "polars_text", fake_polars_text)

    result = run_token_frequencies_task(
        configure_worker_environment=lambda: None,
        user_id="user-1",
        workspace_id="ws-1",
        node_corpora={
            "node-1": ["alpha beta alpha"],
            "node-2": ["alpha beta"],
        },
        node_display_names={"node-1": "Data Block 1", "node-2": "Data Block 2"},
        artifact_dir=str(tmp_path),
        artifact_prefix="token_frequency_test",
        progress_callback=lambda progress, message: progress_updates.append((
            progress,
            message,
        )),
    )

    assert result["state"] == "successful"
    assert progress_updates[0][1].startswith("Loading token frequency")
    assert any(
        "Preparing text data" in message for _progress, message in progress_updates
    )
    assert progress_updates[-1] == (1.0, "Token frequency analysis completed")
