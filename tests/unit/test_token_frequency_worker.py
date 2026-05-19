import sys
from types import ModuleType
from typing import Any, cast

import polars as pl

from ldaca_wordflow.core.worker_tasks_token import run_token_frequencies_task


def test_token_frequency_worker_emits_early_progress_updates(tmp_path, monkeypatch):
    progress_updates: list[tuple[float, str]] = []

    fake_polars_text = cast(Any, ModuleType("polars_text"))
    fake_polars_text.token_frequencies = lambda series: {"alpha": 3, "beta": 1}
    fake_polars_text.token_frequency_stats = lambda left, right: pl.DataFrame(
        {
            "token": ["alpha"],
            "freq_corpus_0": [3],
            "freq_corpus_1": [2],
            "corpus_0_total": [4],
            "corpus_1_total": [4],
            "percent_corpus_0": [0.75],
            "percent_corpus_1": [0.5],
            "log_likelihood_llv": [1.2],
            "percent_diff": [0.25],
            "bayes_factor_bic": [0.5],
            "effect_size_ell": [0.1],
            "relative_risk": [1.5],
            "log_ratio": [0.3],
            "odds_ratio": [1.2],
            "significance": ["*"],
        }
    )
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
        progress_callback=lambda progress, message: progress_updates.append(
            (
                progress,
                message,
            )
        ),
    )

    assert result["state"] == "successful"
    assert progress_updates[0][1].startswith("Loading token frequency")
    assert any(
        "Preparing text data" in message for _progress, message in progress_updates
    )
    assert progress_updates[-1] == (1.0, "Token frequency analysis completed")


def test_patch_stats_keyness_columns_recovers_lancaster_formulas() -> None:
    """Backend patch should overwrite polars-text 0.2.1's two buggy
    keyness columns with the Lancaster wizard's documented formulas:
    `%DIFF` = ((NF_studied − NF_ref) / NF_ref) × 100, and `log_ratio` =
    log₂(NF_studied / NF_ref). Verified on a small frame with a known
    doubling (NF_studied = 2 × NF_ref) so the expected values are
    +100% and +1.0 exactly."""
    import polars as pl

    from ldaca_wordflow.core.worker_tasks_token import _patch_stats_keyness_columns

    stats = pl.DataFrame(
        {
            "token": ["alpha", "only_studied"],
            "freq_corpus_0": [5, 0],
            "freq_corpus_1": [10, 4],
            "corpus_0_total": [1000, 1000],
            "corpus_1_total": [1000, 1000],
            # The two buggy columns coming back from polars-text 0.2.1.
            # Specific values aren't checked; we just verify the patch
            # overwrites them.
            "percent_diff": [-999.0, -999.0],
            "log_ratio": [-999.0, -999.0],
        }
    )

    patched = _patch_stats_keyness_columns(stats).to_dicts()
    alpha = next(r for r in patched if r["token"] == "alpha")
    only_studied = next(r for r in patched if r["token"] == "only_studied")
    assert alpha["percent_diff"] == 100.0
    assert alpha["log_ratio"] == 1.0
    assert only_studied["percent_diff"] == float("inf")
    assert only_studied["log_ratio"] is None
