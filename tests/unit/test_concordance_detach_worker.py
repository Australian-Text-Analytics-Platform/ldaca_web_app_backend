from pathlib import Path
from typing import cast

import polars as pl
from ldaca_web_app_backend.core import worker
from ldaca_web_app_backend.core.worker_tasks_concordance import (
    run_concordance_detach_task,
)


def test_concordance_detach_task_forwards_extra_columns_data(monkeypatch):
    captured: dict[str, object] = {}

    def fake_run_concordance_detach_task(
        configure_worker_environment,
        workspace_dir,
        node_corpus,
        parent_node_id,
        document_column,
        search_word,
        num_left_tokens,
        num_right_tokens,
        regex,
        whole_word,
        case_sensitive,
        new_node_name,
        include_document_column=False,
        extra_columns_data=None,
        progress_callback=None,
    ):
        captured["include_document_column"] = include_document_column
        captured["extra_columns_data"] = extra_columns_data
        captured["whole_word"] = whole_word
        return {"state": "successful"}

    monkeypatch.setattr(
        worker, "run_concordance_detach_task", fake_run_concordance_detach_task
    )

    result = worker.concordance_detach_task(
        user_id="user-1",
        workspace_id="ws-1",
        workspace_dir="/tmp/workspace",
        node_corpus=["alpha beta"],
        parent_node_id="node-1",
        document_column="document",
        search_word="alpha",
        num_left_tokens=2,
        num_right_tokens=2,
        regex=False,
        whole_word=True,
        case_sensitive=False,
        new_node_name="node_1_conc",
        include_document_column=True,
        extra_columns_data={"source": ["a"]},
    )

    assert result == {"state": "successful"}
    assert captured["include_document_column"] is True
    assert captured["extra_columns_data"] == {"source": ["a"]}
    assert captured["whole_word"] is True


def test_concordance_detach_task_writes_node_payload_under_workspace_data(tmp_path):
    progress_updates: list[tuple[float, str]] = []

    result = run_concordance_detach_task(
        configure_worker_environment=lambda: None,
        workspace_dir=str(tmp_path),
        node_corpus=["alpha beta", "beta gamma"],
        parent_node_id="parent-1",
        document_column="document",
        search_word="alpha",
        num_left_tokens=1,
        num_right_tokens=1,
        regex=False,
        whole_word=False,
        case_sensitive=False,
        new_node_name="detached_concordance",
        include_document_column=True,
        progress_callback=lambda progress, message: progress_updates.append(
            (
                progress,
                message,
            )
        ),
    )

    assert result["state"] == "successful"
    payload = result["result"]["node_payload"]
    assert payload["data_path"].startswith("data/")
    assert "artifacts" not in payload["data_path"]

    data_file = tmp_path / Path(payload["data_path"])
    assert data_file.exists()

    restored = pl.LazyFrame.deserialize(data_file.open("rb"), format="binary")
    restored_df = cast(pl.DataFrame, restored.collect())
    assert restored_df.height >= 1
    assert progress_updates[0][1].startswith("Loading concordance")
    assert any(
        "Preparing text data" in message for _progress, message in progress_updates
    )
    assert progress_updates[-1] == (1.0, "Concordance detach completed")
