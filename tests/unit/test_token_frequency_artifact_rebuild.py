from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from fastapi import HTTPException

from ldaca_wordflow.analysis.implementations.token_frequency import (
    TokenFrequencyRequest,
)
from ldaca_wordflow.analysis.models import AnalysisStatus, AnalysisTask
from ldaca_wordflow.analysis.results import GenericAnalysisResult
from ldaca_wordflow.api.workspaces.analyses.token_frequencies import (
    _rebuild_token_result,
)


def _task_with_payload(payload: dict) -> AnalysisTask:
    return AnalysisTask(
        task_id="task-1",
        user_id="user-1",
        workspace_id="workspace-1",
        status=AnalysisStatus.COMPLETED,
        request=TokenFrequencyRequest(
            node_ids=["node-1"],
            node_columns={"node-1": "text"},
            token_limit=25,
            stop_words=[],
        ),
        result=GenericAnalysisResult(payload),
    )


def test_rebuild_token_result_uses_typed_node_artifacts(tmp_path: Path) -> None:
    token_path = tmp_path / "tokens.parquet"
    pl.DataFrame({"token": ["alpha"], "frequency": [3]}).write_parquet(
        token_path
    )
    task = _task_with_payload(
        {
            "state": "successful",
            "message": "done",
            "artifacts": {
                "nodes": [
                    {
                        "node_id": "node-1",
                        "node_name": "Node One",
                        "token_parquet_path": str(token_path),
                    }
                ]
            },
        }
    )

    result = _rebuild_token_result(task)

    assert result["data"]["node-1"]["data"] == [
        {"token": "alpha", "frequency": 3}
    ]
    assert result["metadata"]["node_display_names"] == {"node-1": "Node One"}


def test_rebuild_token_result_rejects_invalid_node_artifact() -> None:
    task = _task_with_payload(
        {
            "artifacts": {
                "nodes": [
                    {
                        "node_id": "node-1",
                    }
                ]
            }
        }
    )

    with pytest.raises(HTTPException) as exc_info:
        _rebuild_token_result(task)

    assert exc_info.value.status_code == 500
    assert exc_info.value.detail == "Token-frequency artifact manifest is invalid"