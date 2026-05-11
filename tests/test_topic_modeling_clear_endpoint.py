import pytest

from ldaca_web_app.api.workspaces.analyses import topic_modeling as topic_modeling_routes
from ldaca_web_app.analysis.implementations.topic_modeling import (
    TopicModelingRequest as AnalysisTopicModelingRequest,
)
from ldaca_web_app.analysis.manager import get_task_manager
from ldaca_web_app.analysis.models import AnalysisStatus, AnalysisTask
from ldaca_web_app.analysis.results import GenericAnalysisResult


@pytest.mark.asyncio
async def test_clear_topic_modeling_results_success(authenticated_client, workspace_id):
    response = await authenticated_client.delete("/api/workspaces/topic-modeling")

    assert response.status_code == 200
    assert response.json() == {
        "state": "successful",
        "message": "Topic modeling analysis results have been cleared.",
    }


@pytest.mark.asyncio
async def test_topic_modeling_result_returns_payload(
    authenticated_client, workspace_id
):
    user_id = "test"
    task_id = "topic-task-1"

    task_manager = get_task_manager(user_id)
    payload = {
        "topics": [
            {
                "id": 0,
                "label": "topic",
                "representative_words": ["alpha", "beta", "gamma"],
                "size": [1],
                "total_size": 1,
                "x": 0.0,
                "y": 0.0,
            }
        ],
        "corpus_sizes": [1],
        "meta": {"native": True},
    }
    task_manager.save_task(
        AnalysisTask(
            task_id=task_id,
            user_id=user_id,
            workspace_id=workspace_id,
            request=AnalysisTopicModelingRequest(
                node_ids=["node-1"],
                node_columns={"node-1": "document"},
                min_topic_size=5,
                random_seed=42,
                representative_words_count=5,
            ),
            status=AnalysisStatus.COMPLETED,
            result=GenericAnalysisResult(payload),
        )
    )

    response = await authenticated_client.get(
        f"/api/workspaces/topic-modeling/tasks/{task_id}/result"
    )

    assert response.status_code == 200
    body = response.json()
    assert body["state"] == "successful"
    assert body["data"]["topics"][0]["label"] == "topic"
    assert body["data"]["topics"][0]["representative_words"] == [
        "alpha",
        "beta",
        "gamma",
    ]


@pytest.mark.asyncio
async def test_topic_modeling_result_update_reaggregates_exact_task(
    authenticated_client, workspace_id, monkeypatch
):
    user_id = "test"
    task_id = "topic-task-exact"

    def fake_reaggregate_exact_topic_modeling_result(**kwargs):
        assert kwargs["topic_size_value"] == 3
        return {
            "topics": [
                {
                    "id": 10,
                    "label": "merged topic",
                    "representative_words": ["merged", "topic"],
                    "size": [2],
                    "total_size": 2,
                    "x": 0.0,
                    "y": 0.0,
                }
            ],
            "corpus_sizes": [2],
            "per_corpus_topic_counts": [{10: 2}],
            "artifacts": {
                "version": 2,
                "topic_meanings_parquet_path": "/tmp/topic_meanings.parquet",
                "exact_reduction_artifact_path": "/tmp/exact.pkl",
                "nodes": [
                    {
                        "node_id": "node-1",
                        "node_name": "Node 1",
                        "text_column": "document",
                        "original_columns": ["document"],
                        "assignments_parquet_path": "/tmp/assignments.parquet",
                    }
                ],
            },
            "meta": {"raw_total_topics": 8},
        }

    monkeypatch.setattr(
        topic_modeling_routes,
        "reaggregate_exact_topic_modeling_result",
        fake_reaggregate_exact_topic_modeling_result,
    )

    task_manager = get_task_manager(user_id)
    payload = {
        "topics": [
            {
                "id": 0,
                "label": "topic",
                "representative_words": ["alpha", "beta", "gamma"],
                "size": [1],
                "total_size": 1,
                "x": 0.0,
                "y": 0.0,
            }
        ],
        "corpus_sizes": [2],
        "artifacts": {
            "version": 2,
            "topic_meanings_parquet_path": "/tmp/topic_meanings.parquet",
            "exact_reduction_artifact_path": "/tmp/exact.pkl",
            "nodes": [
                {
                    "node_id": "node-1",
                    "node_name": "Node 1",
                    "text_column": "document",
                    "original_columns": ["document"],
                    "assignments_parquet_path": "/tmp/assignments.parquet",
                }
            ],
        },
        "meta": {"topic_size_mode": "exact", "topic_size_value": 5, "raw_total_topics": 8},
    }
    task_manager.save_task(
        AnalysisTask(
            task_id=task_id,
            user_id=user_id,
            workspace_id=workspace_id,
            request=AnalysisTopicModelingRequest(
                node_ids=["node-1"],
                node_columns={"node-1": "document"},
                min_topic_size=5,
                random_seed=42,
                representative_words_count=5,
                topic_size_mode="exact",
                topic_size_value=5,
            ),
            status=AnalysisStatus.COMPLETED,
            result=GenericAnalysisResult(payload),
        )
    )

    response = await authenticated_client.post(
        f"/api/workspaces/topic-modeling/tasks/{task_id}/result",
        json={"topic_size_value": 3},
    )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["state"] == "successful"
    assert body["data"]["topics"][0]["id"] == 10
    assert body["data"]["meta"]["topic_size_value"] == 3
    assert body["data"]["meta"]["raw_total_topics"] == 8

    saved_task = task_manager.get_task(task_id)
    assert saved_task is not None
    # The post-fit re-aggregation slider is decoupled from the "Target Topic
    # Number" parameter — it persists the new value in `result.meta` only,
    # leaving `task.request.topic_size_value` (the rerun target) untouched
    # so the user can still see / edit the original parameter independently.
    assert saved_task.request.topic_size_value == 5
