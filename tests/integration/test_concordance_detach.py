import polars as pl
import pytest
from docworkspace import Node
from ldaca_web_app.core.workspace import workspace_manager


@pytest.mark.anyio
async def test_concordance_detach_starts_task(authenticated_client, workspace_id):
    """Ensure detaching concordance starts a background task."""
    df = pl.DataFrame({"text": ["alpha beta", "beta gamma", "alpha gamma"]})
    workspace = workspace_manager.get_current_workspace("test")
    assert workspace is not None

    node = Node(
        data=df.lazy(),
        name="text_node",
        workspace=workspace,
        operation="test_add",
        parents=[],
    )
    workspace.add_node(node)
    assert node is not None

    # Act: call detach endpoint
    resp = await authenticated_client.post(
        f"/api/workspaces/nodes/{node.id}/concordance/detach",
        json={
            "node_id": node.id,
            "column": "text",
            "search_word": "alpha",
            "num_left_tokens": 2,
            "num_right_tokens": 2,
            "regex": False,
            "case_sensitive": False,
        },
    )

    # Assert
    assert resp.status_code == 200, resp.text
    payload = resp.json()
    assert payload.get("state") == "running"
    assert payload.get("metadata", {}).get("task_id")


@pytest.mark.anyio
async def test_concordance_detach_options_include_mandatory_and_optional_columns(
    authenticated_client, workspace_id
):
    df = pl.DataFrame({
        "text": ["alpha beta", "beta gamma", "alpha gamma"],
        "speaker": ["a", "b", "c"],
    })
    workspace = workspace_manager.get_current_workspace("test")
    assert workspace is not None

    node = Node(
        data=df.lazy(),
        name="text_node",
        workspace=workspace,
        operation="test_add",
        parents=[],
    )
    workspace.add_node(node)

    resp = await authenticated_client.get(
        f"/api/workspaces/nodes/{node.id}/concordance/detach-options",
        params={"column": "text"},
    )

    assert resp.status_code == 200, resp.text
    payload = resp.json()
    node_option = payload["data"]["nodes"][0]

    assert node_option["node_id"] == node.id
    assert node_option["text_column"] == "text"
    assert node_option["available_columns"] == [
        "text",
        "CONC_left_context",
        "CONC_matched_text",
        "CONC_right_context",
        "CONC_start_idx",
        "CONC_end_idx",
        "CONC_l1",
        "CONC_r1",
        "CONC_extraction",
        "speaker",
    ]
    # CONC_extraction is opt-in (not mandatory) so it stays out of
    # disabled_columns despite the `CONC_` prefix.
    assert node_option["disabled_columns"] == [
        "CONC_left_context",
        "CONC_matched_text",
        "CONC_right_context",
        "CONC_start_idx",
        "CONC_end_idx",
        "CONC_l1",
        "CONC_r1",
    ]


@pytest.mark.anyio
async def test_concordance_detach_options_hide_derived_columns(
    authenticated_client, workspace_id
):
    """``__derived__.*`` columns (tokens, future analytic derivations) must
    not appear in the detach picker. They live on the source node's
    LazyFrame for analytics consumption (decision 7) but have no
    user-facing role in a detach payload.

    Regression: before the fix, ``list(node.data.collect_schema().names())``
    on a tokenised node leaked ``__derived__.tokens.<source>.<model>`` into
    ``available_columns``.
    """
    from ldaca_web_app.api.workspaces.analyses.generated_columns import (
        TOKENS_FORM,
        derived_column_name,
    )

    df = pl.DataFrame({"text": ["alpha beta", "beta gamma"]})
    workspace = workspace_manager.get_current_workspace("test")
    assert workspace is not None
    node = Node(
        data=df.lazy(),
        name="text_node",
        workspace=workspace,
        operation="test_add",
        parents=[],
    )
    workspace.add_node(node)

    # Register a derived column directly on the in-memory node so the test
    # doesn't need to round-trip through plbin (the polars FFI plan can't
    # always be deserialized cross-version; see prior FfiPlugin episode).
    derived_name = derived_column_name(TOKENS_FORM, "text", "bert-base-uncased")
    node.register_derived_column(  # type: ignore[arg-type]
        derived_name,
        {
            "source_column": "text",
            "form": TOKENS_FORM,
            "model": "bert-base-uncased",
            "language": "en",
            "generated_at": "2026-05-12T00:00:00+00:00",
        },
    )

    resp = await authenticated_client.get(
        f"/api/workspaces/nodes/{node.id}/concordance/detach-options",
        params={"column": "text"},
    )

    assert resp.status_code == 200, resp.text
    node_option = resp.json()["data"]["nodes"][0]
    assert all(
        not isinstance(c, str) or not c.startswith("__derived__.")
        for c in node_option["available_columns"]
    )
    assert all(
        not isinstance(c, str) or not c.startswith("__derived__.")
        for c in node_option["disabled_columns"]
    )
