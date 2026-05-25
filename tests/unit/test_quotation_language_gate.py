"""Phase 3.6: quotation routes reject non-English with a typed payload.

Calls the route handlers directly to keep tests fast (no FastAPI client),
and asserts the language gate fires BEFORE any spaCy / vendored extractor
work is attempted — which is what makes the gate cheap to enforce.
"""

from __future__ import annotations

from typing import Any, cast

import polars as pl
import pytest
from fastapi import HTTPException
from ldaca_wordflow.api.workspaces.analyses import quotation as quotation_api
from ldaca_wordflow.models import (
    QuotationDetachRequest,
    QuotationMaterializeRequest,
    QuotationRequest,
)

from docworkspace import Node


def _zh_node() -> Node:
    df = pl.DataFrame({"text": ["今天天气很好"]}).lazy()
    node = Node(data=df, name="zh_root")
    node.register_derived_column(
        "__derived__.tokens.text.jieba",
        {  # type: ignore[arg-type]
            "source_column": "text",
            "form": "tokens",
            "model": "jieba",
            "language": "zh",
            "generated_at": "2026-05-12T00:00:00+00:00",
        },
    )
    return node


@pytest.fixture
def fake_workspace_manager(monkeypatch: pytest.MonkeyPatch):
    node = _zh_node()

    class _Workspace:
        id = "ws1"
        name = "zh-corpus"
        ws_root_dir = "/tmp/dummy"

        def __init__(self):
            self.nodes = {node.id: node}

    workspace = _Workspace()

    class _Manager:
        def get_current_workspace(self, _user_id: str):
            return workspace

        def get_current_workspace_id(self, _user_id: str):
            return workspace.id

        def get_task_manager(self, _user_id: str):
            # Should never be reached when the gate fires.
            raise AssertionError(
                "task manager accessed before language gate; gate is leaky"
            )

    monkeypatch.setattr(quotation_api, "workspace_manager", _Manager())
    return workspace, node


@pytest.mark.asyncio
async def test_get_quotation_rejects_explicit_chinese(fake_workspace_manager):
    _ws, node = fake_workspace_manager
    request = QuotationRequest(column="text", language="zh")

    with pytest.raises(HTTPException) as exc_info:
        await quotation_api.get_quotation(
            node_id=node.id, request=request, current_user={"id": "user"}
        )

    assert exc_info.value.status_code == 400
    detail = cast(dict[str, Any], exc_info.value.detail)
    assert detail["error"] == "unsupported_language"
    assert detail["tool"] == "Quotation extractor"
    assert detail["language"] == "zh"
    assert detail["supported"] == ["en"]


@pytest.mark.asyncio
async def test_get_quotation_rejects_implicit_chinese_via_derived(
    fake_workspace_manager,
):
    """No explicit language in request, but node has jieba/zh derived
    tokens — gate falls back to derived metadata and refuses."""
    _ws, node = fake_workspace_manager
    request = QuotationRequest(column="text")  # language=None

    with pytest.raises(HTTPException) as exc_info:
        await quotation_api.get_quotation(
            node_id=node.id, request=request, current_user={"id": "user"}
        )

    assert exc_info.value.status_code == 400
    assert cast(dict[str, Any], exc_info.value.detail)["language"] == "zh"


@pytest.mark.asyncio
async def test_detach_quotation_rejects_non_english(fake_workspace_manager):
    _ws, node = fake_workspace_manager
    request = QuotationDetachRequest(node_id=node.id, column="text", language="ja")

    with pytest.raises(HTTPException) as exc_info:
        await quotation_api.detach_quotation(
            node_id=node.id, request=request, current_user={"id": "user"}
        )

    assert exc_info.value.status_code == 400
    assert cast(dict[str, Any], exc_info.value.detail)["language"] == "ja"


@pytest.mark.asyncio
async def test_materialize_quotation_rejects_non_english(fake_workspace_manager):
    _ws, node = fake_workspace_manager
    request = QuotationMaterializeRequest(
        column="text", parent_task_id="task-1", language="zh"
    )

    with pytest.raises(HTTPException) as exc_info:
        await quotation_api.materialize_quotation(
            node_id=node.id, request=request, current_user={"id": "user"}
        )

    assert exc_info.value.status_code == 400
    assert cast(dict[str, Any], exc_info.value.detail)["language"] == "zh"


def test_quotation_gate_helper_returns_resolved_language() -> None:
    """The helper returns the resolved language string when the gate
    passes — useful for downstream logging/telemetry."""
    df = pl.DataFrame({"text": ["hello"]}).lazy()
    node = Node(data=df, name="en_root")

    resolved = quotation_api._enforce_quotation_language_gate(None, node)
    assert resolved == "en"

    resolved = quotation_api._enforce_quotation_language_gate("EN", node)
    assert resolved == "en"
