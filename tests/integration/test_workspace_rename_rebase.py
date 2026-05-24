"""Regression: renaming a workspace must not break in-memory lazy plans.

Before the fix, ``update_workspace`` renamed the on-disk folder via
``_resolve_workspace_dir`` but never rebased the in-memory ``LazyFrame``
plans, so ``collect()`` failed against the now-missing prior paths.
"""

from pathlib import Path

import pytest
from ldaca_wordflow.core.workspace import workspace_manager
from polars_text import list_source_paths


@pytest.mark.anyio
async def test_rename_workspace_keeps_lazy_plans_collectable(
    authenticated_client, workspace_id, tiny_node_id
):
    workspace_before = workspace_manager.get_current_workspace("test")
    assert workspace_before is not None
    folder_before = Path(workspace_before.ws_root_dir)
    # Sanity: lazy plan resolves before the rename.
    assert workspace_before.nodes[tiny_node_id].data.collect().height > 0

    resp = await authenticated_client.put(
        "/api/workspaces/name",
        params={"new_name": "renamed_workspace"},
    )
    assert resp.status_code == 200, resp.text

    workspace_after = workspace_manager.get_current_workspace("test")
    assert workspace_after is not None
    assert workspace_after.name == "renamed_workspace"
    folder_after = Path(workspace_after.ws_root_dir)
    assert folder_after != folder_before
    assert folder_after.name == "renamed_workspace"
    assert folder_after.exists()
    assert not folder_before.exists()

    node_after = workspace_after.nodes[tiny_node_id]
    # plbin on disk must reference paths under the new folder
    plbin = folder_after / "data" / f"{tiny_node_id}.plbin"
    assert plbin.exists()
    sources = list_source_paths(plbin)
    assert sources, "expected at least one scan source"
    # polars stores scan paths POSIX-style; normalise both sides so the
    # substring check works on Windows (where str(Path) uses backslashes).
    folder_after_posix = folder_after.as_posix()
    for src in sources:
        assert folder_after_posix in src.replace("\\", "/"), (
            f"stale source path in plbin: {src}"
        )

    # in-memory plan must collect without resolving stale paths
    df = node_after.data.collect()
    assert df.height > 0
