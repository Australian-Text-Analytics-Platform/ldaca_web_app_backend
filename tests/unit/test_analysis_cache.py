"""Lifecycle tests for analysis side-effect parquet caches.

These tests focus on the cleanup functions in `core.analysis_cache` — in
particular their multi-user isolation, since the same workspace name and
task_id can legitimately occur under different users.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

from ldaca_wordflow.core import utils as core_utils
from ldaca_wordflow.core import workspace as workspace_module
from ldaca_wordflow.core.analysis_cache import (
    cleanup_orphan_caches,
    cleanup_task_caches,
    cleanup_workspace_caches,
    materialized_cache_path,
)
from ldaca_wordflow.core.utils import generate_workspace_id
from ldaca_wordflow.core.workspace import WorkspaceManager


TASK_A = "11111111-1111-1111-1111-111111111111"
TASK_B = "22222222-2222-2222-2222-222222222222"
TASK_C = "33333333-3333-3333-3333-333333333333"


def _bootstrap_workspace(
    manager: WorkspaceManager, user_id: str, name: str
) -> tuple[str, Path]:
    """Create a workspace dir on disk and register it with the manager.

    Returns ``(workspace_id, workspace_dir)``.
    """
    from docworkspace import Workspace

    workspace_id = generate_workspace_id()
    target_dir = manager._resolve_workspace_dir(
        user_id=user_id, workspace_id=workspace_id, workspace_name=name
    )
    target_dir.mkdir(parents=True, exist_ok=True)
    (target_dir / "data").mkdir(parents=True, exist_ok=True)
    ws = Workspace(name=name, ws_root_dir=target_dir)
    ws.id = workspace_id
    ws.modified_at = datetime.now().isoformat()
    ws.save(target_dir)
    manager._set_cached_path(user_id, workspace_id, target_dir)
    return workspace_id, target_dir


def _write_cache(workspace_dir: Path, feature: str, task_id: str, node_id: str) -> Path:
    path = materialized_cache_path(workspace_dir, feature, task_id, node_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"a": [1]}).write_parquet(path)
    return path


@pytest.fixture
def isolated_manager(tmp_path, monkeypatch):
    """Fresh WorkspaceManager rooted under tmp_path, multi-user mode on."""
    monkeypatch.setattr(core_utils.settings, "multi_user", True, raising=False)
    monkeypatch.setattr(
        core_utils.settings, "user_data_folder", "users", raising=False
    )
    monkeypatch.setattr(core_utils.settings, "data_root", tmp_path, raising=False)

    manager = WorkspaceManager()
    monkeypatch.setattr(workspace_module, "workspace_manager", manager)
    return manager


def test_materialized_cache_path_is_canonical(tmp_path):
    p = materialized_cache_path(tmp_path / "ws", "concordance", TASK_A, "node-9")
    assert p == tmp_path / "ws" / "data" / "artifacts" / (
        f".materialized_concordance_{TASK_A}_node-9.parquet"
    )


def test_cache_lives_in_artifacts_subdir_not_data_root(tmp_path):
    """Regression: caches under ``data/`` are deleted by docworkspace's GC
    on every ``workspace.save()``. They must live in ``data/artifacts/``,
    which docworkspace's non-recursive GC iterator skips.
    """
    p = materialized_cache_path(tmp_path / "ws", "concordance", TASK_A, "n")
    assert p.parent.name == "artifacts"
    assert p.parent.parent.name == "data"


def test_cleanup_task_caches_removes_only_named_task(isolated_manager):
    workspace_id, user_dir = _bootstrap_workspace(isolated_manager, "user_one", "ws1")

    a = _write_cache(user_dir, "concordance", TASK_A, "node-x")
    b = _write_cache(user_dir, "quotation", TASK_A, "node-y")
    c = _write_cache(user_dir, "concordance", TASK_B, "node-x")

    removed = cleanup_task_caches("user_one", workspace_id, TASK_A)
    assert removed == 2
    assert not a.exists()
    assert not b.exists()
    assert c.exists(), "cache for unrelated task must survive"


def test_cleanup_task_caches_is_multi_user_isolated(isolated_manager):
    """The same task_id under different users must not bleed across."""
    ws_alpha, dir_a = _bootstrap_workspace(
        isolated_manager, "user_alpha", "shared_name"
    )
    _ws_beta, dir_b = _bootstrap_workspace(
        isolated_manager, "user_beta", "shared_name"
    )

    file_a = _write_cache(dir_a, "concordance", TASK_A, "n")
    file_b = _write_cache(dir_b, "concordance", TASK_A, "n")

    removed = cleanup_task_caches("user_alpha", ws_alpha, TASK_A)
    assert removed == 1
    assert not file_a.exists()
    assert file_b.exists(), (
        "user_beta's identically-named cache file must be untouched — "
        "cleanup_task_caches MUST scope by (user_id, workspace_id)."
    )


def test_cleanup_task_caches_returns_zero_for_missing_workspace(isolated_manager):
    assert cleanup_task_caches("ghost_user", "missing_ws", TASK_A) == 0
    assert cleanup_task_caches("ghost_user", "", TASK_A) == 0
    assert cleanup_task_caches("ghost_user", "missing_ws", "") == 0


def test_cleanup_workspace_caches_clears_all_cache_dotfiles(isolated_manager):
    workspace_id, user_dir = _bootstrap_workspace(isolated_manager, "user_one", "ws1")

    a = _write_cache(user_dir, "concordance", TASK_A, "n1")
    b = _write_cache(user_dir, "concordance", TASK_B, "n2")
    c = _write_cache(user_dir, "quotation", TASK_C, "n3")

    # Unrelated dotfile (e.g. left by another tool) must survive.
    unrelated_dotfile = user_dir / "data" / ".cache_other.parquet"
    pl.DataFrame({"x": [0]}).write_parquet(unrelated_dotfile)

    removed = cleanup_workspace_caches("user_one", workspace_id)
    assert removed == 3
    assert not a.exists() and not b.exists() and not c.exists()
    assert unrelated_dotfile.exists()


def test_cleanup_orphan_caches_keeps_live_tasks(isolated_manager):
    workspace_id, user_dir = _bootstrap_workspace(isolated_manager, "user_one", "ws1")

    live = _write_cache(user_dir, "concordance", TASK_A, "n1")
    dead = _write_cache(user_dir, "concordance", TASK_B, "n2")

    removed = cleanup_orphan_caches("user_one", workspace_id, live_task_ids={TASK_A})
    assert removed == 1
    assert live.exists()
    assert not dead.exists()


def test_cache_survives_workspace_save_gc(isolated_manager):
    """End-to-end regression for the original bug.

    Steps:
      1. Bootstrap a workspace (already saves it once).
      2. Write a cache file via the canonical path.
      3. Call ``workspace.save()`` again — this triggers the docworkspace
         GC that historically deleted any unreferenced parquet under
         ``data/``.
      4. The cache file must still exist.

    The fix that makes this pass is putting caches under
    ``data/artifacts/`` instead of ``data/`` root. The GC's
    ``iterdir()`` is non-recursive and skips directory entries, so files
    inside ``data/artifacts/`` are never scanned.
    """
    from docworkspace import Workspace

    workspace_id, user_dir = _bootstrap_workspace(isolated_manager, "user_one", "wsk")
    cache_path = _write_cache(user_dir, "concordance", TASK_A, "n1")
    assert cache_path.exists()

    ws = Workspace(name="wsk", ws_root_dir=user_dir)
    ws.id = workspace_id
    ws.save(user_dir)

    assert cache_path.exists(), (
        "Cache file under data/artifacts/ must survive workspace.save() — "
        "this is the regression test for the dispersion-detach bug."
    )
