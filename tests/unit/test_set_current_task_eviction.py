"""TaskManager.set_current_task evicts the displaced task and its caches.

The frontend's mental model is that each analytic tab holds at most one
result at a time — clicking Run/Update replaces what was there. The backend
must mirror this: when a new task supersedes the previously-current task on
the same tab, the old task record and its on-disk analysis-cache parquets
should be removed.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

from ldaca_wordflow.analysis.manager import TaskManager
from ldaca_wordflow.analysis.models import BaseAnalysisRequest
from ldaca_wordflow.core import utils as core_utils
from ldaca_wordflow.core import workspace as workspace_module
from ldaca_wordflow.core.analysis_cache import materialized_cache_path
from ldaca_wordflow.core.utils import generate_workspace_id
from ldaca_wordflow.core.workspace import WorkspaceManager


def _bootstrap_workspace(
    manager: WorkspaceManager, user_id: str, name: str
) -> tuple[str, Path]:
    """Create a workspace dir and register it as current for the user."""
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
    manager.set_current_workspace(user_id, workspace_id)
    return workspace_id, target_dir


def _write_cache(
    workspace_dir: Path, feature: str, task_id: str, node_id: str
) -> Path:
    path = materialized_cache_path(workspace_dir, feature, task_id, node_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"a": [1]}).write_parquet(path)
    return path


@pytest.fixture
def isolated_manager(tmp_path, monkeypatch):
    monkeypatch.setattr(core_utils.settings, "multi_user", True, raising=False)
    monkeypatch.setattr(
        core_utils.settings, "user_data_folder", "users", raising=False
    )
    monkeypatch.setattr(core_utils.settings, "data_root", tmp_path, raising=False)

    manager = WorkspaceManager()
    monkeypatch.setattr(workspace_module, "workspace_manager", manager)
    return manager


@pytest.fixture
def reset_task_store(monkeypatch):
    """Give each test a clean per-user task store."""
    from ldaca_wordflow.analysis import manager as analysis_manager_module

    monkeypatch.setattr(analysis_manager_module, "_TASK_MANAGER_STORE", {})
    yield


def test_set_current_task_evicts_displaced_task_and_its_cache(
    isolated_manager, reset_task_store
):
    user_id = "user_one"
    _workspace_id, ws_dir = _bootstrap_workspace(isolated_manager, user_id, "ws")

    tm = TaskManager(user_id)
    task_a = tm.create_task(BaseAnalysisRequest())
    task_b = tm.create_task(BaseAnalysisRequest())

    cache_a = _write_cache(ws_dir, "concordance", task_a, "node-1")
    cache_b = _write_cache(ws_dir, "concordance", task_b, "node-1")

    tm.set_current_task("concordance", task_a)
    # First swap doesn't evict anything (no previous current).
    assert tm.get_task(task_a) is not None
    assert cache_a.exists()

    tm.set_current_task("concordance", task_b)
    # Now task_a is the displaced predecessor — its record and cache must go.
    assert tm.get_task(task_a) is None
    assert not cache_a.exists()
    # task_b's record + cache must be untouched.
    assert tm.get_task(task_b) is not None
    assert cache_b.exists()


def test_set_current_task_on_other_tab_does_not_evict_concordance(
    isolated_manager, reset_task_store
):
    """Eviction is per-tab. Changing the quotation tab must not touch the
    concordance tab's task or cache."""
    user_id = "user_one"
    _workspace_id, ws_dir = _bootstrap_workspace(isolated_manager, user_id, "ws")

    tm = TaskManager(user_id)
    conc_task = tm.create_task(BaseAnalysisRequest())
    quot_task_a = tm.create_task(BaseAnalysisRequest())
    quot_task_b = tm.create_task(BaseAnalysisRequest())

    conc_cache = _write_cache(ws_dir, "concordance", conc_task, "node-c")
    _quot_cache_a = _write_cache(ws_dir, "quotation", quot_task_a, "node-q")

    tm.set_current_task("concordance", conc_task)
    tm.set_current_task("quotation", quot_task_a)
    tm.set_current_task("quotation", quot_task_b)  # displaces quot_task_a only

    assert tm.get_task(conc_task) is not None
    assert conc_cache.exists()
    assert tm.get_task(quot_task_a) is None


def test_set_current_task_multi_user_isolation(isolated_manager, reset_task_store):
    """Eviction in one user must never touch another user's caches."""
    _wid_a, dir_a = _bootstrap_workspace(isolated_manager, "alpha", "shared")
    _wid_b, dir_b = _bootstrap_workspace(isolated_manager, "beta", "shared")

    tm_alpha = TaskManager("alpha")
    tm_beta = TaskManager("beta")

    # Currents on alpha — first swap doesn't evict anything.
    a_task_1 = tm_alpha.create_task(BaseAnalysisRequest())
    tm_alpha.set_current_task("concordance", a_task_1)
    # Beta has its own concordance task.
    b_task_1 = tm_beta.create_task(BaseAnalysisRequest())
    tm_beta.set_current_task("concordance", b_task_1)

    a_cache_1 = _write_cache(dir_a, "concordance", a_task_1, "n")
    b_cache_1 = _write_cache(dir_b, "concordance", b_task_1, "n")

    # Now displace alpha's current with a new alpha task.
    a_task_2 = tm_alpha.create_task(BaseAnalysisRequest())
    tm_alpha.set_current_task("concordance", a_task_2)

    assert not a_cache_1.exists(), "alpha's displaced cache must be removed"
    assert b_cache_1.exists(), (
        "beta's cache must be untouched by alpha's set_current_task — "
        "cleanup must be scoped by user_id+workspace_id, never global."
    )
    assert tm_alpha.get_task(a_task_1) is None
    assert tm_beta.get_task(b_task_1) is not None
