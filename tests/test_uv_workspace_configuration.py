from __future__ import annotations

import tomllib
from pathlib import Path


def _load_toml(path: Path) -> dict:
    with path.open("rb") as fh:
        return tomllib.load(fh)


def _backend_repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def test_python_uv_workspace_is_rooted_in_repository_directory() -> None:
    backend_repo_root = _backend_repo_root()
    backend_config = _load_toml(backend_repo_root / "pyproject.toml")

    assert "workspace" not in backend_config.get("tool", {}).get("uv", {})

    monorepo_root = backend_repo_root.parent
    monorepo_pyproject = monorepo_root / "pyproject.toml"
    if not monorepo_pyproject.exists():
        return

    root_config = _load_toml(monorepo_pyproject)
    assert root_config["project"]["dependencies"] == [
        "ldaca-web-app-backend>=0.2.0",
        "ldaca-loader",
    ]
    assert "workspace" not in root_config.get("tool", {}).get("uv", {})
    assert root_config["tool"]["uv"]["sources"]["ldaca-loader"] == {
        "git": "https://github.com/Australian-Text-Analytics-Platform/ldaca-tabulator",
        "branch": "ldaca_web_app_integration",
    }


def test_backend_workspace_declares_local_sources_only_for_workspace_members() -> None:
    backend_config = _load_toml(_backend_repo_root() / "pyproject.toml")
    backend_sources = backend_config["tool"]["uv"]["sources"]

    assert backend_sources["ldaca-loader"] == {
        "git": "https://github.com/Australian-Text-Analytics-Platform/ldaca-tabulator",
        "branch": "ldaca_web_app_integration",
    }
    assert "docworkspace" not in backend_sources
    assert "polars-text" not in backend_sources
