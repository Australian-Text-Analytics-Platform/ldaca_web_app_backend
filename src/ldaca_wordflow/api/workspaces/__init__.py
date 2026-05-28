"""Unified workspaces API package.

Exports a single FastAPI `router` that combines core workspace endpoints
(`base.py`) and modular analysis endpoints under `analyses/`.

Used by:
- `main.py` router registration because they need this unit's "Unified workspaces API package" behavior.

Why:
- Keeps workspace endpoint composition centralized.

Flow:
- Create the package-level workspace router consumed by `main.py`.
- Include lifecycle, node, base, analysis, and UI-state subrouters in one place.
- Re-export the workspace manager so existing tests can patch the historical path.

Refactor note:
- Router includes are manually enumerated; if module count grows, consider
    declarative router registration to reduce merge conflicts.
"""

from fastapi import APIRouter

from ...core.workspace import (
    workspace_manager,  # re-export for test patches expecting api.workspaces.workspace_manager
)
from . import base, lifecycle, nodes, ui_state
from .analyses import (
    ai_annotation,
    concordance,
    quotation,
    sequential_analysis,
    token_frequencies,
    topic_modeling,
)

# Aggregate routers. Subrouters already define their own prefixes.
router = APIRouter()
router.include_router(lifecycle.router)
router.include_router(nodes.router)
router.include_router(base.router)
router.include_router(token_frequencies.router)
router.include_router(sequential_analysis.router)
router.include_router(quotation.router)
router.include_router(concordance.router)
router.include_router(topic_modeling.router)
router.include_router(ai_annotation.router)
router.include_router(ui_state.router)

__all__ = ["router", "workspace_manager"]
