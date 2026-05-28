"""Shared helper for resolving current task IDs for an analysis type.

Used by:
- FastAPI workspace analysis routers, frontend analysis features, and backend tests because they need this unit's "Shared helper for resolving current task IDs for an analysis type" behavior.

Flow:
- Analysis routes pass all accepted key aliases for the tool they are querying.
- The helper reads current task IDs from the per-user analysis task manager.
- Duplicate and empty IDs are skipped before returning the API response shape.
"""

from __future__ import annotations

from ....analysis.manager import get_task_manager as get_analysis_task_manager


async def get_current_task_ids_for_analysis(
    user_id: str,
    analysis_keys: list[str],
) -> dict[str, list[str]]:
    """Return current task IDs for an analysis type.

    Each analysis type stores at most one current task ID per key.

    Parameters:
        user_id: The authenticated user's ID.
        analysis_keys: Candidate key forms for the analysis (e.g. ["token_frequencies", "token-frequencies"]).

    Returns:
        dict: {"task_ids": [...]}

    Steps:
    - Normalize caller input into the representation this module expects.
    - Delegate stateful, expensive, or validating work to the owning manager/helper when needed.
    - Return the compact value the caller uses for artifacts, validation, or response shaping.

    Used by:
    - backend API routes because they need this unit's "Return current task IDs for an analysis type" behavior.
    """
    manager = get_analysis_task_manager(user_id)

    task_ids: list[str] = []
    for key in analysis_keys:
        for task_id in manager.get_current_task_ids(key):
            if task_id and task_id not in task_ids:
                task_ids.append(task_id)

    return {"task_ids": task_ids}
