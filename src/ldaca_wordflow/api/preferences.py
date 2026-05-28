"""User preferences REST endpoints.

Used by:
- FastAPI router registration, frontend API clients, and backend tests because they need this unit's "User preferences REST endpoints" behavior.

Flow:
- FastAPI mounts these endpoints under the preferences API prefix.
- Route handlers resolve the authenticated user and load the persisted preferences model.
- Updates merge typed preference patches before saving and returning the canonical state.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends

from ..core.auth import get_current_user
from ..core.preferences import load_preferences, merge_preferences, save_preferences
from ..models.preferences import UserPreferences, UserPreferencesUpdate

router = APIRouter(prefix="/preferences", tags=["preferences"])


@router.get("/", response_model=UserPreferences)
async def get_preferences(user: dict = Depends(get_current_user)):
    """Return get preferences API requests for preference routes.

    Flow:
    - Resolve authentication and request parameters from FastAPI dependencies.
    - Delegate validation, manager calls, artifacts, or state changes to the owning helper.
    - Shape the response payload or raise the HTTP error the client should see.

    Used by:
    - Frontend and API clients through the FastAPI GET / route because they need this unit's "Return get preferences API requests for preference routes" behavior.
    """

    return load_preferences(user["id"])


@router.put("/", response_model=UserPreferences)
async def update_preferences(
    body: UserPreferencesUpdate,
    user: dict = Depends(get_current_user),
):
    """Update update preferences API requests for preference routes.

    Flow:
    - Resolve authentication and request parameters from FastAPI dependencies.
    - Delegate validation, manager calls, artifacts, or state changes to the owning helper.
    - Shape the response payload or raise the HTTP error the client should see.

    Used by:
    - Frontend and API clients through the FastAPI PUT / route because they need this unit's "Update update preferences API requests for preference routes" behavior.
    """

    current = load_preferences(user["id"])
    merged = merge_preferences(current, body)
    return save_preferences(user["id"], merged)
