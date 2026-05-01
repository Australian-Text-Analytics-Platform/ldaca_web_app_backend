"""User preferences REST endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from ..core.auth import get_current_user
from ..core.preferences import load_preferences, merge_preferences, save_preferences
from ..models.preferences import UserPreferences, UserPreferencesUpdate

router = APIRouter(prefix="/preferences", tags=["preferences"])


@router.get("/", response_model=UserPreferences)
async def get_preferences(user: dict = Depends(get_current_user)):
    return load_preferences(user["id"])


@router.put("/", response_model=UserPreferences)
async def update_preferences(
    body: UserPreferencesUpdate,
    user: dict = Depends(get_current_user),
):
    current = load_preferences(user["id"])
    merged = merge_preferences(current, body)
    return save_preferences(user["id"], merged)
