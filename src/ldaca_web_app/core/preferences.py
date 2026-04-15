"""Load and save user preferences as a JSON file on disk."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from ..core.utils import get_user_data_folder
from ..models.preferences import UserPreferences, UserPreferencesUpdate

logger = logging.getLogger(__name__)

PREFERENCES_FILENAME = "preferences.json"


def _preferences_path(user_id: str) -> Path:
    user_root = get_user_data_folder(user_id).parent
    return user_root / PREFERENCES_FILENAME


def load_preferences(user_id: str) -> UserPreferences:
    """Read preferences from disk, returning validated defaults when the file is
    missing or malformed."""
    path = _preferences_path(user_id)
    if not path.exists():
        return UserPreferences().validated()

    try:
        raw = path.read_text(encoding="utf-8")
        prefs = UserPreferences.model_validate_json(raw)
        return prefs.validated()
    except json.JSONDecodeError, ValueError:
        logger.warning("Corrupt preferences at %s – returning defaults", path)
        return UserPreferences().validated()


def save_preferences(user_id: str, prefs: UserPreferences) -> UserPreferences:
    """Validate and atomically write preferences to disk. Returns the saved copy."""
    clean = prefs.validated()
    path = _preferences_path(user_id)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(clean.model_dump_json(indent=2), encoding="utf-8")
    tmp.replace(path)
    return clean


def merge_preferences(
    current: UserPreferences, update: UserPreferencesUpdate
) -> UserPreferences:
    """Apply a partial update on top of current preferences."""
    overrides: dict[str, object] = {}
    if update.hidden_views is not None:
        overrides["hidden_views"] = update.hidden_views
    if update.favorite_workspaces is not None:
        overrides["favorite_workspaces"] = update.favorite_workspaces
    if update.quotation is not None:
        overrides["quotation"] = update.quotation
    merged = current.model_copy(update=overrides)
    return merged.validated()
