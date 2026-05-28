"""Phase 4.1: ``UserPreferences`` carries optional ``default_language`` and
``default_tokenizer_model`` so the frontend can persist a per-user choice.

Both fields default to ``None`` (no preference) so existing users see no
behaviour change. The merge helper honors the partial-update contract:
``None`` means "no change", any value means "set to this".
"""

from __future__ import annotations

from ldaca_wordflow.core.preferences import merge_preferences
from ldaca_wordflow.models.preferences import (
    UserPreferences,
    UserPreferencesUpdate,
)


def test_user_preferences_defaults_language_and_tokenizer_to_none() -> None:
    prefs = UserPreferences()
    assert prefs.default_language is None
    assert prefs.default_tokenizer_model is None
    assert prefs.ldaca_oni_api_token is None


def test_merge_preferences_sets_default_language() -> None:
    current = UserPreferences()
    update = UserPreferencesUpdate(default_language="zh")
    merged = merge_preferences(current, update)
    assert merged.default_language == "zh"
    assert merged.default_tokenizer_model is None  # untouched


def test_merge_preferences_sets_tokenizer_model_independently() -> None:
    current = UserPreferences(default_language="zh")
    update = UserPreferencesUpdate(default_tokenizer_model="lindera:jieba")
    merged = merge_preferences(current, update)
    assert merged.default_language == "zh"  # preserved
    assert merged.default_tokenizer_model == "lindera:jieba"


def test_merge_preferences_none_means_no_change() -> None:
    """The partial-update contract: ``None`` on either new field doesn't
    erase an existing value. This mirrors how ``hidden_views`` /
    ``quotation`` already behave."""
    current = UserPreferences(
        default_language="ja", default_tokenizer_model="lindera:jieba"
    )
    update = UserPreferencesUpdate(favorite_workspaces=["a"])
    merged = merge_preferences(current, update)
    assert merged.default_language == "ja"
    assert merged.default_tokenizer_model == "lindera:jieba"
    assert merged.favorite_workspaces == ["a"]


def test_merge_preferences_sets_and_clears_ldaca_token() -> None:
    current = UserPreferences()
    saved = merge_preferences(
        current,
        UserPreferencesUpdate(ldaca_oni_api_token="portal-token"),
    )
    assert saved.ldaca_oni_api_token == "portal-token"

    cleared = merge_preferences(
        saved,
        UserPreferencesUpdate(ldaca_oni_api_token=None),
    )
    assert cleared.ldaca_oni_api_token is None


def test_round_trip_json_serialisation() -> None:
    """Persistence + REST contract: fields round-trip through JSON without
    surprises."""
    prefs = UserPreferences(
        default_language="zh",
        default_tokenizer_model="lindera:jieba",
    )
    serialized = prefs.model_dump_json()
    rehydrated = UserPreferences.model_validate_json(serialized)
    assert rehydrated.default_language == "zh"
    assert rehydrated.default_tokenizer_model == "lindera:jieba"
    assert rehydrated.ldaca_oni_api_token is None


def test_legacy_preferences_without_new_fields_load_with_defaults() -> None:
    """Backward compat: older preferences.json files lack the new fields;
    loading them must produce ``None`` for both rather than raising."""
    legacy_json = '{"hidden_views": ["ai-annotator"], "favorite_workspaces": []}'
    prefs = UserPreferences.model_validate_json(legacy_json)
    assert prefs.default_language is None
    assert prefs.default_tokenizer_model is None
    assert prefs.ldaca_oni_api_token is None
