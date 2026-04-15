"""Tests for user preferences load / save / merge logic."""

import json
from pathlib import Path
from unittest.mock import patch

import pytest
from ldaca_web_app.core.preferences import (
    load_preferences,
    merge_preferences,
    save_preferences,
)
from ldaca_web_app.models.preferences import (
    ALWAYS_VISIBLE_VIEWS,
    DEFAULT_HIDDEN_VIEWS,
    QuotationPreferences,
    UserPreferences,
    UserPreferencesUpdate,
)


@pytest.fixture()
def user_data_dir(tmp_path: Path):
    """Patch get_user_data_folder to return tmp_path/user_data.

    _preferences_path uses .parent on that, so preferences.json lands in tmp_path.
    The fixture yields tmp_path (the user root) so tests write files there.
    """
    data_dir = tmp_path / "user_data"
    data_dir.mkdir()
    with patch(
        "ldaca_web_app.core.preferences.get_user_data_folder", return_value=data_dir
    ):
        yield tmp_path


class TestLoadPreferences:
    def test_returns_defaults_when_file_missing(self, user_data_dir: Path):
        prefs = load_preferences("test-user")
        assert prefs.hidden_views == DEFAULT_HIDDEN_VIEWS
        assert prefs.favorite_workspaces == []
        assert prefs.quotation.engine.type.value == "local"

    def test_loads_from_disk(self, user_data_dir: Path):
        payload = {"hidden_views": ["export"], "favorite_workspaces": ["ws-1"]}
        (user_data_dir / "preferences.json").write_text(json.dumps(payload))
        prefs = load_preferences("test-user")
        assert prefs.hidden_views == ["export"]
        assert prefs.favorite_workspaces == ["ws-1"]

    def test_returns_defaults_on_corrupt_json(self, user_data_dir: Path):
        (user_data_dir / "preferences.json").write_text("{bad json!!!")
        prefs = load_preferences("test-user")
        assert prefs == UserPreferences().validated()

    def test_strips_invalid_view_names(self, user_data_dir: Path):
        payload = {"hidden_views": ["export", "nonexistent-view"]}
        (user_data_dir / "preferences.json").write_text(json.dumps(payload))
        prefs = load_preferences("test-user")
        assert "nonexistent-view" not in prefs.hidden_views
        assert "export" in prefs.hidden_views

    def test_cannot_hide_data_loader(self, user_data_dir: Path):
        payload = {"hidden_views": ["data-loader", "export"]}
        (user_data_dir / "preferences.json").write_text(json.dumps(payload))
        prefs = load_preferences("test-user")
        assert "data-loader" not in prefs.hidden_views
        assert "export" in prefs.hidden_views


class TestSavePreferences:
    def test_round_trip(self, user_data_dir: Path):
        prefs = UserPreferences(
            hidden_views=["ai-annotator", "export"],
            favorite_workspaces=["ws-2"],
        )
        saved = save_preferences("test-user", prefs)
        assert saved.hidden_views == ["ai-annotator", "export"]

        loaded = load_preferences("test-user")
        assert loaded == saved

    def test_validated_on_save(self, user_data_dir: Path):
        prefs = UserPreferences(hidden_views=["data-loader"])
        saved = save_preferences("test-user", prefs)
        assert "data-loader" not in saved.hidden_views


class TestMergePreferences:
    def test_partial_update_preserves_other_fields(self):
        current = UserPreferences(
            hidden_views=["ai-annotator"],
            favorite_workspaces=["ws-1"],
        )
        update = UserPreferencesUpdate(hidden_views=["export"])
        merged = merge_preferences(current, update)
        assert merged.hidden_views == ["export"]
        assert merged.favorite_workspaces == ["ws-1"]

    def test_empty_update_is_noop(self):
        current = UserPreferences(hidden_views=["ai-annotator"])
        update = UserPreferencesUpdate()
        merged = merge_preferences(current, update)
        assert merged == current.validated()

    def test_quotation_merge(self):
        current = UserPreferences()
        update = UserPreferencesUpdate(
            quotation=QuotationPreferences(last_remote_url="http://example.com")
        )
        merged = merge_preferences(current, update)
        assert merged.quotation.last_remote_url == "http://example.com"


class TestAlwaysVisibleViews:
    def test_data_loader_in_always_visible(self):
        assert "data-loader" in ALWAYS_VISIBLE_VIEWS
