from __future__ import annotations

import pytest

from ldaca_web_app.api import feedback as feedback_api


def test_require_airtable_config_returns_strings(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(feedback_api.settings, "airtable_api_key", "key123")
    monkeypatch.setattr(feedback_api.settings, "airtable_base_id", "app123")
    monkeypatch.setattr(feedback_api.settings, "airtable_table_id", "tbl123")

    assert feedback_api._require_airtable_config() == (
        "key123",
        "app123",
        "tbl123",
    )


def test_require_airtable_config_rejects_missing_values(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(feedback_api.settings, "airtable_api_key", "key123")
    monkeypatch.setattr(feedback_api.settings, "airtable_base_id", None)
    monkeypatch.setattr(feedback_api.settings, "airtable_table_id", "tbl123")

    with pytest.raises(RuntimeError, match="Airtable is not fully configured"):
        feedback_api._require_airtable_config()