"""User preference models persisted as JSON on disk."""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, ConfigDict, Field

from . import QuotationEngineConfig, QuotationEngineType

DEFAULT_HIDDEN_VIEWS: list[str] = ["ai-annotator"]

VALID_VIEWS: set[str] = {
    "data-loader",
    "filter",
    "token-frequency",
    "concordance",
    "analysis",
    "topic-modeling",
    "quotation",
    "ai-annotator",
    "export",
}

ALWAYS_VISIBLE_VIEWS: set[str] = {"data-loader"}


class QuotationPreferences(BaseModel):
    engine: QuotationEngineConfig = Field(
        default_factory=lambda: QuotationEngineConfig(type=QuotationEngineType.LOCAL)
    )
    last_remote_url: str = ""

    model_config = ConfigDict(extra="forbid")


class UserPreferences(BaseModel):
    hidden_views: list[str] = Field(default_factory=lambda: list(DEFAULT_HIDDEN_VIEWS))
    favorite_workspaces: list[str] = Field(default_factory=list)
    quotation: QuotationPreferences = Field(default_factory=QuotationPreferences)

    model_config = ConfigDict(extra="forbid")

    def validated(self) -> UserPreferences:
        """Return a copy with invalid view names stripped and always-visible views unhidden."""
        cleaned_hidden = [
            v
            for v in self.hidden_views
            if v in VALID_VIEWS and v not in ALWAYS_VISIBLE_VIEWS
        ]
        return self.model_copy(update={"hidden_views": cleaned_hidden})


class UserPreferencesUpdate(BaseModel):
    """Partial update payload — only provided fields are merged."""

    hidden_views: Optional[list[str]] = None
    favorite_workspaces: Optional[list[str]] = None
    quotation: Optional[QuotationPreferences] = None

    model_config = ConfigDict(extra="forbid")
