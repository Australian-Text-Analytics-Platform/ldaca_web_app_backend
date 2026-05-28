"""User preference models persisted as JSON on disk."""

from __future__ import annotations

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
    # ``None`` falls back to the per-request resolution chain.
    default_language: str | None = None
    default_tokenizer_model: str | None = None
    ldaca_oni_api_token: str | None = None
    # Demo-snapshot master switch. Default off — the analytic tools'
    # Save/Load buttons are unmounted entirely until the user opts in
    # via the sidebar dropdown menu. See ``docs/snapshot-view/plan.md``
    # §3.6 in the wordflow repo.
    demo_snapshots_enabled: bool = False

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

    hidden_views: list[str] | None = None
    favorite_workspaces: list[str] | None = None
    quotation: QuotationPreferences | None = None
    default_language: str | None = None
    default_tokenizer_model: str | None = None
    ldaca_oni_api_token: str | None = None
    demo_snapshots_enabled: bool | None = None

    model_config = ConfigDict(extra="forbid")
