"""Concordance analysis request/result schema module.

Used by:
- concordance analysis API routes and task persistence payloads

Why:
- Keeps concordance-specific request/result contracts separated from route logic.
"""

from typing import Any, Literal

from pydantic import Field

from ..models import BaseAnalysisRequest
from ..results import BaseAnalysisResult


class ConcordanceRequest(BaseAnalysisRequest):
    """Request payload schema for concordance runs.

    Used by:
    - concordance route/task creation flows

    Why:
    - Validates all search/context/pagination-related analysis inputs.
    """

    node_ids: list[str]
    node_columns: dict[str, str] | None = None
    search_word: str
    num_left_tokens: int = 50
    num_right_tokens: int = 50
    regex: bool = False
    whole_word: bool = False
    case_sensitive: bool = False
    combined: bool = False
    # Engine selector: "regex" walks raw text (default, preserves partial-word
    # patterns); "tokens" walks the active node's tokenization column for
    # exact-token matches with N-actual-token context. Persisted on the task
    # so hydration replays the same engine.
    search_mode: Literal["regex", "tokens"] = "regex"
    # Phase 4.4 language hint — resolver chain falls back to tokenization metadata
    # then "en" when this is None.
    language: str | None = None
    # node_id -> parquet path holding flattened occurrence rows.
    # Populated when a materialize background task completes for that node.
    materialized_paths: dict[str, str] | None = None


class ConcordanceResult(BaseAnalysisResult):
    """Serializable concordance result wrapper.

    Used by:
    - analysis task result serialization helpers

    Why:
    - Provides a consistent JSON output shape for concordance payloads.
    """

    def __init__(self, results: list[dict[str, Any]], total_hits: int):
        self.results = results
        self.total_hits = total_hits

    def to_json(self, **kwargs: Any) -> dict[str, Any]:
        """Return JSON-serializable concordance payload."""
        return {
            "results": self.results,
            "total_hits": self.total_hits,
        }
