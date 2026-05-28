"""Token-frequency analysis request schema module.

Used by:
- token-frequency routes and worker task request validation because they need a backend
  boundary that validates inputs before delegating to workspace or worker state.
Why:
- Keeps token-frequency analysis input contract centralized.

Flow: normalize inputs, delegate to the owning backend state or service boundary, and
    return serialized values or existing domain errors to callers.
"""

from pydantic import Field

from ..models import BaseAnalysisRequest


class TokenFrequencyRequest(BaseAnalysisRequest):
    """Request model for token-frequency analysis.

    Used by:
    - token-frequency run/update endpoints because they need a backend boundary that
      validates inputs before delegating to workspace or worker state.
    Why:
    - Validates node selection, stop-word, and token-limit parameters.

    Flow: normalize inputs, delegate to the owning backend state or service boundary, and
        return serialized values or existing domain errors to callers.
    """

    node_ids: list[str] = Field(..., description="List of node IDs to analyze (1 or 2)")
    node_columns: dict[str, str] | None = Field(
        None, description="Map of node_id to column name"
    )
    stop_words: list[str] | None = Field(
        None, description="List of stop words to exclude"
    )
    token_limit: int | None = Field(
        None, description="Limit on number of tokens returned"
    )
    tokenizer_model: str | None = Field(
        None,
        description="Tokenizer model ID used for raw text token-frequency analysis",
    )
    node_tokenizer_models: dict[str, str] | None = Field(
        None,
        description="Map of node_id to tokenizer model ID for raw text token-frequency analysis",
    )
