"""Quotation analysis request/result schema module.

Used by:
- quotation analysis API routes and worker task payload marshalling because they need a
  backend boundary that validates inputs before delegating to workspace or worker state.
Why:
- Encapsulates quotation-specific request/result contracts.

Flow: normalize inputs, delegate to the owning backend state or service boundary, and
    return serialized values or existing domain errors to callers.
"""

from typing import Any

from pydantic import BaseModel, Field

from ..models import BaseAnalysisRequest
from ..results import BaseAnalysisResult


class QuotationEngineConfig(BaseModel):
    """Configuration schema for quotation extraction engines.

    Used by:
    - `QuotationRequest` because callers need the shared shared backend behavior rule in one
      place instead of duplicating it.
    Why:
    - Supports local and remote engine selection with optional model/auth fields.

    Flow: normalize inputs, delegate to the owning backend state or service boundary, and
        return serialized values or existing domain errors to callers.
    """

    type: str = "local"  # "local" or "remote"
    model: str | None = None
    url: str | None = None
    api_key: str | None = None


class QuotationRequest(BaseAnalysisRequest):
    """Request payload schema for quotation analysis.

    Used by:
    - quotation run/update endpoints because they need a backend boundary that validates
      inputs before delegating to workspace or worker state.
    Why:
    - Validates node/column/engine/paging options for quotation workflows.

    Flow: normalize inputs, delegate to the owning backend state or service boundary, and
        return serialized values or existing domain errors to callers.
    """

    node_id: str
    column: str
    engine: QuotationEngineConfig | None = None
    page: int | None = 1
    page_size: int | None = None
    sort_by: str | None = None
    descending: bool | None = True
    context_length: int | None = None
    materialized_path: str | None = None


class QuotationResult(BaseAnalysisResult):
    """Serializable quotation result wrapper.

    Used by:
    - analysis task result persistence/rehydration paths because analysis flows need
      per-user task state to survive across route calls and worker result persistence.
    Why:
    - Preserves arbitrary quotation payloads behind unified result interface.

    Flow: normalize inputs, delegate to the owning backend state or service boundary, and
        return serialized values or existing domain errors to callers.
    """

    def __init__(self, data: dict[str, Any]):
        """Initialize QuotationResult state used by quotation analysis adapters.

        Called by:
        - `QuotationResult` construction in backend services and tests because tests need the
          same observable contract that production routes and workers rely on.

        Flow: normalize inputs, delegate to the owning backend state or service boundary, and
            return serialized values or existing domain errors to callers.
        """

        self.data = data

    def to_json(self, **kwargs: Any) -> dict[str, Any]:
        """Return JSON-serializable quotation payload.

        Called by:
        - `QuotationResult` instances owned by backend services, routes, and tests because they
          need a backend boundary that validates inputs before delegating to workspace or worker
          state.

        Flow: normalize inputs, delegate to the owning backend state or service boundary, and
            return serialized values or existing domain errors to callers.
        """
        return self.data
