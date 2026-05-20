"""Quotation analysis request/result schema module.

Used by:
- quotation analysis API routes and worker task payload marshalling

Why:
- Encapsulates quotation-specific request/result contracts.
"""

from typing import Any

from pydantic import BaseModel, Field

from ..models import BaseAnalysisRequest
from ..results import BaseAnalysisResult


class QuotationEngineConfig(BaseModel):
    """Configuration schema for quotation extraction engines.

    Used by:
    - `QuotationRequest`

    Why:
    - Supports local and remote engine selection with optional model/auth fields.
    """

    type: str = "local"  # "local" or "remote"
    model: str | None = None
    url: str | None = None
    api_key: str | None = None


class QuotationRequest(BaseAnalysisRequest):
    """Request payload schema for quotation analysis.

    Used by:
    - quotation run/update endpoints

    Why:
    - Validates node/column/engine/paging options for quotation workflows.
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
    - analysis task result persistence/rehydration paths

    Why:
    - Preserves arbitrary quotation payloads behind unified result interface.
    """

    def __init__(self, data: dict[str, Any]):
        self.data = data

    def to_json(self, **kwargs: Any) -> dict[str, Any]:
        """Return JSON-serializable quotation payload."""
        return self.data
