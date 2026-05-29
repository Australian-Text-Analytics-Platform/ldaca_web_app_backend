"""Shared API response wrappers and file-preview models.

Split from models/__init__.py.
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional
from pydantic import BaseModel

class APIResponse(BaseModel):
    """Generic API response wrapper

    Used by:
    - backend request/response models because they need a stable JSON contract shared by
      route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None



class PaginatedResponse(BaseModel):
    """Generic paginated response

    Used by:
    - backend request/response models because they need a stable JSON contract shared by
      route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    data: List[Dict[str, Any]]
    page: int
    page_size: int
    total_items: int
    total_pages: int
    has_more: bool



class ErrorResponse(BaseModel):
    """Error response model

    Used by:
    - backend request/response models, core workspace and worker services because they need
      a stable JSON contract shared by route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    error: str
    detail: str
    status_code: int


# =============================================================================
# FILTER AND SLICE MODELS
# =============================================================================


# =============================================================================
# FILE PREVIEW MODELS (Unified endpoint)
# =============================================================================



class FilePreviewRequest(BaseModel):
    """Request schema used by API routes and generated clients for file preview request.

    Used by:
    - backend API routes, backend request/response models because they need a stable JSON
      contract shared by route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    filename: str
    page: int = 0
    page_size: int = 20
    payload: Optional[Dict[str, Any]] = None  # e.g., {"sheet_name": "Sheet1"}



class FilePreviewResponse(BaseModel):
    """Response schema returned by API routes and consumed by generated clients for file preview response.

    Used by:
    - backend API routes, backend request/response models because they need a stable JSON
      contract shared by route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    filename: str
    file_type: str
    supported_types: List[str]  # ["LazyFrame", "DataFrame"]
    columns: List[str]
    preview: List[Dict[str, Any]]
    total_rows: int
    sheet_names: Optional[List[str]] = None
    selected_sheet: Optional[str] = None



