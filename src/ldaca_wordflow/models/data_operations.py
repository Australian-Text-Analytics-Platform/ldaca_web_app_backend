"""Dataframe operation models (filter, join, replace, cast, etc.).

Split from models/__init__.py.
"""

from __future__ import annotations
from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, Field

class DataOperation(BaseModel):
    """API schema used by routes and generated clients for data operation.

    Used by:
    - backend request/response models because they need a stable JSON contract shared by
      route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    operation_type: str  # 'filter', 'slice', 'transform', 'aggregate'
    parameters: Dict[str, Any]
    target_columns: Optional[List[str]] = None



class FilterOperation(BaseModel):
    """API schema used by routes and generated clients for filter operation.

    Used by:
    - backend request/response models because they need a stable JSON contract shared by
      route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    column: str
    operator: str  # 'eq', 'gt', 'lt', 'contains', 'regex'
    value: Any



class SliceOperation(BaseModel):
    """API schema used by routes and generated clients for slice operation.

    Used by:
    - backend request/response models because they need a stable JSON contract shared by
      route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    start_row: Optional[int] = None
    end_row: Optional[int] = None
    columns: Optional[List[str]] = None



class TransformOperation(BaseModel):
    """API schema used by routes and generated clients for transform operation.

    Used by:
    - backend request/response models because they need a stable JSON contract shared by
      route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    operation: str  # 'rename', 'add_column', 'drop_column', 'convert_type'
    parameters: Dict[str, Any]



class AggregateOperation(BaseModel):
    """API schema used by routes and generated clients for aggregate operation.

    Used by:
    - backend request/response models because they need a stable JSON contract shared by
      route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    group_by: Optional[List[str]] = None
    aggregations: Dict[str, str]  # column -> function



class ReplaceRequest(BaseModel):
    """Request schema used by API routes and generated clients for replace request.

    Used by:
    - backend API routes, backend request/response models because they need a stable JSON
      contract shared by route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    source_column: str = Field(..., min_length=1, max_length=200)
    pattern: str = Field(..., min_length=1)
    replacement: str = Field(default="")
    output_column_name: Optional[str] = Field(default=None, max_length=200)
    preview_limit: Optional[int] = Field(default=50, ge=1, le=500)
    mode: Literal["replace", "extract"] = Field(default="replace")
    count: Literal["all", "first"] = Field(default="all")
    n: Optional[int] = Field(default=None, ge=1)
    connector: str = Field(default=" ")



class ReplacePreviewResponse(BaseModel):
    """Response schema returned by API routes and consumed by generated clients for replace preview response.

    Used by:
    - backend request/response models because they need a stable JSON contract shared by
      route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    columns: List[str]
    dtypes: Dict[str, str]
    data: List[Dict[str, Any]]



class ReplaceApplyResponse(BaseModel):
    """Response schema returned by API routes and consumed by generated clients for replace apply response.

    Used by:
    - backend API routes, backend request/response models because they need a stable JSON
      contract shared by route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    state: Literal["successful"]
    node_id: str
    column_name: str
    dtype: Optional[str] = None
    message: str



class JoinRequest(BaseModel):
    """Request schema used by API routes and generated clients for join request.

    Used by:
    - backend request/response models because they need a stable JSON contract shared by
      route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    right_node_id: str
    join_type: str  # 'inner', 'left', 'right', 'outer'
    left_on: List[str]
    right_on: List[str]
    suffix: str = "_right"



class ConcatPreviewRequest(BaseModel):
    """Request schema used by API routes and generated clients for concat preview request.

    Used by:
    - backend API routes, backend request/response models, backend tests because they need a
      stable JSON contract shared by route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    node_ids: List[str] = Field(..., min_length=2)
    deduplicate: bool = True



class ConcatRequest(ConcatPreviewRequest):
    """Request schema used by API routes and generated clients for concat request.

    Used by:
    - backend API routes, backend request/response models, backend tests because they need a
      stable JSON contract shared by route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    new_node_name: Optional[str] = None



class NodeOperationResponse(BaseModel):
    """Response schema returned by API routes and consumed by generated clients for node operation response.

    Used by:
    - backend API routes, backend request/response models because they need a stable JSON
      contract shared by route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    node_name: str
    node_id: str



class NodeActionResponse(BaseModel):
    """Response schema returned by API routes and consumed by generated clients for node action response.

    Used by:
    - backend API routes, backend request/response models because they need a stable JSON
      contract shared by route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    state: Literal["successful"]
    message: str



class CastNodeRequest(BaseModel):
    """Request schema used by API routes and generated clients for cast node request.

    Used by:
    - backend API routes, backend request/response models because they need a stable JSON
      contract shared by route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    column: str
    target_type: str
    format: str | None = None
    strict: bool | None = None



class CastNodeInfo(BaseModel):
    """Metadata schema used by API responses to describe cast node info.

    Used by:
    - backend request/response models because they need a stable JSON contract shared by
      route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    column: str
    original_type: str
    new_type: str
    target_type: str
    format_used: str | None = None
    strict_used: bool | None = None



class CastNodeResponse(BaseModel):
    """Response schema returned by API routes and consumed by generated clients for cast node response.

    Used by:
    - backend API routes, backend request/response models because they need a stable JSON
      contract shared by route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    state: Literal["successful"]
    node_id: str
    cast_info: CastNodeInfo
    message: str



class DataFrameOperationRequest(BaseModel):
    """Request schema used by API routes and generated clients for data frame operation request.

    Used by:
    - backend request/response models because they need a stable JSON contract shared by
      route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    workspace_id: str
    parent_node_id: str
    operation: DataOperation
    result_name: Optional[str] = None


# =============================================================================
# TEXT ANALYSIS MODELS
# =============================================================================



