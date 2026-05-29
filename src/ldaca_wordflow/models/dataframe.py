"""DataFrame and node-lineage models.

Split from models/__init__.py.
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional
from pydantic import BaseModel

class DataFrameNode(BaseModel):
    """API schema used by routes and generated clients for data frame node.

    Used by:
    - backend request/response models because they need a stable JSON contract shared by
      route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    node_id: str
    name: str
    parent_id: Optional[str] = None
    parent_ids: Optional[List[str]] = None  # Enhanced: support multiple parents
    child_ids: Optional[List[str]] = None  # Enhanced: support multiple children
    operation: str
    shape: tuple
    columns: List[str]
    created_at: str
    preview: List[Dict[str, Any]]
    document: Optional[str] = None  # Enhanced: active document column for text data
    column_schema: Optional[Dict[str, str]] = (
        None  # Enhanced: column schema information
    )



class NodeLineage(BaseModel):
    """API schema used by routes and generated clients for node lineage.

    Used by:
    - backend request/response models because they need a stable JSON contract shared by
      route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    node_id: str
    ancestors: List[str]
    descendants: List[str]
    depth: int
    lineage_path: List[str]



class DataFrameInfo(BaseModel):
    """Metadata schema used by API responses to describe data frame info.

    Used by:
    - backend request/response models because they need a stable JSON contract shared by
      route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    node_id: str
    shape: tuple
    columns: List[str]
    dtypes: Dict[str, str]
    memory_usage: str
    is_text_data: bool  # Whether it's a text-oriented node
    document: Optional[str] = None  # Enhanced: document column for text data
    column_schema: Optional[Dict[str, str]] = (
        None  # Enhanced: column schema information
    )
    operation: Optional[str] = None  # Enhanced: operation that created this node
    parent_ids: Optional[List[str]] = None  # Enhanced: parent node IDs
    child_ids: Optional[List[str]] = None  # Enhanced: child node IDs


# =============================================================================
# DATA OPERATION MODELS
# =============================================================================



