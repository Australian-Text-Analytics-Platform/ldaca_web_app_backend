"""Pydantic request/response models for backend API contracts.

Used by:
- API routers and worker result serialization boundaries

Why:
- Centralizes schema contracts shared between frontend and backend endpoints.

Refactor note:
- This module is large and mixes domains (auth/files/workspaces/analysis); future
    split by domain files could improve maintainability and import clarity.
"""

from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field, model_validator

from ..analysis.models import BaseAnalysisRequest

# =============================================================================
# AUTHENTICATION MODELS
# =============================================================================


class User(BaseModel):
    id: str
    email: str
    name: str
    picture: Optional[str] = None
    is_active: Optional[bool] = None
    is_verified: Optional[bool] = None
    created_at: Optional[str] = None
    last_login: Optional[str] = None


class AuthMethod(BaseModel):
    name: str  # "google", "github", etc. (changed from 'type' to match frontend)
    display_name: str
    enabled: bool


class AuthInfoResponse(BaseModel):
    """Main auth info response - tells frontend everything it needs to know"""

    authenticated: bool
    user: Optional[User] = None
    available_auth_methods: List[AuthMethod] = []
    requires_authentication: bool
    data_folder: Optional[str] = None


class GoogleIn(BaseModel):
    id_token: str


class GoogleOut(BaseModel):
    access_token: str
    refresh_token: str
    expires_in: int
    scope: str
    token_type: str
    user: User  # Updated to use User instead of UserInfo


class UserResponse(BaseModel):
    id: str  # UUID string, not integer
    email: str
    name: str
    picture: Optional[str] = None  # Made optional
    is_active: bool
    is_verified: bool
    created_at: str  # Will be converted from datetime
    last_login: str  # Will be converted from datetime


# =============================================================================
# USER MANAGEMENT MODELS
# =============================================================================


# =============================================================================
# FILE MANAGEMENT MODELS
# =============================================================================


class FileUploadResponse(BaseModel):
    filename: str
    size: int
    upload_time: str
    file_type: str
    preview_available: bool


class ImportSampleDataResponse(BaseModel):
    status: str
    removed_existing: bool
    file_count: int
    bytes_copied: int
    message: str
    sample_dir: Optional[str] = None
    remote_download_started: bool = False


class ImportSampleDataRequest(BaseModel):
    collection_ids: List[str] = Field(default_factory=list)


class SampleDataFileEntry(BaseModel):
    path: str
    size: int
    sha256: str


class SampleDataCollection(BaseModel):
    id: str
    name: str
    description: str
    language: str
    bundled: bool
    total_size_bytes: int
    recommended_for: List[str]
    files: List[SampleDataFileEntry]
    status: str  # bundled | downloaded | partial | not_downloaded


class SampleDataCatalogueResponse(BaseModel):
    schema_version: int
    collections: List[SampleDataCollection]


# ── Demo-snapshot catalogue ───────────────────────────────────────────────
#
# Parallel to the sample-data catalogue above, but each entry describes a
# single ``.ldaca-snapshot`` bundle hosted in the sample-data repo under
# ``demo_snapshots/``. The frontend renders these as a second tab in the
# import dialog; the importer downloads selected bundles into the user's
# snapshot folder (``get_user_snapshots_folder``) so each tool's Load
# dialog picks them up automatically.


class DemoSnapshotEntry(BaseModel):
    """A single demo-snapshot bundle in the catalogue."""

    id: str
    """Stable identifier (e.g. ``concordance-scl-tutorial``)."""
    filename: str
    """Bundle filename written to the user's snapshot folder. Must match
    the tool's filename convention (``<tool>-<name>.ldaca-snapshot``) so
    the tool-scoped Load dialog finds it."""
    path: str
    """Relative path inside the sample-data repo (e.g.
    ``demo_snapshots/concordance-scl-tutorial.ldaca-snapshot``)."""
    tool: str
    """Tool key from ``SnapshotToolKey`` (concordance / quotation /
    token_frequencies / sequential_analysis / topic_modeling)."""
    name: str
    """Human-readable label shown in the catalogue list."""
    description: str
    """One-line description shown under the name."""
    size: int
    """Bundle size in bytes."""
    sha256: str
    """Expected SHA-256 of the bundle. Verified on download and used for
    status computation (downloaded / conflict / not_downloaded)."""
    tool_version: Optional[str] = None
    """App version that captured the bundle. Informational."""
    recommended_dataset: Optional[str] = None
    """Catalogue collection id (e.g. ``SCL``) the snapshot was built on
    — informational so users can import the matching dataset alongside."""
    status: str = "not_downloaded"
    """Computed per-user: ``downloaded`` | ``not_downloaded`` | ``conflict``.
    ``conflict`` means a file with the same name exists locally but its
    SHA differs (older bundle, or the user's own save). The importer
    skips ``conflict`` rows unless the user opts in via ``replace_ids``."""


class DemoSnapshotsCatalogueResponse(BaseModel):
    schema_version: int
    snapshots: List[DemoSnapshotEntry]


class ImportDemoSnapshotsRequest(BaseModel):
    snapshot_ids: List[str] = Field(default_factory=list)
    """Snapshot ids to import. Empty list = no-op."""
    replace_ids: List[str] = Field(default_factory=list)
    """Subset of ``snapshot_ids`` for which the importer should replace an
    existing ``conflict`` local copy. Entries outside this list with a
    conflict are skipped."""


class DemoSnapshotImportResult(BaseModel):
    id: str
    filename: str
    status: str  # imported | replaced | skipped_existing | skipped_conflict | failed
    message: Optional[str] = None


class ImportDemoSnapshotsResponse(BaseModel):
    results: List[DemoSnapshotImportResult]
    snapshot_dir: str


class DataFileInfo(BaseModel):
    filename: str
    size: int
    created_at: str
    file_type: str


class LDaCAImportRequest(BaseModel):
    url: str
    filename: Optional[str] = None


class OniSearchRequest(BaseModel):
    method: str = Field(default="keyword")
    query: str = Field(default="")
    limit: int = Field(default=25, ge=1, le=100)
    offset: int = Field(default=0, ge=0)


class OniSearchResult(BaseModel):
    id: str
    crate_id: str | None = None
    title: str
    description: str | None = None
    types: list[str] = Field(default_factory=list)
    license: str | None = None
    importable: bool = True
    access: dict[str, Any] | None = None
    collections: list[str] = Field(default_factory=list)
    file_formats: list[str] = Field(default_factory=list)
    stats: dict[str, Any] = Field(default_factory=dict)


class OniSearchResponse(BaseModel):
    state: str
    data: list[OniSearchResult]
    message: str


class FileTreeNodeResponse(BaseModel):
    name: str
    path: str
    type: Literal["file", "directory"]
    size: Optional[int] = None
    children: Optional[List["FileTreeNodeResponse"]] = None

    @model_validator(mode="after")
    def normalize_directory_children(self) -> "FileTreeNodeResponse":
        if self.type == "directory" and self.children is None:
            self.children = []
        return self


FileTreeNodeResponse.model_rebuild()


class CreateFolderRequest(BaseModel):
    name: str
    parent_path: str = ""


class CreateFolderResponse(BaseModel):
    message: str
    path: str


class MoveFileRequest(BaseModel):
    source_path: str
    target_directory_path: str


class MessageResponse(BaseModel):
    message: str


class FilesTaskMetadataResponse(BaseModel):
    task_id: str


class FilesImportTaskStartResponse(BaseModel):
    state: str
    message: str
    metadata: FilesTaskMetadataResponse


class FilesTasksListResponse(BaseModel):
    state: str
    data: List[Dict[str, Any]]
    message: str


class FilesTaskActionDataResponse(BaseModel):
    cancelled: Optional[bool] = None
    cancelled_count: Optional[int] = None
    cleared_count: Optional[int] = None


class FilesTaskActionResponse(BaseModel):
    state: str
    data: FilesTaskActionDataResponse
    message: str


class FileInfoResponse(BaseModel):
    filename: str
    size_Byte: int
    created_at: float
    modified_at: float
    file_type: str


# =============================================================================
# WORKSPACE MODELS
# =============================================================================


class WorkspaceInfo(BaseModel):
    id: str
    name: str
    description: str = ""
    created_at: Optional[str] = None
    modified_at: Optional[str] = None
    total_nodes: int
    root_nodes: int
    leaf_nodes: int


class WorkspaceSummary(BaseModel):
    """Summary metadata for a workspace row in list responses."""

    id: str
    name: str
    description: str = ""
    created_at: str = ""
    modified_at: str = ""
    total_nodes: int = 0
    root_nodes: int = 0
    leaf_nodes: int = 0
    workspace_size_Byte: int = 0
    folder_name: Optional[str] = None


class WorkspaceCreateRequest(BaseModel):
    name: str
    description: Optional[str] = None


class WorkspaceSaveRequest(BaseModel):
    workspace_id: str
    name: Optional[str] = None
    description: Optional[str] = None


# =============================================================================
# DATAFRAME MODELS
# =============================================================================


class DataFrameNode(BaseModel):
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
    node_id: str
    ancestors: List[str]
    descendants: List[str]
    depth: int
    lineage_path: List[str]


class DataFrameInfo(BaseModel):
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


class DataOperation(BaseModel):
    operation_type: str  # 'filter', 'slice', 'transform', 'aggregate'
    parameters: Dict[str, Any]
    target_columns: Optional[List[str]] = None


class FilterOperation(BaseModel):
    column: str
    operator: str  # 'eq', 'gt', 'lt', 'contains', 'regex'
    value: Any


class SliceOperation(BaseModel):
    start_row: Optional[int] = None
    end_row: Optional[int] = None
    columns: Optional[List[str]] = None


class TransformOperation(BaseModel):
    operation: str  # 'rename', 'add_column', 'drop_column', 'convert_type'
    parameters: Dict[str, Any]


class AggregateOperation(BaseModel):
    group_by: Optional[List[str]] = None
    aggregations: Dict[str, str]  # column -> function


class ReplaceRequest(BaseModel):
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
    columns: List[str]
    dtypes: Dict[str, str]
    data: List[Dict[str, Any]]


class ReplaceApplyResponse(BaseModel):
    state: Literal["successful"]
    node_id: str
    column_name: str
    dtype: Optional[str] = None
    message: str


class JoinRequest(BaseModel):
    right_node_id: str
    join_type: str  # 'inner', 'left', 'right', 'outer'
    left_on: List[str]
    right_on: List[str]
    suffix: str = "_right"


class ConcatPreviewRequest(BaseModel):
    node_ids: List[str] = Field(..., min_length=2)
    deduplicate: bool = True


class ConcatRequest(ConcatPreviewRequest):
    new_node_name: Optional[str] = None


class DataFrameOperationRequest(BaseModel):
    workspace_id: str
    parent_node_id: str
    operation: DataOperation
    result_name: Optional[str] = None


# =============================================================================
# TEXT ANALYSIS MODELS
# =============================================================================


class TextSetupRequest(BaseModel):
    document_column: str
    content_column: Optional[str] = None
    auto_detect: bool = True


class DTMRequest(BaseModel):
    max_features: Optional[int] = 1000
    min_df: float = 0.01
    max_df: float = 0.95
    ngram_range: tuple = (1, 2)
    use_tfidf: bool = False


class KeywordExtractionRequest(BaseModel):
    method: str  # 'tfidf', 'count', 'custom'
    top_k: int = 20
    by_document: bool = False


class ConcordanceAnalysisRequest(BaseModel):
    node_ids: List[str]  # Support up to 2 nodes (1 = single node mode)
    node_columns: Dict[str, str]  # node_id -> column_name mapping
    search_word: str
    num_left_tokens: int = 10
    num_right_tokens: int = 10
    regex: bool = False
    whole_word: bool = False
    case_sensitive: bool = False
    combined: bool = False  # if true, backend builds a combined view across nodes
    # "regex" (default) uses the polars-text concordance engine on raw text,
    # preserving partial-word patterns like ``equ\w*`` for English users.
    # "tokens" looks up a tokenization column (decision 7) and walks it for
    # exact-token matches with N-actual-token left/right context — the
    # word-aware semantics CJK users want once Tokenise has been run.
    # Falls back to regex behaviour if no tokenization column exists.
    search_mode: Literal["regex", "tokens"] = "regex"
    # Phase 4.4 language hint: lets the frontend tell the backend what
    # language to assume (drives the resolver chain in core/i18n.py).
    # ``None`` defers to the active node's tokenization metadata then ``"en"``.
    language: Optional[str] = None
    # Sorting parameters
    sort_by: Optional[str] = None  # column name to sort by
    descending: bool = True

    model_config = ConfigDict(extra="forbid")


class ConcordanceDetachRequest(BaseModel):
    node_id: str
    column: str
    search_word: str
    num_left_tokens: int = 10
    num_right_tokens: int = 10
    regex: bool = False
    whole_word: bool = False
    case_sensitive: bool = False
    new_node_name: Optional[str] = None  # If not provided, will be auto-generated
    selected_columns: Optional[list[str]] = None
    materialized_path: Optional[str] = None  # Reuse existing flattened parquet


class ConcordanceDispersionDetachRequest(BaseModel):
    """Detach a per-document aggregation of concordance hits.

    Unlike `ConcordanceDetachRequest` (one row per hit), this produces one row
    per source document with the hits collected into `List<T>` columns and the
    raw match-window text rendered as a multi-line `CONC_extraction` string.

    `selected_bins` + `total_bins` optionally restrict the aggregation to hits
    whose `start_idx / doc_length` falls inside one of the selected bins (the
    chart's "in-range hits only" semantic).
    """

    column: str
    search_word: str
    num_left_tokens: int = 10
    num_right_tokens: int = 10
    regex: bool = False
    whole_word: bool = False
    case_sensitive: bool = False
    new_node_name: Optional[str] = None
    selected_columns: Optional[list[str]] = None
    materialized_path: Optional[str] = None
    # When the slow path runs (no materialized_path), the worker also writes
    # the materialised flat parquet so the user doesn't have to click "Process
    # All" separately before iterating on bin selections. The parent analysis
    # task id + this node's id are needed to publish the standard
    # `analysis_materialized` event back to the frontend.
    parent_task_id: Optional[str] = None
    selected_bins: Optional[list[int]] = None
    total_bins: Optional[int] = None
    # When the chart legend is filtered, the detach should aggregate only over
    # hits whose `CONC_matched_text` lands in this set. `None` means "all".
    # `match_case_insensitive` mirrors the chart's `lowercaseMatches` toggle:
    # when true, both the column and the candidate set are lowercased before
    # comparison so the filter agrees with the legend grouping.
    selected_matched_texts: Optional[list[str]] = None
    match_case_insensitive: bool = False


class ConcordanceMaterializeRequest(BaseModel):
    column: str
    search_word: str
    num_left_tokens: int = 10
    num_right_tokens: int = 10
    regex: bool = False
    whole_word: bool = False
    case_sensitive: bool = False
    # Mirror the live ``/concordance`` request — materialize must honour the
    # engine the user actually searched with. Defaults to ``"regex"`` so
    # existing English flows are byte-identical.
    search_mode: Literal["regex", "tokens"] = "regex"
    parent_task_id: str


class ConcordanceDetachNodeOption(BaseModel):
    node_id: str
    node_name: str
    text_column: Optional[str] = None
    available_columns: List[str]
    disabled_columns: List[str] = Field(default_factory=list)


class ConcordanceDetachOptionsResponse(BaseModel):
    state: str
    message: str
    data: Dict[str, List[ConcordanceDetachNodeOption]]
    metadata: Optional[Dict[str, Any]] = None


# Quotation requests (mirror concordance shape but without search parameters)
class QuotationEngineType(str, Enum):
    LOCAL = "local"
    REMOTE = "remote"


class QuotationEngineConfig(BaseModel):
    type: QuotationEngineType = QuotationEngineType.LOCAL
    url: Optional[AnyHttpUrl] = None

    @model_validator(mode="after")
    def _validate_remote(self) -> "QuotationEngineConfig":
        if self.type is QuotationEngineType.LOCAL:
            # Normalise to ensure we never persist stale URLs for local mode
            self.url = None
        elif self.url is None:
            raise ValueError("Remote quotation engines require a URL")
        return self

    model_config = ConfigDict(extra="forbid")


class QuotationRequest(BaseModel):
    column: str
    # Pagination parameters
    page: int = 1
    page_size: Optional[int] = None
    # Sorting parameters
    sort_by: Optional[str] = None  # column name to sort by
    descending: bool = True
    engine: Optional[QuotationEngineConfig] = None
    # Phase 3.6: quotation is English-only. The route resolves an effective
    # language and rejects non-EN with a typed UnsupportedLanguageError so
    # users see a clear "English-only" message rather than garbage output.
    # ``None`` falls back to the node's tokenization metadata (if it's been
    # tokenised) and then ``"en"``.
    language: Optional[str] = None

    model_config = ConfigDict(extra="forbid")


class QuotationDetachRequest(BaseModel):
    node_id: str
    column: str
    new_node_name: Optional[str] = None  # If not provided, will be auto-generated
    engine: Optional[QuotationEngineConfig] = None
    selected_columns: Optional[list[str]] = None
    materialized_path: Optional[str] = None  # Reuse existing flattened parquet
    language: Optional[str] = None

    model_config = ConfigDict(extra="forbid")


class QuotationMaterializeRequest(BaseModel):
    column: str
    engine: Optional[QuotationEngineConfig] = None
    parent_task_id: str
    language: Optional[str] = None

    model_config = ConfigDict(extra="forbid")


class QuotationDetachNodeOption(BaseModel):
    node_id: str
    node_name: str
    text_column: Optional[str] = None
    available_columns: List[str]
    disabled_columns: List[str] = Field(default_factory=list)


class QuotationDetachOptionsResponse(BaseModel):
    state: str
    message: str
    data: Dict[str, List[QuotationDetachNodeOption]]
    metadata: Optional[Dict[str, Any]] = None


class QuotationResultQuery(BaseModel):
    page: Optional[int] = None
    # Accepts the literal ``'all'`` for the snapshot capture path —
    # server caps at ``SNAPSHOT_ALL_PAGE_SIZE_CAP`` (see
    # api/workspaces/analyses/quotation.py).
    page_size: Optional[Union[int, Literal["all"]]] = None
    sort_by: Optional[str] = None
    descending: Optional[bool] = None
    context_length: Optional[int] = None
    update_only: bool = False

    model_config = ConfigDict(extra="forbid")


class SequentialAnalysisRequest(BaseModel):
    time_column: str
    group_by_columns: Optional[List[str]] = None
    # ``second`` and ``minute`` are valid backend frequencies but the
    # live UI's preset dropdown intentionally hides them — they're only
    # exposed in the Trends snapshot-capture dialog as the "finest time
    # bin" option, since fine-grained snapshots enable richer
    # client-side coarsening in the viewer. Live users wanting per-second
    # buckets can still reach them via the ``custom`` flow.
    frequency: Literal[
        "second",
        "minute",
        "hourly",
        "daily",
        "weekly",
        "monthly",
        "quarterly",
        "yearly",
        "custom",
    ] = "monthly"
    sort_by_time: bool = True
    column_type: Literal["datetime", "numeric"] = "datetime"
    numeric_origin: Optional[float] = None
    numeric_interval: Optional[float] = None
    custom_interval_value: Optional[int] = None
    custom_interval_unit: Optional[
        Literal["seconds", "minutes", "hours", "days", "weeks"]
    ] = None
    case_sensitive: bool = True

    @model_validator(mode="after")
    def validate_numeric_params(self) -> "SequentialAnalysisRequest":
        if self.column_type == "numeric":
            if self.numeric_interval is None or self.numeric_interval <= 0:
                raise ValueError(
                    "numeric_interval must be a positive number when column_type='numeric'"
                )
        if self.column_type == "datetime" and self.frequency == "custom":
            if self.custom_interval_value is None or self.custom_interval_value <= 0:
                raise ValueError(
                    "custom_interval_value must be a positive integer when frequency='custom'"
                )
            if self.custom_interval_unit is None:
                raise ValueError(
                    "custom_interval_unit is required when frequency='custom'"
                )
        return self

    # Pydantic v2 model config
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "time_column": "created_at",
                "group_by_columns": ["party", "electorate"],
                "frequency": "monthly",
                "sort_by_time": True,
                "column_type": "datetime",
                "numeric_origin": None,
                "numeric_interval": None,
                "custom_interval_value": None,
                "custom_interval_unit": None,
            }
        }
    )


class TextAnalysisInfo(BaseModel):
    document: Optional[str]
    avg_document_length: Optional[float]
    total_documents: int
    vocabulary_size: Optional[int]
    is_text_ready: bool


# =============================================================================
# RESPONSE MODELS
# =============================================================================


class APIResponse(BaseModel):
    """Generic API response wrapper"""

    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None


class PaginatedResponse(BaseModel):
    """Generic paginated response"""

    data: List[Dict[str, Any]]
    page: int
    page_size: int
    total_items: int
    total_pages: int
    has_more: bool


class ErrorResponse(BaseModel):
    """Error response model"""

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
    filename: str
    page: int = 0
    page_size: int = 20
    payload: Optional[Dict[str, Any]] = None  # e.g., {"sheet_name": "Sheet1"}


class FilePreviewResponse(BaseModel):
    filename: str
    file_type: str
    supported_types: List[str]  # ["LazyFrame", "DataFrame"]
    columns: List[str]
    preview: List[Dict[str, Any]]
    total_rows: int
    sheet_names: Optional[List[str]] = None
    selected_sheet: Optional[str] = None


class FilterCondition(BaseModel):
    column: str
    operator: str  # Allow any string to support new operators like 'between'
    value: Any
    id: Optional[str] = None  # Frontend includes this for tracking
    dataType: Optional[str] = None  # Frontend includes this for UI
    # New flags from frontend Filter UI
    negate: Optional[bool] = False
    regex: Optional[bool] = False
    case_sensitive: Optional[bool] = False


class FilterRequest(BaseModel):
    conditions: List[FilterCondition]
    logic: Optional[str] = "and"
    new_node_name: Optional[str] = None


class SliceRequest(BaseModel):
    mode: Literal["slice", "random_sample", "shuffle"] = "slice"
    offset: int = Field(default=0, ge=0)
    length: Optional[int] = Field(default=None, ge=0)
    sample_size: Optional[float] = Field(default=None, gt=0)
    random_seed: Optional[int] = Field(default=None, ge=0)
    new_node_name: Optional[str] = None

    @model_validator(mode="after")
    def validate_sampling_mode(self) -> "SliceRequest":
        if self.mode == "random_sample":
            if self.sample_size is None:
                raise ValueError("sample_size is required when mode is 'random_sample'")
            if self.sample_size >= 1 and self.sample_size != int(self.sample_size):
                raise ValueError(
                    "sample_size >= 1 must be an integer (absolute row count)"
                )
        return self


class PaginationInfo(BaseModel):
    page: int
    page_size: int
    total_rows: int
    total_pages: int
    has_next: bool
    has_prev: bool


class FilterPreviewResponse(BaseModel):
    data: List[Dict[str, Any]]
    columns: List[str]
    dtypes: Dict[str, str]
    pagination: PaginationInfo


# =============================================================================
# POLARS EXPRESSION MODELS
# =============================================================================


class PolarsExpressionContext(str, Enum):
    filter = "filter"
    with_columns = "with_columns"
    select = "select"
    sort = "sort"
    group_by_agg = "group_by_agg"


class PolarsExpressionItem(BaseModel):
    """A single polars expression supplied as a Python code string, e.g. ``pl.col('x') > 0``."""

    code: str  # Python expression string evaluated with pl available
    descending: Optional[bool] = None  # used only in sort context


class PolarsExpressionRequest(BaseModel):
    context: PolarsExpressionContext
    expressions: List[PolarsExpressionItem]
    # For group_by_agg: these are the grouping key expressions
    group_by_keys: Optional[List[PolarsExpressionItem]] = None
    new_node_name: Optional[str] = None


class PolarsExpressionApplyResponse(BaseModel):
    node_id: str
    node_name: str


# =============================================================================
# TOKEN FREQUENCY MODELS
# =============================================================================


class StopWordsPayload(BaseModel):
    stop_words: List[str]


class TokenFrequencyRequest(BaseModel):
    node_ids: List[str]  # 1 or 2 node IDs
    node_columns: Dict[str, str]  # Maps node_id -> column_name
    stop_words: Optional[List[str]] = None
    token_limit: Optional[int] = None
    # Pydantic v2 model config
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "node_ids": ["node1", "node2"],
                "node_columns": {"node1": "text_column", "node2": "content_column"},
                "stop_words": ["the", "and", "or"],
                "token_limit": 50,
            }
        },
    )


class TokenFrequencyData(BaseModel):
    token: str
    frequency: int


class TokenStatisticsData(BaseModel):
    """Token-level comparative statistics.

    Numeric statistics use a JSON-safe union to preserve semantic distinctions:
    - finite number -> float
    - positive infinity -> "+Inf"
    - negative infinity -> "-Inf"
    - missing/undefined -> None
    """

    token: str
    freq_reference: int  # OR - observed frequency in reference corpus
    freq_study: int  # OS - observed frequency in study corpus
    expected_reference: float | str | None  # Expected frequency in reference corpus
    expected_study: float | str | None  # Expected frequency in study corpus
    reference_total: int  # Total tokens in reference corpus
    study_total: int  # Total tokens in study corpus
    percent_reference: float | str | None  # %R - percentage in reference corpus
    percent_study: float | str | None  # %S - percentage in study corpus
    percent_diff: float | str | None  # %DIFF - percentage difference
    log_likelihood_llv: float | str | None  # LL - log likelihood G2 statistic
    bayes_factor_bic: float | str | None  # Bayes - Bayes factor (BIC)
    effect_size_ell: float | str | None  # ELL - effect size for log likelihood
    relative_risk: float | str | None = None  # RRisk - relative risk ratio
    log_ratio: float | str | None = None  # LogRatio - log of relative frequencies
    odds_ratio: float | str | None = None  # OddsRatio - odds ratio
    significance: str  # Significance level indicator


class TokenFrequencyNodeResult(BaseModel):
    data: List[TokenFrequencyData]
    columns: List[str] = ["token", "frequency"]
    # Optional metadata (e.g., server-side truncation info)
    metadata: Optional[Dict[str, Any]] = None


class TokenFrequencyResponse(BaseModel):
    """Unified response model for token frequency analysis."""

    state: Optional[str] = None  # 'successful', 'failed', 'running'
    message: str
    data: Optional[Dict[str, TokenFrequencyNodeResult]] = (
        None  # Maps node_name -> { data: [...], columns: [...] }
    )
    statistics: Optional[List[TokenStatisticsData]] = (
        None  # Statistical measures (only when comparing 2 nodes)
    )
    token_limit: Optional[int] = None
    analysis_params: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None
    stop_words: Optional[List[str]] = None


# =============================================================================
# AI ANNOTATION MODELS
# =============================================================================


class AiAnnotationClassDef(BaseModel):
    name: str
    description: str


class AiAnnotationExample(BaseModel):
    query: str
    classification: str


class AiAnnotationModelsRequest(BaseModel):
    base_url: Optional[str] = None
    api_key: Optional[str] = None


class AiAnnotationRequest(BaseAnalysisRequest):
    node_ids: List[str]
    node_columns: Dict[str, str]
    annotation_column: Optional[str] = None

    classes: List[AiAnnotationClassDef] = Field(min_length=1)
    examples: List[AiAnnotationExample] = Field(default_factory=list)

    model: str
    api_key: Optional[str] = None
    base_url: Optional[str] = None

    temperature: float = Field(default=1.0, gt=0)
    top_p: float = Field(default=1.0, gt=0, le=1.0)
    seed: Optional[int] = 42
    batch_size: int = Field(default=100, ge=1)
    # Phase 3.7: when set, the classification system prompt gains a line
    # like "Texts are in Chinese." so the LLM doesn't mistake CJK for noise.
    # ``None`` falls back to ``effective_language(None, node)`` per node,
    # which keeps existing English flows unchanged (default = "en").
    language: Optional[str] = None

    page: int = 1
    page_size: int = 20
    sort_by: Optional[str] = None
    descending: bool = True

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "node_ids": ["node1"],
                "node_columns": {"node1": "document"},
                "classes": [
                    {"name": "support", "description": "Supportive tone"},
                    {"name": "critical", "description": "Critical tone"},
                ],
                "examples": [
                    {
                        "query": "This policy is fantastic and fair.",
                        "classification": "support",
                    }
                ],
                "model": "gpt-4o-mini",
                "temperature": 0.7,
                "top_p": 0.9,
            }
        }
    )


class AiAnnotationDetachRequest(BaseModel):
    column: str
    new_node_name: Optional[str] = None
    annotation_column: Optional[str] = None

    classes: List[AiAnnotationClassDef] = Field(min_length=1)
    examples: List[AiAnnotationExample] = Field(default_factory=list)

    model: str
    api_key: Optional[str] = None
    base_url: Optional[str] = None

    temperature: float = Field(default=1.0, gt=0)
    top_p: float = Field(default=1.0, gt=0, le=1.0)
    seed: Optional[int] = 42
    batch_size: int = Field(default=100, ge=1)
    # Phase 3.7: optional language hint surfaced to the LLM prompt; falls
    # back to the node's tokenization metadata then to ``"en"``.
    language: Optional[str] = None


class AiAnnotationEdit(BaseModel):
    row_index: int = Field(ge=0)
    provider: str = Field(min_length=1)
    annotation: str = ""


class AiAnnotationSaveRequest(BaseModel):
    annotation_column: Optional[str] = None
    edits: List[AiAnnotationEdit] = Field(default_factory=list)


class AiAnnotationNodeResult(BaseModel):
    data: List[Dict[str, Any]]
    columns: List[str]
    metadata: Optional[Dict[str, Any]] = None
    pagination: Optional[Dict[str, Any]] = None
    sorting: Optional[Dict[str, Any]] = None


class AiAnnotationResultQuery(BaseModel):
    page: Optional[int] = None
    page_size: Optional[int] = None
    sort_by: Optional[str] = None
    descending: Optional[bool] = None

    model_config = ConfigDict(extra="forbid")


class AiAnnotationResponse(BaseModel):
    state: str
    message: str
    data: Optional[Dict[str, AiAnnotationNodeResult]] = None
    analysis_params: Optional[Dict[str, Any]] = None
    combinable: Optional[bool] = None
    metadata: Optional[Dict[str, Any]] = None


# =============================================================================
# TOPIC MODELING MODELS
# =============================================================================


class TopicModelingRequest(BaseModel):
    node_ids: List[str]  # 1 or 2 node IDs
    node_columns: Dict[str, str]  # Maps node_id -> column_name
    min_topic_size: Optional[int] = (
        10  # kept for backwards compat; ignored when topic_size_mode != "min"
    )
    random_seed: Optional[int] = 42
    representative_words_count: Optional[int] = 5
    # Sampling: one entry per corpus in node_ids order. None = no sampling for that corpus.
    sample_fractions: Optional[List[Optional[float]]] = None
    # Topic size mode: controls how min_topic_size is derived
    topic_size_mode: Optional[Literal["target", "min", "exact"]] = "target"
    topic_size_value: Optional[int] = 25
    # Phase 3.5: controls the per-topic LABEL stage's CountVectorizer stopword
    # filter (not the clustering stage). Default ``None`` falls back to
    # ``effective_language(...)`` per node. English uses sklearn's "english"
    # list; other languages get ``None`` so Chinese function words aren't
    # English-filtered (and so don't dominate every topic label).
    language: Optional[str] = None

    # Pydantic v2 model config
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "node_ids": ["node1", "node2"],
                "node_columns": {"node1": "text", "node2": "content"},
                "random_seed": 42,
                "representative_words_count": 5,
                "sample_fractions": [0.2, 0.5],
                "topic_size_mode": "target",
                "topic_size_value": 25,
            }
        }
    )


class TopicModelingTopic(BaseModel):
    id: int
    label: str
    representative_words: List[str] = Field(default_factory=list)
    size: List[int]  # per-corpus sizes aligned to request.node_ids order
    total_size: int
    x: float
    y: float


class TopicModelingData(BaseModel):
    topics: List[TopicModelingTopic]
    corpus_sizes: List[int]
    per_corpus_topic_counts: Optional[List[Dict[int, int]]] = None
    meta: Optional[Dict[str, Any]] = None


class TopicModelingResponse(BaseModel):
    state: str  # 'successful', 'failed', 'running', 'cancelled'
    message: str
    data: Optional[TopicModelingData] = None
    metadata: Optional[Dict[str, Any]] = None


class TopicMeaningOverrideItem(BaseModel):
    """One topic's representative-words override for detach.

    Lets the frontend ship exactly what the user sees — post-fit
    "Words per topic" slice, post-fit stopword filter — instead of
    forcing the meanings parquet (written at fit time) into the
    detached node.
    """

    topic_id: int
    words: List[str]


class TopicModelingDetachRequest(BaseModel):
    """Request payload for detaching topic assignments from cached topic-modeling output."""

    node_ids: Optional[List[str]] = None
    selected_columns: Dict[str, List[str]] = Field(default_factory=dict)
    new_node_names: Optional[Dict[str, str]] = None
    topic_column_name: Optional[str] = "TOPIC_topic"
    topic_ids: Optional[List[int]] = None
    topic_meanings_override: Optional[List[TopicMeaningOverrideItem]] = None


class TopicModelingDetachNodeOption(BaseModel):
    node_id: str
    node_name: str
    text_column: Optional[str] = None
    available_columns: List[str]
    disabled_columns: List[str] = Field(default_factory=list)


class TopicModelingDetachOptionsResponse(BaseModel):
    state: str
    message: str
    data: Dict[str, List[TopicModelingDetachNodeOption]]
    metadata: Optional[Dict[str, Any]] = None


class TopicModelingDetachResponse(BaseModel):
    state: str
    message: str
    data: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None


# Concordance response models
class ConcordanceMetadata(BaseModel):
    """Metadata about concordance columns to help frontend display logic"""

    concordance_columns: List[
        str
    ]  # Core concordance columns (CONC_left_context, CONC_matched_text, CONC_right_context, etc.)
    metadata_columns: List[str]  # Original document metadata columns
    all_columns: List[str]  # All available columns


class ConcordanceNodeResult(BaseModel):
    """Per-node concordance payload returned to the frontend."""

    data: List[Dict[str, Any]]
    columns: List[str]
    metadata: ConcordanceMetadata
    total_matches: int
    pagination: Dict[str, Any]
    sorting: Dict[str, Any]


class ConcordanceAnalysisResponse(BaseModel):
    """Unified concordance response for single or multi-node requests."""

    state: str  # 'successful', 'failed', 'running', 'cancelled'
    message: str
    data: Dict[
        str, Dict[str, Any]
    ]  # node label -> ConcordanceNodeResult | combined summary
    analysis_params: Optional[Dict[str, Any]] = None


# =============================================================================
# COLUMN DESCRIBE MODELS
# =============================================================================


class ColumnDescribeResponse(BaseModel):
    """Response model for column describe statistics."""

    column_name: str
    count: Optional[int] = None
    null_count: Optional[int] = None
    mean: Optional[Any] = None  # Can be float (numeric) or str (datetime ISO format)
    std: Optional[Any] = None  # Can be float (numeric) or None (datetime)
    min: Optional[Any] = None
    percentile_25: Optional[Any] = None
    median: Optional[Any] = None
    percentile_75: Optional[Any] = None
    max: Optional[Any] = None
