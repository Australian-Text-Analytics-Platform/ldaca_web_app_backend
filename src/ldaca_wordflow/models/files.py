"""File management, snapshot, import, and data catalogue models.

Split from models/__init__.py.
"""

from __future__ import annotations
from typing import Any, Dict, List, Literal, Optional, Union
from typing_extensions import TypedDict
from pydantic import BaseModel, Field, model_validator


class TaskEntry(TypedDict, total=False):
    """Dict shape of a serialized task from ``WorkerTaskManager._serialize_task``.

    Keys match ``core/worker_task_manager.py:_serialize_task``.
    """

    task_id: str
    task_type: str
    name: str
    user_id: str
    workspace_id: str
    state: str
    created_at: str
    started_at: str | None
    finished_at: str | None
    progress: float
    progress_message: str | None
    parent_task_id: str | None

class FileUploadResponse(BaseModel):
    """Response schema returned by API routes and consumed by generated clients for file upload response.

    Used by:
    - backend API routes, backend request/response models because they need a stable JSON
      contract shared by route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    filename: str
    size: int
    upload_time: str
    file_type: str
    preview_available: bool



class ImportSampleDataResponse(BaseModel):
    """Response schema returned by API routes and consumed by generated clients for import sample data response.

    Used by:
    - backend API routes, backend request/response models because they need a stable JSON
      contract shared by route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    status: str
    removed_existing: bool
    file_count: int
    bytes_copied: int
    message: str
    sample_dir: Optional[str] = None
    remote_download_started: bool = False



class ImportSampleDataRequest(BaseModel):
    """Request schema used by API routes and generated clients for import sample data request.

    Used by:
    - backend API routes, backend request/response models because they need a stable JSON
      contract shared by route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    collection_ids: List[str] = Field(default_factory=list)



class SampleDataFileEntry(BaseModel):
    """API schema used by routes and generated clients for sample data file entry.

    Used by:
    - backend API routes, backend request/response models because they need a stable JSON
      contract shared by route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    path: str
    size: int
    sha256: str



class SampleDataCollection(BaseModel):
    """API schema used by routes and generated clients for sample data collection.

    Used by:
    - backend API routes, backend request/response models because they need a stable JSON
      contract shared by route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    id: str
    name: str
    description: str
    language: str
    bundled: bool
    total_size_bytes: int
    recommended_for: List[str]
    files: List[SampleDataFileEntry]
    status: Literal["bundled", "downloaded", "partial", "not_downloaded"]



class SampleDataCatalogueResponse(BaseModel):
    """Response schema returned by API routes and consumed by generated clients for sample data catalogue response.

    Used by:
    - backend API routes, backend request/response models because they need a stable JSON
      contract shared by route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

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
    """A single demo-snapshot bundle in the catalogue.

    Used by:
    - backend API routes, backend request/response models because they need a stable JSON
      contract shared by route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

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
    status: Literal["downloaded", "not_downloaded", "conflict"]
    """Computed per-user: ``downloaded`` | ``not_downloaded`` | ``conflict``.
    ``conflict`` means a file with the same name exists locally but its
    SHA differs (older bundle, or the user's own save). The importer
    skips ``conflict`` rows unless the user opts in via ``replace_ids``."""



class DemoSnapshotsCatalogueResponse(BaseModel):
    """Response schema returned by API routes and consumed by generated clients for demo snapshots catalogue response.

    Used by:
    - backend API routes, backend request/response models because they need a stable JSON
      contract shared by route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    schema_version: int
    snapshots: List[DemoSnapshotEntry]



class ImportDemoSnapshotsRequest(BaseModel):
    """Request schema used by API routes and generated clients for import demo snapshots request.

    Used by:
    - backend API routes, backend request/response models because they need a stable JSON
      contract shared by route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    snapshot_ids: List[str] = Field(default_factory=list)
    """Snapshot ids to import. Empty list = no-op."""
    replace_ids: List[str] = Field(default_factory=list)
    """Subset of ``snapshot_ids`` for which the importer should replace an
    existing ``conflict`` local copy. Entries outside this list with a
    conflict are skipped."""



class DemoSnapshotImportResult(BaseModel):
    """API schema used by routes and generated clients for demo snapshot import result.

    Used by:
    - backend API routes, backend request/response models because they need a stable JSON
      contract shared by route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    id: str
    filename: str
    status: Literal[
        "imported",
        "replaced",
        "skipped_existing",
        "skipped_conflict",
        "failed",
    ]
    message: Optional[str] = None



class ImportDemoSnapshotsResponse(BaseModel):
    """Response schema returned by API routes and consumed by generated clients for import demo snapshots response.

    Used by:
    - backend API routes, backend request/response models because they need a stable JSON
      contract shared by route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    results: List[DemoSnapshotImportResult]
    snapshot_dir: str



class DataFileInfo(BaseModel):
    """Metadata schema used by API responses to describe data file info.

    Used by:
    - backend request/response models because they need a stable JSON contract shared by
      route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    filename: str
    size: int
    created_at: str
    file_type: str



class LDaCAImportRequest(BaseModel):
    """Request schema used by API routes and generated clients for l da c a import request.

    Used by:
    - backend API routes, backend request/response models because they need a stable JSON
      contract shared by route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    url: str
    filename: Optional[str] = None



class OniSearchRequest(BaseModel):
    """Request schema used by API routes and generated clients for oni search request.

    Used by:
    - backend API routes, backend request/response models because they need a stable JSON
      contract shared by route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    method: Literal[
        "keyword",
        "identifier",
        "id",
        "string",
        "collection",
        "file_format",
        "all",
    ] = Field(default="keyword")
    query: str = Field(default="")
    limit: int = Field(default=25, ge=1, le=100)
    offset: int = Field(default=0, ge=0)



class OniSearchResult(BaseModel):
    """API schema used by routes and generated clients for oni search result.

    Used by:
    - backend API routes, backend request/response models because they need a stable JSON
      contract shared by route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    id: str
    crate_id: str | None = None
    title: str
    description: str | None = None
    types: list[str]
    license: str | None = None
    importable: bool
    access: dict[str, Any] | None = None
    collections: list[str]
    file_formats: list[str]
    stats: dict[str, Any]



class OniSearchResponse(BaseModel):
    """Response schema returned by API routes and consumed by generated clients for oni search response.

    Used by:
    - backend API routes, backend request/response models because they need a stable JSON
      contract shared by route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    state: Literal["successful"]
    data: list[OniSearchResult]
    message: str



class FileTreeNodeResponse(BaseModel):
    """Response schema returned by API routes and consumed by generated clients for file tree node response.

    Used by:
    - backend API routes, backend request/response models because they need a stable JSON
      contract shared by route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    name: str
    path: str
    type: Literal["file", "directory"]
    size: Optional[int] = None
    children: Optional[List["FileTreeNodeResponse"]] = None

    @model_validator(mode="after")
    def normalize_directory_children(self) -> "FileTreeNodeResponse":
        """Support API schema contracts with a normalize directory children helper.

        Called by:
        - `FileTreeNodeResponse` instances owned by backend services, routes, and tests because
          they need a backend boundary that validates inputs before delegating to workspace or
          worker state.

        Flow: validate incoming API fields, apply defaults or validators, and serialize route
            responses in the shape expected by frontend clients and tests.
        """

        if self.type == "directory" and self.children is None:
            self.children = []
        return self


FileTreeNodeResponse.model_rebuild()



class CreateFolderRequest(BaseModel):
    """Request schema used by API routes and generated clients for create folder request.

    Used by:
    - backend API routes, backend request/response models because they need a stable JSON
      contract shared by route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    name: str
    parent_path: str = ""



class CreateFolderResponse(BaseModel):
    """Response schema returned by API routes and consumed by generated clients for create folder response.

    Used by:
    - backend API routes, backend request/response models because they need a stable JSON
      contract shared by route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    message: str
    path: str



class MoveFileRequest(BaseModel):
    """Request schema used by API routes and generated clients for move file request.

    Used by:
    - backend API routes, backend request/response models because they need a stable JSON
      contract shared by route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    source_path: str
    target_directory_path: str



class MessageResponse(BaseModel):
    """Response schema returned by API routes and consumed by generated clients for message response.

    Used by:
    - backend API routes, backend request/response models because they need a stable JSON
      contract shared by route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    message: str



class RenameColumnRequest(BaseModel):
    """Request schema used by API routes and generated clients for rename column request.

    Used by:
    - backend API routes, backend request/response models because they need a stable JSON
      contract shared by route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    new_name: str



class FilesTaskMetadataResponse(BaseModel):
    """Response schema returned by API routes and consumed by generated clients for files task metadata response.

    Used by:
    - backend request/response models because they need a stable JSON contract shared by
      route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    task_id: str



class FilesImportTaskStartResponse(BaseModel):
    """Response schema returned by API routes and consumed by generated clients for files import task start response.

    Used by:
    - backend API routes, backend request/response models because they need a stable JSON
      contract shared by route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    state: Literal["running"]
    message: str
    metadata: FilesTaskMetadataResponse



class TaskListResponse(BaseModel):
    """Response schema returned by API routes and consumed by generated clients for task list response.

    Used by:
    - backend API routes, backend request/response models because they need a stable JSON
      contract shared by route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    state: Literal["successful"]
    data: list[TaskEntry]
    message: str



class TaskClearActionDataResponse(BaseModel):
    """Response schema returned by API routes and consumed by generated clients for task clear action data response.

    Used by:
    - backend request/response models because they need a stable JSON contract shared by
      route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    cleared_worker: bool
    cleared_analysis: bool
    cleared_worker_ids: list[str]
    cleared_analysis_ids: list[str]
    cleared_task_ids: list[str]



class TaskClearActionResponse(BaseModel):
    """Response schema returned by API routes and consumed by generated clients for task clear action response.

    Used by:
    - backend API routes, backend request/response models because they need a stable JSON
      contract shared by route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    state: Literal["successful"]
    data: TaskClearActionDataResponse
    message: str



class TaskCancelActionDataResponse(BaseModel):
    """Response schema returned by API routes and consumed by generated clients for task cancel action data response.

    Used by:
    - backend request/response models because they need a stable JSON contract shared by
      route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    stopped: bool



class TaskCancelActionResponse(BaseModel):
    """Response schema returned by API routes and consumed by generated clients for task cancel action response.

    Used by:
    - backend API routes, backend request/response models because they need a stable JSON
      contract shared by route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    state: Literal["successful"]
    data: TaskCancelActionDataResponse
    message: str



class FilesTasksListResponse(BaseModel):
    """Response schema returned by API routes and consumed by generated clients for files tasks list response.

    Used by:
    - backend API routes, backend request/response models because they need a stable JSON
      contract shared by route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    state: str
    data: List[Dict[str, Any]]
    message: str



class FilesTaskActionDataResponse(BaseModel):
    """Response schema returned by API routes and consumed by generated clients for files task action data response.

    Used by:
    - backend request/response models because they need a stable JSON contract shared by
      route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    cancelled: Optional[bool] = None
    cancelled_count: Optional[int] = None
    cleared_count: Optional[int] = None



class FilesTaskActionResponse(BaseModel):
    """Response schema returned by API routes and consumed by generated clients for files task action response.

    Used by:
    - backend API routes, backend request/response models because they need a stable JSON
      contract shared by route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    state: str
    data: FilesTaskActionDataResponse
    message: str



class FileInfoResponse(BaseModel):
    """Response schema returned by API routes and consumed by generated clients for file info response.

    Used by:
    - backend API routes, backend request/response models because they need a stable JSON
      contract shared by route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    filename: str
    size_Byte: int
    created_at: float
    modified_at: float
    file_type: str


SnapshotToolKey = Literal[
    "topic_modeling",
    "token_frequencies",
    "sequential_analysis",
    "concordance",
    "quotation",
]



class SnapshotCapabilities(BaseModel):
    """API schema used by routes and generated clients for snapshot capabilities.

    Used by:
    - backend request/response models because they need a stable JSON contract shared by
      route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    canPaginate: bool
    canSortAndFilterResult: bool
    canExport: bool
    canFilterSourceRows: bool
    canCrossJump: bool



class SnapshotPayloadEntryResult(BaseModel):
    """API schema used by routes and generated clients for snapshot payload entry result.

    Used by:
    - backend request/response models because they need a stable JSON contract shared by
      route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    kind: Literal["result"]
    path: str



class SnapshotPayloadEntryDispersionBins(BaseModel):
    """API schema used by routes and generated clients for snapshot payload entry dispersion bins.

    Used by:
    - backend request/response models because they need a stable JSON contract shared by
      route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    kind: Literal["dispersion-bins"]
    path: str



class SnapshotPayloadEntrySourceProjection(BaseModel):
    """API schema used by routes and generated clients for snapshot payload entry source projection.

    Used by:
    - backend request/response models because they need a stable JSON contract shared by
      route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    kind: Literal["source-projection"]
    path: str
    columns: list[str]



class SnapshotPayloadEntrySettings(BaseModel):
    """API schema used by routes and generated clients for snapshot payload entry settings.

    Used by:
    - backend request/response models because they need a stable JSON contract shared by
      route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    kind: Literal["settings"]
    path: str


SnapshotPayloadEntry = Union[
    SnapshotPayloadEntryResult,
    SnapshotPayloadEntryDispersionBins,
    SnapshotPayloadEntrySourceProjection,
    SnapshotPayloadEntrySettings,
]



class ConcordanceSnapshotPreview(BaseModel):
    """API schema used by routes and generated clients for concordance snapshot preview.

    Used by:
    - backend request/response models because they need a stable JSON contract shared by
      route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    tool: Literal["concordance"]
    searchTerm: str
    totalHits: int
    materialised: bool
    displayColumns: list[str]



class QuotationSnapshotPreview(BaseModel):
    """API schema used by routes and generated clients for quotation snapshot preview.

    Used by:
    - backend request/response models because they need a stable JSON contract shared by
      route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    tool: Literal["quotation"]
    openPattern: str
    closePattern: str
    totalHits: int
    displayColumns: list[str]



class TokenFrequenciesSnapshotPreview(BaseModel):
    """API schema used by routes and generated clients for token frequencies snapshot preview.

    Used by:
    - backend request/response models because they need a stable JSON contract shared by
      route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    tool: Literal["token_frequencies"]
    vocabSize: int
    topToken: str
    topTokenCount: int
    tokeniserId: str



class SequentialAnalysisSnapshotPreview(BaseModel):
    """API schema used by routes and generated clients for sequential analysis snapshot preview.

    Used by:
    - backend request/response models because they need a stable JSON contract shared by
      route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    tool: Literal["sequential_analysis"]
    seriesCount: int
    bucketCount: int
    chartType: str



class TopicModelingSnapshotPreview(BaseModel):
    """API schema used by routes and generated clients for topic modeling snapshot preview.

    Used by:
    - backend request/response models because they need a stable JSON contract shared by
      route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    tool: Literal["topic_modeling"]
    numTopics: int
    vocabSize: int
    embedder: str
    wordsPerTopic: int


SnapshotPreview = Union[
    ConcordanceSnapshotPreview,
    QuotationSnapshotPreview,
    TokenFrequenciesSnapshotPreview,
    SequentialAnalysisSnapshotPreview,
    TopicModelingSnapshotPreview,
]



class SnapshotSource(BaseModel):
    """API schema used by routes and generated clients for snapshot source.

    Used by:
    - backend request/response models because they need a stable JSON contract shared by
      route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    workspace_id: str
    workspace_name: str
    node_ids: list[str]
    node_labels: list[str]
    per_block_rows: list[int] | None = None
    total_source_rows: int



class SnapshotManifest(BaseModel):
    """API schema used by routes and generated clients for snapshot manifest.

    Used by:
    - backend request/response models because they need a stable JSON contract shared by
      route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    schema_version: Literal[1]
    mode: Literal["demo", "share"]
    tool: SnapshotToolKey
    tool_version: str
    captured_at: str
    title: str
    source: SnapshotSource
    capabilities: SnapshotCapabilities
    preview: SnapshotPreview
    payloads: list[SnapshotPayloadEntry]
    node_colors: dict[str, str]



class SnapshotListItem(BaseModel):
    """API schema used by routes and generated clients for snapshot list item.

    Used by:
    - backend request/response models because they need a stable JSON contract shared by
      route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    filename: str
    manifest: SnapshotManifest
    size_bytes: int



class SnapshotListResponse(BaseModel):
    """Response schema returned by API routes and consumed by generated clients for snapshot list response.

    Used by:
    - backend API routes, backend request/response models because they need a stable JSON
      contract shared by route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    items: list[SnapshotListItem]



class SnapshotUploadResponse(BaseModel):
    """Response schema returned by API routes and consumed by generated clients for snapshot upload response.

    Used by:
    - backend API routes, backend request/response models because they need a stable JSON
      contract shared by route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    filename: str
    manifest: SnapshotManifest



class SnapshotDeleteResponse(BaseModel):
    """Response schema returned by API routes and consumed by generated clients for snapshot delete response.

    Used by:
    - backend API routes, backend request/response models because they need a stable JSON
      contract shared by route handlers, generated clients, and tests.

    Flow: validate incoming API fields, apply defaults or validators, and serialize route
        responses in the shape expected by frontend clients and tests.
    """

    deleted: list[str]


# =============================================================================
# WORKSPACE MODELS
# =============================================================================




SnapshotPayloadEntry = Union[
    SnapshotPayloadEntryResult,
    SnapshotPayloadEntryDispersionBins,
    SnapshotPayloadEntrySourceProjection,
    SnapshotPayloadEntrySettings,
]
SnapshotPreview = Union[
    ConcordanceSnapshotPreview,
    QuotationSnapshotPreview,
    TokenFrequenciesSnapshotPreview,
    SequentialAnalysisSnapshotPreview,
    TopicModelingSnapshotPreview,
]
SnapshotToolKey = Literal[
    "topic_modeling",
    "token_frequencies",
    "sequential_analysis",
    "concordance",
    "quotation",
]
