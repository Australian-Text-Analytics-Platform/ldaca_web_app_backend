"""Analysis data models.

Used by:
- Analysis routes, worker result persistence, and backend tests because they need a
  backend boundary that validates inputs before delegating to workspace or worker state.

Flow: validate task or result payload fields, update terminal state, and serialize
    nested values into JSON-safe structures.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field

from .results import BaseAnalysisResult


class AnalysisStatus(str, Enum):
    """Lifecycle states for analysis tasks.

    Used by:
    - `AnalysisTask` because callers need the shared analysis task/result serialization rule
      in one place instead of duplicating it.
    - analysis/task APIs and worker sync helpers because they need a backend boundary that
      validates inputs before delegating to workspace or worker state.
    Why:
    - Keeps task status values consistent across storage and API responses.

    Flow: validate task or result payload fields, update terminal state, and serialize
        nested values into JSON-safe structures.
    """

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class BaseAnalysisRequest(BaseModel):
    """Base request model for all analyses.

    Used by:
    - analysis task helpers, backend request/response models, backend tests because they
      need a stable JSON contract shared by route handlers, generated clients, and tests.

    Flow: validate task or result payload fields, update terminal state, and serialize
        nested values into JSON-safe structures.
    """

    model_config = ConfigDict(extra="allow")
    # Additional common fields can go here


TRequest = TypeVar("TRequest", bound=BaseAnalysisRequest)
TResult = TypeVar("TResult", bound=BaseAnalysisResult)


class AnalysisTask(BaseModel, Generic[TRequest, TResult]):
    """Container for a single analysis task.

    Used by:
    - analysis task helpers, backend API routes, backend tests because they need a backend
      boundary that validates inputs before delegating to workspace or worker state.

    Flow: validate task or result payload fields, update terminal state, and serialize
        nested values into JSON-safe structures.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    task_id: str
    user_id: str
    workspace_id: str
    status: AnalysisStatus = AnalysisStatus.PENDING
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    request: TRequest
    result: TResult | None = None
    error: str | None = None
    parent_task_id: str | None = None
    child_task_ids: list[str] = Field(default_factory=list)

    def complete(self, result: TResult) -> None:
        """Mark task completed and attach result payload.

        Used by:
        - `analysis.manager.TaskManager.update_task` because analysis flows need per-user task
          state to survive across route calls and worker result persistence.
        - worker sync paths that finalize memory tasks because background jobs need one
          lifecycle owner for submission, progress, cancellation, and artifact cleanup.
        Why:
        - Provides one canonical status transition to `COMPLETED`.

        Flow: validate task or result payload fields, update terminal state, and serialize
            nested values into JSON-safe structures.
        """
        self.result = result
        self.status = AnalysisStatus.COMPLETED
        self.updated_at = datetime.now()

    def fail(self, error: str) -> None:
        """Mark task failed with error details.

        Used by:
        - worker sync and error propagation paths because background jobs need one lifecycle
          owner for submission, progress, cancellation, and artifact cleanup.
        Why:
        - Standardizes failure transition metadata for UI/task APIs.

        Flow: validate task or result payload fields, update terminal state, and serialize
            nested values into JSON-safe structures.
        """
        self.error = error
        self.status = AnalysisStatus.FAILED
        self.updated_at = datetime.now()
