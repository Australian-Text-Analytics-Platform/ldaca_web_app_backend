"""Analysis result base classes.

Used by:
- Analysis routes, worker result persistence, and backend tests because they need a
  backend boundary that validates inputs before delegating to workspace or worker state.

Flow: validate task or result payload fields, update terminal state, and serialize
    nested values into JSON-safe structures.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class BaseAnalysisResult(ABC):
    """Abstract base class for analysis results.

    Used by:
    - analysis task helpers because analysis flows need per-user task state to survive
      across route calls and worker result persistence.

    Flow: validate task or result payload fields, update terminal state, and serialize
        nested values into JSON-safe structures.
    """

    @abstractmethod
    def to_json(self, **kwargs: Any) -> dict[str, Any]:
        """Convert result to API-ready JSON payload.

        Used by:
        - analysis routes when returning stored task results because they need a backend
          boundary that validates inputs before delegating to workspace or worker state.
        Why:
        - Keeps response serialization polymorphic across result types.

        Flow: validate task or result payload fields, update terminal state, and serialize
            nested values into JSON-safe structures.
        """
        pass


class GenericAnalysisResult(BaseAnalysisResult):
    """Simple result wrapper for generic dictionary results.

    Used by:
    - analysis task helpers, backend API routes, backend tests, core workspace and worker
      services because they need a backend boundary that validates inputs before delegating
      to workspace or worker state.

    Flow: validate task or result payload fields, update terminal state, and serialize
        nested values into JSON-safe structures.
    """

    def __init__(self, data: dict[str, Any]):
        """Initialize GenericAnalysisResult state used by analysis result serialization.

        Called by:
        - `GenericAnalysisResult` construction in backend services and tests because tests need
          the same observable contract that production routes and workers rely on.

        Flow: validate task or result payload fields, update terminal state, and serialize
            nested values into JSON-safe structures.
        """

        self.data = data

    def to_json(self, **kwargs: Any) -> dict[str, Any]:
        """Return wrapped dictionary payload without transformation.

        Used by:
        - worker result persistence for generic analysis outputs because background jobs need
          one lifecycle owner for submission, progress, cancellation, and artifact cleanup.
        Why:
        - Provides a lightweight default result adapter.

        Flow: validate task or result payload fields, update terminal state, and serialize
            nested values into JSON-safe structures.
        """
        return self.data
