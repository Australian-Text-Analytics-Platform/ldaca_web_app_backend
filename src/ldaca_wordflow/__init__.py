"""Public package exports for backend app entrypoints.

Used by:
- desktop/runtime launchers and external embedding contexts because callers need the
  shared shared backend behavior rule in one place instead of duplicating it.
Why:
- Provides stable import surface for app object and startup helpers.

Flow: normalize inputs, delegate to the owning backend state or service boundary, and
    return serialized values or existing domain errors to callers.
"""

from .core.workspace import workspace_manager
from .main import app, start_server

__all__ = ["app", "workspace_manager", "start_server"]
