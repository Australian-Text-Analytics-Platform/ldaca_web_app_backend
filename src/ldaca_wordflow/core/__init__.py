"""Core backend utilities and orchestration helpers.

Used by:
- API routers, worker task manager, and workspace services because they need a backend
  boundary that validates inputs before delegating to workspace or worker state.
Why:
- Groups reusable non-route primitives for auth, workspace, and analysis runtime.

Flow: normalize inputs, delegate to the owning backend state or service boundary, and
    return serialized values or existing domain errors to callers.
"""
