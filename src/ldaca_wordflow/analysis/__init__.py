"""Analysis domain package.

Used by:
- workspace analysis routers and task manager serialization code because they need a
  backend boundary that validates inputs before delegating to workspace or worker state.
Why:
- Groups request/result contracts and implementation-specific schema modules.

Flow: normalize inputs, delegate to the owning backend state or service boundary, and
    return serialized values or existing domain errors to callers.
"""
