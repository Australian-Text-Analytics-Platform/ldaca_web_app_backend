"""Workspace analysis router package.

Contains analysis endpoint modules and shared helpers used by workspace routes.

Used by:
- `api.workspaces` router aggregation because they need this unit's "Workspace analysis router package" behavior.

Why:
- Centralizes analysis-related route modules under one package boundary.
"""

# Server-side result-page cap for snapshot captures requesting ``page_size="all"``.
# Shared across concordance.py and quotation.py.
SNAPSHOT_ALL_PAGE_SIZE_CAP = 500_000
