"""Shared helper for estimating concordance/quotation page sizes.

Used by:
- `concordance_core.build_concordance_response`
- `quotation_core.compute_on_demand_page`

Why:
- Sparse occurrence distributions produce near-empty pages when a fixed
  document-slice size is used. Walking a candidate list and picking the
  smallest size whose first-page yields enough occurrences keeps pages dense
  without requiring the frontend to know extraction-specific defaults.
"""

from __future__ import annotations

from typing import Callable, Sequence

# Capped at 100: probing larger slices made sense back when the regex
# engine was the only path, but in practice a search that yields <10 hits
# in the first 100 docs is already sparse enough to fall through to the
# last-candidate fallback. Going up to 800 was also wasteful when the
# tokens engine made probing precise (no \b-on-CJK false negatives), and
# it produced a misleading "after processing 800 documents" label on
# corpora smaller than 800 rows.
DEFAULT_PAGE_SIZE_CANDIDATES: tuple[int, ...] = (10, 20, 50, 100)
TARGET_OCCURRENCES: int = 10


def estimate_page_size(
    probe_fn: Callable[[int], int],
    *,
    candidates: Sequence[int] = DEFAULT_PAGE_SIZE_CANDIDATES,
    target: int = TARGET_OCCURRENCES,
) -> int:
    """Return the smallest candidate whose probe yields at least `target` hits.

    If every candidate is below `target`, returns the largest candidate.
    """
    if not candidates:
        raise ValueError("candidates must be non-empty")
    last = candidates[0]
    for size in candidates:
        last = size
        try:
            count = int(probe_fn(size) or 0)
        except Exception:
            count = 0
        if count >= target:
            return size
    return last
