"""Shared helper for estimating concordance/quotation page sizes.

Used by:
- `concordance_core.build_concordance_response` because they need this unit's "Shared helper for estimating concordance/quotation page sizes" behavior.
- `quotation_core.compute_on_demand_page` because they need this unit's "Shared helper for estimating concordance/quotation page sizes" behavior.

Why:
- Sparse occurrence distributions produce near-empty pages when a fixed
  document-slice size is used. Walking a candidate list and picking the
  smallest size whose first-page yields enough occurrences keeps pages dense
  without requiring the frontend to know extraction-specific defaults.

Flow:
- Analysis response builders provide a cheap probe function for candidate document counts.
- The helper walks candidates until one yields the target occurrence density.
- Callers fall back to the largest candidate when every probe remains sparse.
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

    Steps:
    - Normalize caller input into the representation this module expects.
    - Delegate stateful, expensive, or validating work to the owning manager/helper when needed.
    - Return the compact value the caller uses for artifacts, validation, or response shaping.

    Used by:
    - backend API routes because they need this unit's "Return the smallest candidate whose probe yields at least `target` hits" behavior.
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
