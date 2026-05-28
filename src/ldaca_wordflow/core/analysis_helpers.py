"""Shared helpers for analysis request sanitization.

Used by:
- Backend API routes, worker tasks, workspace services, and backend tests because they
  need a backend boundary that validates inputs before delegating to workspace or worker
  state.

Flow: normalize inputs, delegate to the owning backend state or service boundary, and
    return serialized values or existing domain errors to callers.
"""

from __future__ import annotations

from typing import Iterable


def sanitize_stop_words(stop_words: object) -> list[str]:
    """Normalize stop-words input into a clean, de-duplicated list.

    Keeps insertion order, strips whitespace, and discards empty values.

    Used by:
    - backend API routes, core workspace and worker services because they need a backend
      boundary that validates inputs before delegating to workspace or worker state.

    Flow: normalize inputs, delegate to the owning backend state or service boundary, and
        return serialized values or existing domain errors to callers.
    """
    if stop_words is None:
        return []

    if isinstance(stop_words, str):
        candidates: Iterable[object] = [stop_words]
    elif isinstance(stop_words, (list, tuple, set)):
        candidates = stop_words
    else:
        return []

    normalized: list[str] = []
    seen: set[str] = set()
    for value in candidates:
        text = str(value).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        normalized.append(text)

    return normalized
