"""Load backend-owned configuration for RO-Crate tabular conversion.

Used by:
- Backend API routes, worker tasks, workspace services, and backend tests because they
  need a backend boundary that validates inputs before delegating to workspace or worker
  state.

Flow: normalize inputs, delegate to the owning backend state or service boundary, and
    return serialized values or existing domain errors to callers.
"""

from __future__ import annotations

import json
import re
from importlib import resources
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

CONFIG_PACKAGE = "ldaca_wordflow.resources.ldaca_tabular_configs"
GENERAL_CONFIG = "general/general-config.json"
CORPUS_CONFIG_DIR = "corpora"
WINDOWS_RESERVED_FILENAMES = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    *(f"COM{number}" for number in range(1, 10)),
    *(f"LPT{number}" for number in range(1, 10)),
}
WINDOWS_UNSAFE_FILENAME_CHARS = re.compile('[<>:"/\\\\|?*\\x00-\\x1f]')


def _extract_crate_id(value: str) -> str:
    """Extract crate id values used by LDaCA tabular configuration loading.

    Called by:
    - Local helpers, route handlers, or service methods in this module because they need a
      backend boundary that validates inputs before delegating to workspace or worker state.

    Flow: normalize inputs, delegate to the owning backend state or service boundary, and
        return serialized values or existing domain errors to callers.
    """

    candidate = value.strip()
    parsed = urlparse(candidate)
    if parsed.scheme in {"http", "https"}:
        query_values = parse_qs(parsed.query)
        for key in ("_crateId", "id"):
            values = query_values.get(key)
            if values and values[0].strip():
                return values[0].strip()
    return unquote(candidate)


def _safe_corpus_config_filename(crate_id: str) -> str | None:
    """Create safe corpus config filename values for LDaCA tabular configuration loading.

    Called by:
    - Local helpers, route handlers, or service methods in this module because they need a
      backend boundary that validates inputs before delegating to workspace or worker state.

    Flow: normalize inputs, delegate to the owning backend state or service boundary, and
        return serialized values or existing domain errors to callers.
    """

    identifier = re.sub(
        r"^arcp://", "", _extract_crate_id(crate_id), flags=re.IGNORECASE
    )
    filename_stem = WINDOWS_UNSAFE_FILENAME_CHARS.sub("_", identifier).strip(" ._")
    if not filename_stem:
        return None
    if filename_stem.split(".", maxsplit=1)[0].upper() in WINDOWS_RESERVED_FILENAMES:
        filename_stem = f"_{filename_stem}"
    return f"{filename_stem}.json"


def _load_json_resource(relative_path: str) -> dict[str, Any]:
    """Load json resource data for LDaCA tabular configuration loading.

    Called by:
    - Local helpers, route handlers, or service methods in this module because they need a
      backend boundary that validates inputs before delegating to workspace or worker state.

    Flow: normalize inputs, delegate to the owning backend state or service boundary, and
        return serialized values or existing domain errors to callers.
    """

    resource = resources.files(CONFIG_PACKAGE).joinpath(relative_path)
    return json.loads(resource.read_text(encoding="utf-8"))


def load_tabular_config(crate_id: str | None = None) -> dict[str, Any]:
    """Load the corpus-specific tabulator config when available.

    Used by:
    - backend tests, core workspace and worker services because tests need the same
      observable contract that production routes and workers rely on.

    Flow: normalize inputs, delegate to the owning backend state or service boundary, and
        return serialized values or existing domain errors to callers.
    """
    if crate_id:
        filename = _safe_corpus_config_filename(crate_id)
        if filename:
            relative_path = f"{CORPUS_CONFIG_DIR}/{filename}"
            if resources.files(CONFIG_PACKAGE).joinpath(relative_path).is_file():
                return _load_json_resource(relative_path)
    return _load_json_resource(GENERAL_CONFIG)
