import logging
import os
from pathlib import Path

from fastapi import APIRouter
from pydantic import BaseModel

from ..settings import reload_settings, settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/config", tags=["configuration"])


class ConfigResponse(BaseModel):
    data_root: str
    multi_user_mode: bool
    google_client_id: str = ""
    # TEMPORARY (Phase 2/2.5 local-testing aid) — surfaces the
    # LDACA_LAZY_TOKENISE env flag so the frontend can render a dev
    # badge during the soak window. REMOVE BEFORE PUBLISH (Phase 3+):
    # delete this field, the wiring in `get_config`, the matching
    # field in `frontend/src/api/config.ts`, and the
    # <LazyTokeniseDevBadge /> component + its mount in App.tsx.
    lazy_tokenise_enabled: bool = False


class ConfigUpdate(BaseModel):
    data_root: str


@router.get("/", response_model=ConfigResponse)
async def get_config():
    """Return currently effective runtime configuration values.

    Used by:
    - frontend settings/config panels and OAuth client bootstrap

    Why:
    - Exposes backend mode, storage root, and Google OAuth client ID so the
      frontend can initialize the login provider at runtime.
    """
    # TEMPORARY (Phase 2/2.5) — import inside the handler to avoid the
    # top-of-module dep on core.derived_columns just for a dev badge.
    # Remove together with the field. See ConfigResponse note above.
    from ..core.derived_columns import _lazy_tokenise_enabled

    return ConfigResponse(
        data_root=str(settings.get_data_root()),
        multi_user_mode=settings.multi_user,
        google_client_id=settings.google_client_id or "",
        lazy_tokenise_enabled=_lazy_tokenise_enabled(),
    )


@router.post("/", response_model=ConfigResponse)
async def update_config(config: ConfigUpdate):
    """Update in-memory runtime configuration values.

    Used by:
    - frontend config edit flow

    Why:
    - Allows runtime overrides without process restart.

    Refactor note:
    - Current update is in-memory only; persist-or-reload strategy may be needed
        for multi-process or restart-stable configuration behavior.
    """
    new_path = Path(config.data_root)

    logger.info("Updating data_root to %s", new_path)
    # Write to env var and reload so the singleton stays in sync
    os.environ["DATA_ROOT"] = str(new_path)
    updated = reload_settings()

    # TEMPORARY (Phase 2/2.5) — see ConfigResponse note above.
    from ..core.derived_columns import _lazy_tokenise_enabled

    return ConfigResponse(
        data_root=str(updated.get_data_root()),
        multi_user_mode=updated.multi_user,
        google_client_id=updated.google_client_id or "",
        lazy_tokenise_enabled=_lazy_tokenise_enabled(),
    )
