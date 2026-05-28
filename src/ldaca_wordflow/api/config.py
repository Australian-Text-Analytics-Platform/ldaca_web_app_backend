"""Runtime configuration routes used by the frontend settings flow.

Used by:
- FastAPI router registration, frontend API clients, and backend tests because they need this unit's "Runtime configuration routes used by the frontend settings flow" behavior.

Flow:
- FastAPI mounts these endpoints under the config API prefix.
- Route handlers read or update runtime settings through the shared settings module.
- Responses return generated config models so clients can refresh their runtime assumptions.
"""

import logging
import os
from pathlib import Path

from fastapi import APIRouter
from pydantic import BaseModel

from ..settings import reload_settings, settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/config", tags=["configuration"])


class ConfigResponse(BaseModel):
    """Response schema used by config routes and generated API clients.

    Used by:
    - backend API routes because they need this unit's "Response schema used by config routes and generated API clients" behavior.
    """

    data_root: str
    multi_user_mode: bool
    google_client_id: str = ""


class ConfigUpdate(BaseModel):
    """Request schema used when the frontend updates runtime data-root settings.

    Used by:
    - backend API routes because they need this unit's "Request schema used when the frontend updates runtime data-root settings" behavior.
    """

    data_root: str


@router.get("/", response_model=ConfigResponse)
async def get_config():
    """Return currently effective runtime configuration values.

    Flow:
    - Resolve authentication and request parameters from FastAPI dependencies.
    - Delegate validation, manager calls, artifacts, or state changes to the owning helper.
    - Shape the response payload or raise the HTTP error the client should see.

    Used by:
    - frontend settings/config panels and OAuth client bootstrap because they need this unit's "Return currently effective runtime configuration values" behavior.

    Why:
    - Exposes backend mode, storage root, and Google OAuth client ID so the
      frontend can initialize the login provider at runtime.
    """
    return ConfigResponse(
        data_root=str(settings.get_data_root()),
        multi_user_mode=settings.multi_user,
        google_client_id=settings.google_client_id or "",
    )


@router.post("/", response_model=ConfigResponse)
async def update_config(config: ConfigUpdate):
    """Update in-memory runtime configuration values.

    Flow:
    - Resolve authentication and request parameters from FastAPI dependencies.
    - Delegate validation, manager calls, artifacts, or state changes to the owning helper.
    - Shape the response payload or raise the HTTP error the client should see.

    Used by:
    - frontend config edit flow because they need this unit's "Update in-memory runtime configuration values" behavior.

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

    return ConfigResponse(
        data_root=str(updated.get_data_root()),
        multi_user_mode=updated.multi_user,
        google_client_id=updated.google_client_id or "",
    )
