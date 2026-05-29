"""Admin endpoints

Used by:
- FastAPI router registration, frontend API clients, and backend tests because they need this unit's "Admin endpoints" behavior.

Flow:
- FastAPI mounts these endpoints under the admin API prefix.
- Route handlers authorize the current user before reading database state.
- Responses expose operational user/session state or an admin-only HTTP error.
"""

import logging

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select

from ..core.auth import get_current_user
from ..core.auth_service import _utc_now_naive, cleanup_expired_sessions
from ..db import async_session_maker
from ..models.db import User, UserSession
from ..settings import settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin", tags=["admin"])


def _require_admin(current_user: dict) -> None:
    """Authorize admin routes.

    - Single-user mode: always allowed.
    - Multi-user mode: requires current user email to be in `ADMIN_EMAILS`.

    Steps:
    - Normalize caller input into the representation this module expects.
    - Delegate stateful, expensive, or validating work to the owning manager/helper when needed.
    - Return the compact value the caller uses for artifacts, validation, or response shaping.

    Called by:
    - Local helpers, route handlers, or service methods in this module because they need this unit's "Authorize admin routes" behavior.
    """
    if not settings.multi_user:
        return

    current_email = str(current_user.get("email") or "").strip().lower()
    admin_allowlist = settings.get_admin_emails()

    if current_email and current_email in admin_allowlist:
        return

    raise HTTPException(status_code=403, detail="Admin access required")


@router.get("/users")
async def list_users(current_user: dict = Depends(get_current_user)):
    """List users with active-session counts.

    Flow:
    - Resolve authentication and request parameters from FastAPI dependencies.
    - Delegate validation, manager calls, artifacts, or state changes to the owning helper.
    - Shape the response payload or raise the HTTP error the client should see.

    Used by:
    - admin dashboard/user management views because they need this unit's "List users with active-session counts" behavior.

    Why:
    - Provides operational visibility into user and session activity.

    Refactor note:
    - Add `require_admin` dependency before wider deployment to avoid role drift.
    """
    _require_admin(current_user)
    logger.info("Admin user list requested by %s", current_user["email"])

    async with async_session_maker() as session:
        # Get all users
        result = await session.execute(select(User))
        users = result.scalars().all()

        user_list = []
        for user in users:
            # Count active sessions for each user
            session_result = await session.execute(
                select(UserSession)
                .where(UserSession.user_id == user.id)
                .where(UserSession.expires_at > _utc_now_naive())
            )
            active_sessions = len(session_result.scalars().all())

            user_list.append(
                {
                    "id": str(user.id),
                    "email": user.email,
                    "name": user.name,
                    "created_at": user.created_at,
                    "last_login": user.last_login,
                    "active_sessions": active_sessions,
                }
            )

        return {
            "users": user_list,
            "total": len(user_list),
            "requested_by": current_user["email"],
        }


@router.get("/cleanup")
async def admin_cleanup(current_user: dict = Depends(get_current_user)):
    """Trigger cleanup of expired session rows.

    Flow:
    - Resolve authentication and request parameters from FastAPI dependencies.
    - Delegate validation, manager calls, artifacts, or state changes to the owning helper.
    - Shape the response payload or raise the HTTP error the client should see.

    Used by:
    - admin maintenance actions because they need this unit's "Trigger cleanup of expired session rows" behavior.

    Why:
    - Allows manual session-store maintenance in addition to automatic cleanup.

    Refactor note:
    - Add `require_admin` dependency before wider deployment.
    """
    _require_admin(current_user)
    logger.info("Session cleanup triggered by %s", current_user["email"])
    await cleanup_expired_sessions()
    return {
        "message": "Expired sessions cleaned up successfully",
        "performed_by": current_user["email"],
    }
