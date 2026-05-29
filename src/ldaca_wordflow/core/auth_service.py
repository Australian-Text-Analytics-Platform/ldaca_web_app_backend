"""Auth business logic: user provisioning, session management, token validation.

Used by:
- API auth routes, admin routes, and core auth dependencies because they need a
  single source of truth for user/session persistence operations.

Flow: open a short-lived async session, execute the relevant query/mutation,
    and return normalized dicts or None to the caller.
"""

import logging
import secrets
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, cast

from sqlalchemy import select
from sqlalchemy.sql.elements import ColumnElement

from .. import db as _db
from ..models.db import User, UserSession
from ..settings import settings

logger = logging.getLogger(__name__)

_USER_FIELDS = (
    "email",
    "name",
    "picture",
    "google_id",
    "user_folder_path",
    "created_at",
    "last_login",
    "is_active",
    "is_superuser",
    "is_verified",
)


def _utc_now_naive() -> datetime:
    """Return the current UTC time as a timezone-naive datetime.

    Used by:
    - All auth service functions that compare or assign timestamps against
      the database (which stores naive UTC), and by admin routes that
      perform expiry checks.
    """
    return datetime.now(UTC).replace(tzinfo=None)


def _generate_session_tokens() -> tuple[str, str]:
    """Produce a cryptographically random access+refresh token pair.

    Called by:
    - ``create_user_session`` so token generation logic is reusable and
      testable in isolation.
    """
    return secrets.token_urlsafe(32), secrets.token_urlsafe(32)


def _user_to_dict(user: User) -> dict[str, Any]:
    """Serialize a ``User`` row into the dict shape expected by API callers.

    Called by:
    - Local helpers, route handlers, or service methods in this module because they need a
      backend boundary that validates inputs before delegating to workspace or worker state.
    """
    payload: dict[str, Any] = {"id": str(user.id)}
    for field in _USER_FIELDS:
        payload[field] = getattr(user, field)
    return payload


async def get_or_create_user(
    email: str, name: str, picture: str, google_id: str
) -> dict[str, Any]:
    """Fetch existing user by email or create/update OAuth user record.

    Used by:
    - ``api.auth.google_auth`` because they need a backend boundary that validates inputs
      before delegating to workspace or worker state.

    Why:
    - Maintains idempotent user provisioning from Google identity payloads.
    """
    async with _db.async_session_maker() as session:
        result = await session.execute(
            select(User).where(
                cast(ColumnElement[bool], User.email == email)
            )
        )
        user = result.scalar_one_or_none()

        if user:
            user.name = name
            user.picture = picture
            user.google_id = google_id
            user.last_login = _utc_now_naive()
            await session.commit()
            await session.refresh(user)
        else:
            user = User(
                email=email,
                name=name,
                picture=picture,
                google_id=google_id,
                user_folder_path=None,
                last_login=_utc_now_naive(),
                is_active=True,
                is_superuser=False,
                is_verified=True,
                hashed_password="oauth_user",
            )
            session.add(user)
            await session.commit()
            await session.refresh(user)

        return _user_to_dict(user)


async def create_user_session(user_id: str) -> dict[str, Any]:
    """Create/replace active session token pair for a user.

    Used by:
    - ``api.auth.google_auth`` because they need a backend boundary that validates inputs
      before delegating to workspace or worker state.

    Why:
    - Enforces single active session row per user in current design.
    """
    async with _db.async_session_maker() as session:
        access_token, refresh_token = _generate_session_tokens()
        expires_at = _utc_now_naive() + timedelta(hours=settings.token_expire_hours)

        result = await session.execute(
            select(UserSession).where(UserSession.user_id == uuid.UUID(user_id))
        )
        old_sessions = result.scalars().all()
        for old_session in old_sessions:
            await session.delete(old_session)

        new_session = UserSession(
            user_id=uuid.UUID(user_id),
            access_token=access_token,
            refresh_token=refresh_token,
            expires_at=expires_at,
        )
        session.add(new_session)
        await session.commit()

        return {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "expires_in": settings.token_expire_hours * 3600,
            "expires_at": expires_at,
        }


async def validate_access_token(access_token: str) -> dict[str, Any] | None:
    """Validate access token and return user/session payload when active.

    Used by:
    - auth dependency validation paths because callers need the shared authentication and
      session persistence rule in one place instead of duplicating it.

    Why:
    - Centralizes token expiry and join logic for user identity resolution.
    """
    async with _db.async_session_maker() as session:
        result = await session.execute(
            select(User, UserSession)
            .join(
                UserSession,
                cast(ColumnElement[bool], User.id == UserSession.user_id),
            )
            .where(UserSession.access_token == access_token)
            .where(UserSession.expires_at > _utc_now_naive())
        )
        row = result.first()

        if row:
            user, session_data = row
            payload = _user_to_dict(user)
            payload["access_token"] = session_data.access_token
            payload["expires_at"] = session_data.expires_at
            return payload
        return None


async def get_user_by_email(email: str) -> dict[str, Any] | None:
    """Return user payload by email when present.

    Used by:
    - auth/user administration lookup paths because callers need the shared authentication
      and session persistence rule in one place instead of duplicating it.

    Why:
    - Provides a consistent dict payload shape for caller code.
    """
    async with _db.async_session_maker() as session:
        result = await session.execute(
            select(User).where(
                cast(ColumnElement[bool], User.email == email)
            )
        )
        user = result.scalar_one_or_none()

        if user:
            return _user_to_dict(user)
        return None


async def cleanup_expired_sessions():
    """Delete expired session rows from storage.

    Used by:
    - app startup/shutdown maintenance and logout flows because callers need the shared
      authentication and session persistence rule in one place instead of duplicating it.

    Why:
    - Prevents stale sessions from accumulating indefinitely.
    """
    async with _db.async_session_maker() as session:
        result = await session.execute(
            select(UserSession).where(
                UserSession.expires_at <= _utc_now_naive()
            )
        )
        expired_sessions = result.scalars().all()
        for expired_session in expired_sessions:
            await session.delete(expired_session)
        await session.commit()


async def update_user_folder_path(user_id: str, folder_path: str) -> None:
    """Persist user folder location after storage provisioning.

    Used by:
    - ``api.auth.google_auth`` because they need a backend boundary that validates inputs
      before delegating to workspace or worker state.

    Why:
    - Keeps DB user metadata aligned with filesystem initialization.
    """
    async with _db.async_session_maker() as session:
        result = await session.execute(
            select(User).where(
                cast(ColumnElement[bool], User.id == uuid.UUID(user_id))
            )
        )
        user = result.scalar_one_or_none()

        if user:
            user.user_folder_path = folder_path
            await session.commit()
            logger.info("Updated user %s folder path to: %s", user_id, folder_path)
        else:
            logger.warning("User %s not found for folder path update", user_id)
