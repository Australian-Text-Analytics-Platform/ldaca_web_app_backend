"""Database models and session helpers used by auth and application startup.

Used by:
- Backend package imports, application startup, and backend tests because tests need the
  same observable contract that production routes and workers rely on.

Flow: open the configured database/session boundary, normalize user or token records,
    and return dependency-ready authentication state.
"""

import logging
import secrets
import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from typing import Any, cast

from fastapi import Depends
from fastapi_users.db import SQLAlchemyBaseUserTableUUID, SQLAlchemyUserDatabase
from sqlalchemy import DateTime, ForeignKey, Integer, String, Text, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.sql import func
from sqlalchemy.sql.elements import ColumnElement

from .settings import settings

logger = logging.getLogger(__name__)


# SQLAlchemy setup
class Base(DeclarativeBase):
    """Declarative base used by SQLAlchemy models to share metadata.

    Used by:
    - analysis task helpers, backend package imports because analysis flows need per-user
      task state to survive across route calls and worker result persistence.

    Flow: open the configured database/session boundary, normalize user or token records,
        and return dependency-ready authentication state.
    """

    pass


class User(SQLAlchemyBaseUserTableUUID, Base):
    """User model with additional fields

    Used by:
    - backend API routes, backend package imports, backend request/response models, backend
      tests because they need a stable JSON contract shared by route handlers, generated
      clients, and tests.

    Flow: open the configured database/session boundary, normalize user or token records,
        and return dependency-ready authentication state.
    """

    name: Mapped[str] = mapped_column(String(255), nullable=False)
    picture: Mapped[str | None] = mapped_column(Text, nullable=True)
    google_id: Mapped[str | None] = mapped_column(
        String(255), nullable=True, unique=True
    )
    user_folder_path: Mapped[str | None] = mapped_column(String(512), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    last_login: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class UserSession(Base):
    """User session model for token management

    Used by:
    - backend API routes, backend package imports, backend tests because they need a backend
      boundary that validates inputs before delegating to workspace or worker state.

    Flow: open the configured database/session boundary, normalize user or token records,
        and return dependency-ready authentication state.
    """

    __tablename__ = "user_sessions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("user.id"), nullable=False)
    access_token: Mapped[str] = mapped_column(String(255), nullable=False)
    refresh_token: Mapped[str | None] = mapped_column(String(255), nullable=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())


# Create async engine and session maker
# Use derived URL which respects DATA_ROOT
engine = create_async_engine(settings.get_database_url())
async_session_maker = async_sessionmaker(engine, expire_on_commit=False)


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


def _user_to_dict(user: User) -> dict[str, Any]:
    """Serialize a `User` row into the dict shape expected by API callers.

    Called by:
    - Local helpers, route handlers, or service methods in this module because they need a
      backend boundary that validates inputs before delegating to workspace or worker state.

    Flow: open the configured database/session boundary, normalize user or token records,
        and return dependency-ready authentication state.
    """
    payload: dict[str, Any] = {"id": str(user.id)}
    for field in _USER_FIELDS:
        payload[field] = getattr(user, field)
    return payload


async def create_db_and_tables():
    """Create all configured SQLAlchemy tables.

    Used by:
    - `init_db` because callers need the shared authentication and session persistence rule
      in one place instead of duplicating it.
    Why:
    - Ensures schema exists before auth/session operations begin.

    Flow: open the configured database/session boundary, normalize user or token records,
        and return dependency-ready authentication state.
    """
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    """Yield an async SQLAlchemy session for dependency injection.

    Used by:
    - FastAPI DB dependencies (`get_user_db`) because they need a backend boundary that
      validates inputs before delegating to workspace or worker state.
    Why:
    - Centralizes session lifecycle and transaction scope.

    Flow: open the configured database/session boundary, normalize user or token records,
        and return dependency-ready authentication state.
    """
    async with async_session_maker() as session:
        yield session


async def get_user_db(session: AsyncSession = Depends(get_async_session)):
    """Yield FastAPI Users DB adapter bound to current async session.

    Used by:
    - auth/user management dependencies because callers need the shared authentication and
      session persistence rule in one place instead of duplicating it.
    Why:
    - Bridges app user model with fastapi-users integration points.

    Flow: open the configured database/session boundary, normalize user or token records,
        and return dependency-ready authentication state.
    """
    yield SQLAlchemyUserDatabase(session, User)


async def init_db():
    """Initialize persistent DB environment and schema.

    Used by:
    - app startup lifespan hook because startup and shutdown flows need the same
      initialization and cleanup behavior in packaged and local runs.
    Why:
    - Creates data root + schema before handling API traffic.

    Flow: open the configured database/session boundary, normalize user or token records,
        and return dependency-ready authentication state.
    """
    # Ensure DATA_ROOT exists before creating/opening DB file
    data_root = settings.get_data_root()
    data_root.mkdir(parents=True, exist_ok=True)
    await create_db_and_tables()
    logger.info("Database initialized at: %s", settings.get_database_url())


async def get_or_create_user(
    email: str, name: str, picture: str, google_id: str
) -> dict[str, Any]:
    """Fetch existing user by email or create/update OAuth user record.

    Used by:
    - `api.auth.google_auth` because they need a backend boundary that validates inputs
      before delegating to workspace or worker state.
    Why:
    - Maintains idempotent user provisioning from Google identity payloads.

    Flow: open the configured database/session boundary, normalize user or token records,
        and return dependency-ready authentication state.
    """
    async with async_session_maker() as session:
        # Try to get existing user by email
        result = await session.execute(
            select(User).where(cast(ColumnElement[bool], User.email == email))
        )
        user = result.scalar_one_or_none()

        if user:
            # Update existing user info and last login
            user.name = name
            user.picture = picture
            user.google_id = google_id
            user.last_login = datetime.now(UTC).replace(tzinfo=None)
            await session.commit()
            await session.refresh(user)
        else:
            # Create new user
            user = User(
                email=email,
                name=name,
                picture=picture,
                google_id=google_id,
                user_folder_path=None,  # Will be set when folders are created
                last_login=datetime.now(UTC).replace(tzinfo=None),
                is_active=True,
                is_superuser=False,
                is_verified=True,  # Auto-verify Google users
                hashed_password="oauth_user",  # Placeholder for OAuth users
            )
            session.add(user)
            await session.commit()
            await session.refresh(user)

        return _user_to_dict(user)


async def create_user_session(user_id: str) -> dict[str, Any]:
    """Create/replace active session token pair for a user.

    Used by:
    - `api.auth.google_auth` because they need a backend boundary that validates inputs
      before delegating to workspace or worker state.
    Why:
    - Enforces single active session row per user in current design.

    Flow: open the configured database/session boundary, normalize user or token records,
        and return dependency-ready authentication state.
    """
    async with async_session_maker() as session:
        # Generate our own access token
        access_token = secrets.token_urlsafe(32)
        refresh_token = secrets.token_urlsafe(32)

        # Calculate expiry time
        expires_at = datetime.now(UTC).replace(tzinfo=None) + timedelta(
            hours=settings.token_expire_hours
        )

        # Clean up old sessions for this user (optional - keep only latest)
        result = await session.execute(
            select(UserSession).where(UserSession.user_id == uuid.UUID(user_id))
        )
        old_sessions = result.scalars().all()
        for old_session in old_sessions:
            await session.delete(old_session)

        # Create new session
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
            "expires_in": settings.token_expire_hours * 3600,  # in seconds
            "expires_at": expires_at,
        }


async def validate_access_token(access_token: str) -> dict[str, Any] | None:
    """Validate access token and return user/session payload when active.

    Used by:
    - auth dependency validation paths because callers need the shared authentication and
      session persistence rule in one place instead of duplicating it.
    Why:
    - Centralizes token expiry and join logic for user identity resolution.

    Flow: open the configured database/session boundary, normalize user or token records,
        and return dependency-ready authentication state.
    """
    async with async_session_maker() as session:
        result = await session.execute(
            select(User, UserSession)
            .join(
                UserSession,
                cast(ColumnElement[bool], User.id == UserSession.user_id),
            )
            .where(UserSession.access_token == access_token)
            .where(UserSession.expires_at > datetime.now(UTC).replace(tzinfo=None))
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

    Flow: open the configured database/session boundary, normalize user or token records,
        and return dependency-ready authentication state.
    """
    async with async_session_maker() as session:
        result = await session.execute(
            select(User).where(cast(ColumnElement[bool], User.email == email))
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

    Flow: open the configured database/session boundary, normalize user or token records,
        and return dependency-ready authentication state.
    """
    async with async_session_maker() as session:
        result = await session.execute(
            select(UserSession).where(
                UserSession.expires_at <= datetime.now(UTC).replace(tzinfo=None)
            )
        )
        expired_sessions = result.scalars().all()
        for expired_session in expired_sessions:
            await session.delete(expired_session)
        await session.commit()


async def update_user_folder_path(user_id: str, folder_path: str) -> None:
    """Persist user folder location after storage provisioning.

    Used by:
    - `api.auth.google_auth` because they need a backend boundary that validates inputs
      before delegating to workspace or worker state.
    Why:
    - Keeps DB user metadata aligned with filesystem initialization.

    Flow: open the configured database/session boundary, normalize user or token records,
        and return dependency-ready authentication state.
    """
    async with async_session_maker() as session:
        result = await session.execute(
            select(User).where(cast(ColumnElement[bool], User.id == uuid.UUID(user_id)))
        )
        user = result.scalar_one_or_none()

        if user:
            user.user_folder_path = folder_path
            await session.commit()
            logger.info("Updated user %s folder path to: %s", user_id, folder_path)
        else:
            logger.warning("User %s not found for folder path update", user_id)
