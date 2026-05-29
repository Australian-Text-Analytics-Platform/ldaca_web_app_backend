"""Database engine/session plumbing for async SQLAlchemy.

Used by:
- Application startup, route dependencies, and auth services because they
  need a shared async engine and session factory.

Flow: create the async engine from settings, provide a session dependency
    for FastAPI, and expose initialization helpers for application startup.
"""

import logging
from collections.abc import AsyncGenerator

from fastapi import Depends
from fastapi_users.db import SQLAlchemyUserDatabase
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from .models.db import User
from .settings import settings

logger = logging.getLogger(__name__)

engine = create_async_engine(settings.get_database_url())
async_session_maker = async_sessionmaker(engine, expire_on_commit=False)


async def create_db_and_tables():
    """Create all configured SQLAlchemy tables.

    Used by:
    - ``init_db`` because callers need the shared authentication and session persistence rule
      in one place instead of duplicating it.

    Why:
    - Ensures schema exists before auth/session operations begin.
    """
    from .models.db import Base

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    """Yield an async SQLAlchemy session for dependency injection.

    Used by:
    - FastAPI DB dependencies (``get_user_db``) because they need a backend boundary that
      validates inputs before delegating to workspace or worker state.

    Why:
    - Centralizes session lifecycle and transaction scope.
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
    """
    yield SQLAlchemyUserDatabase(session, User)


async def init_db():
    """Initialize persistent DB environment and schema.

    Used by:
    - app startup lifespan hook because startup and shutdown flows need the same
      initialization and cleanup behavior in packaged and local runs.

    Why:
    - Creates data root + schema before handling API traffic.
    """
    data_root = settings.get_data_root()
    data_root.mkdir(parents=True, exist_ok=True)
    await create_db_and_tables()
    logger.info("Database initialized at: %s", settings.get_database_url())
