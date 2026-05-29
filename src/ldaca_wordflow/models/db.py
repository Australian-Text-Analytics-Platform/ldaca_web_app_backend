"""ORM model definitions for SQLAlchemy declarative base.

Used by:
- backend package imports, analysis task helpers, and tests because they need the
  same model definitions that production routes and workers rely on.

Flow: define models with SQLAlchemy declarative mappings so they can be imported
    wherever schema-aware access (queries, migrations, tests) is needed.
"""

import uuid
from datetime import datetime

from fastapi_users.db import SQLAlchemyBaseUserTableUUID
from sqlalchemy import DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.sql import func


class Base(DeclarativeBase):
    """Declarative base used by SQLAlchemy models to share metadata.

    Used by:
    - analysis task helpers, backend package imports because analysis flows need per-user
      task state to survive across route calls and worker result persistence.
    """

    pass


class User(SQLAlchemyBaseUserTableUUID, Base):
    """User model with additional fields

    Used by:
    - backend API routes, backend package imports, backend request/response models, backend
      tests because they need a stable JSON contract shared by route handlers, generated
      clients, and tests.
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
    """

    __tablename__ = "user_sessions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("user.id"), nullable=False)
    access_token: Mapped[str] = mapped_column(String(255), nullable=False)
    refresh_token: Mapped[str | None] = mapped_column(String(255), nullable=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())
