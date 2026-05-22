from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from ldaca_wordflow import db
from sqlalchemy import select


def _unique_email(prefix: str) -> str:
    return f"{prefix}-{uuid4().hex}@example.com"


@pytest.mark.anyio
async def test_get_or_create_user_creates_then_updates_existing_user():
    email = _unique_email("db-user")

    created = await db.get_or_create_user(
        email=email,
        name="Test User",
        picture="https://example.com/avatar.jpg",
        google_id="google-123",
    )
    updated = await db.get_or_create_user(
        email=email,
        name="Updated User",
        picture="https://example.com/new-avatar.jpg",
        google_id="google-456",
    )

    assert updated["id"] == created["id"]
    assert updated["email"] == email
    assert updated["name"] == "Updated User"
    assert updated["picture"] == "https://example.com/new-avatar.jpg"
    assert updated["google_id"] == "google-456"
    assert updated["is_active"] is True
    assert updated["is_verified"] is True


@pytest.mark.anyio
async def test_create_user_session_replaces_old_session_and_validates_latest_token():
    user = await db.get_or_create_user(
        email=_unique_email("session-user"),
        name="Session User",
        picture="https://example.com/avatar.jpg",
        google_id="session-google-id",
    )

    first_session = await db.create_user_session(user["id"])
    second_session = await db.create_user_session(user["id"])

    assert first_session["access_token"] != second_session["access_token"]
    assert await db.validate_access_token(first_session["access_token"]) is None

    validated = await db.validate_access_token(second_session["access_token"])
    assert validated is not None
    assert validated["id"] == user["id"]
    assert validated["email"] == user["email"]
    assert validated["access_token"] == second_session["access_token"]
    assert validated["expires_at"] == second_session["expires_at"]


@pytest.mark.anyio
async def test_validate_access_token_returns_none_for_missing_token():
    assert await db.validate_access_token("missing-token") is None


@pytest.mark.anyio
async def test_cleanup_expired_sessions_removes_only_expired_rows():
    user = await db.get_or_create_user(
        email=_unique_email("cleanup-user"),
        name="Cleanup User",
        picture="https://example.com/avatar.jpg",
        google_id="cleanup-google-id",
    )
    user_id = user["id"]

    expired_token = f"expired-{uuid4().hex}"
    active_token = f"active-{uuid4().hex}"
    async with db.async_session_maker() as session:
        session.add_all(
            [
                db.UserSession(
                    user_id=user_id,
                    access_token=expired_token,
                    refresh_token=None,
                    expires_at=datetime.now(UTC).replace(tzinfo=None)
                    - timedelta(hours=1),
                ),
                db.UserSession(
                    user_id=user_id,
                    access_token=active_token,
                    refresh_token=None,
                    expires_at=datetime.now(UTC).replace(tzinfo=None)
                    + timedelta(hours=1),
                ),
            ]
        )
        await session.commit()

    await db.cleanup_expired_sessions()

    async with db.async_session_maker() as session:
        remaining_tokens = (
            (await session.execute(select(db.UserSession.access_token))).scalars().all()
        )

    assert expired_token not in remaining_tokens
    assert active_token in remaining_tokens
