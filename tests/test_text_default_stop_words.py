"""Tests for the default stop words endpoint."""

import pytest
from httpx import ASGITransport, AsyncClient
from ldaca_web_app.main import app


@pytest.mark.asyncio
async def test_default_stop_words_endpoint_available():
    """Ensure the default stop words endpoint is reachable at /api/text."""

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/api/text/default-stop-words")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data["stopwords"], list)
        assert "the" in data["stopwords"]

        spanish = await client.get("/api/text/default-stop-words?language=es")
        assert spanish.status_code == 200
        spanish_data = spanish.json()
        assert isinstance(spanish_data["stopwords"], list)
        assert "de" in spanish_data["stopwords"]

        legacy = await client.get("/api/api/text/default-stop-words")
        assert legacy.status_code == 404


@pytest.mark.asyncio
async def test_default_stop_words_serves_chinese_list():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/api/text/default-stop-words?language=zh")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data["stopwords"], list)
        # 的 is the most common Chinese function word; if the zh resource
        # is wired up correctly it must be in the list.
        assert "的" in data["stopwords"]


@pytest.mark.asyncio
async def test_default_stop_words_serves_japanese_list():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/api/text/default-stop-words?language=ja")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data["stopwords"], list)
        # から (particle "from") and など (particle "and so on") are core
        # Japanese function words; both must be present if the ja resource
        # is wired up correctly.
        assert "から" in data["stopwords"]
        assert "など" in data["stopwords"]
        # ``japanese`` alias should resolve to the same list.
        long_form = await client.get(
            "/api/text/default-stop-words?language=japanese"
        )
        assert long_form.status_code == 200
        assert long_form.json()["stopwords"] == data["stopwords"]


@pytest.mark.asyncio
async def test_default_stop_words_strict_returns_empty_for_unknown_language():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Without strict, unknown language falls back to English (legacy
        # behaviour preserved for token-frequency).
        loose = await client.get("/api/text/default-stop-words?language=xx")
        assert loose.status_code == 200
        assert "the" in loose.json()["stopwords"]

        # With strict, unknown language returns an empty list so the
        # topic-modelling filter can hide its toggle cleanly.
        strict = await client.get(
            "/api/text/default-stop-words?language=xx&strict=true"
        )
        assert strict.status_code == 200
        assert strict.json()["stopwords"] == []
