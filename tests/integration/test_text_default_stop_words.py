import pytest
from httpx import ASGITransport, AsyncClient

from ldaca_wordflow.main import app


async def _get_default_stop_words(language: str | None = None, strict: bool = False):
    params: dict[str, str] = {}
    if language is not None:
        params["language"] = language
    if strict:
        params["strict"] = "true"

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        return await client.get("/api/text/default-stop-words", params=params)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("language", "expected_words"),
    [
        (None, {"the"}),
        ("es", {"de"}),
        ("zh", {"的"}),
        ("ja", {"から", "など"}),
        ("ko", {"그리고", "하지만"}),
    ],
)
async def test_default_stop_words_endpoint_serves_supported_languages(language, expected_words):
    response = await _get_default_stop_words(language)

    assert response.status_code == 200
    stop_words = response.json()["stopwords"]
    assert isinstance(stop_words, list)
    assert expected_words.issubset(set(stop_words))


@pytest.mark.asyncio
@pytest.mark.parametrize("alias", ["japanese", "korean"])
async def test_default_stop_words_long_form_aliases_match_short_codes(alias):
    short_code = {"japanese": "ja", "korean": "ko"}[alias]

    alias_response = await _get_default_stop_words(alias)
    short_code_response = await _get_default_stop_words(short_code)

    assert alias_response.status_code == 200
    assert short_code_response.status_code == 200
    assert alias_response.json()["stopwords"] == short_code_response.json()["stopwords"]


@pytest.mark.asyncio
async def test_default_stop_words_strict_returns_empty_for_unknown_language():
    loose = await _get_default_stop_words("xx")
    strict = await _get_default_stop_words("xx", strict=True)

    assert loose.status_code == 200
    assert "the" in loose.json()["stopwords"]
    assert strict.status_code == 200
    assert strict.json()["stopwords"] == []


@pytest.mark.asyncio
async def test_legacy_default_stop_words_double_api_path_is_not_registered():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        legacy = await client.get("/api/api/text/default-stop-words")

    assert legacy.status_code == 404