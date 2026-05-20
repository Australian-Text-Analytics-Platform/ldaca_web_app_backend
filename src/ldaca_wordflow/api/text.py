"""
Text analysis utility endpoints
"""

import logging
from functools import lru_cache
from importlib import resources

from fastapi import APIRouter

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/text", tags=["text_analysis"])


@router.get("/default-stop-words")
async def get_default_stop_words(
    language: str = "english",
    strict: bool = False,
):
    """Return bundled default stop words for a language.

    Used by:
    - frontend token-frequency defaults loader (English fallback OK)
    - frontend topic-modelling post-fit stopword filter (passes
      ``strict=true`` so unknown languages return ``[]`` instead of
      silently substituting the English list — otherwise the filter
      toggle would appear with English words on a Chinese run that
      had no bundled zh list).

    Why:
    - Provides deterministic language stop-word sets from packaged
      resources. ``strict`` controls the unsupported-language behaviour:
      ``False`` (default) keeps the legacy English fallback for
      backwards compatibility; ``True`` returns an empty list.
    """
    try:
        return {"stopwords": _load_stopwords(language, strict)}
    except Exception as e:
        logger.error("Failed to load stopwords for language %s: %s", language, e)
        return {"error": f"Failed to load stopwords: {str(e)}", "stopwords": []}


LANGUAGE_FILE_MAP = {
    "english": "stopwords_en.txt",
    "en": "stopwords_en.txt",
    "spanish": "stopwords_es.txt",
    "es": "stopwords_es.txt",
    "french": "stopwords_fr.txt",
    "fr": "stopwords_fr.txt",
    "german": "stopwords_de.txt",
    "de": "stopwords_de.txt",
    "chinese": "stopwords_zh.txt",
    "zh": "stopwords_zh.txt",
    "japanese": "stopwords_ja.txt",
    "ja": "stopwords_ja.txt",
    "korean": "stopwords_ko.txt",
    "ko": "stopwords_ko.txt",
}


@lru_cache(maxsize=64)
def _load_stopwords(language: str, strict: bool = False) -> list[str]:
    """Load and cache stop words from packaged resource text files.

    Used by:
    - `get_default_stop_words`

    Why:
    - Avoids repeated disk/resource reads for common language requests.
    - ``strict=True`` makes unsupported language codes return ``[]``
      instead of falling through to the English list — needed for
      topic-modelling's language-aware filter so it can hide its toggle
      cleanly when no bundled list exists.
    """
    normalized = (language or "english").strip().lower()
    if normalized not in LANGUAGE_FILE_MAP:
        if strict:
            return []
        normalized = "english"
    filename = LANGUAGE_FILE_MAP[normalized]
    text = (
        resources.files("ldaca_wordflow.resources")
        .joinpath(filename)
        .read_text(encoding="utf-8")
    )
    return [
        line.strip()
        for line in text.splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]
