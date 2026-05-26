"""Language-routing helpers for analysis tools (Phase 3).

Tools that have per-language behavior (quotation extractor, topic embedder,
POS tagger, AI annotation prompts) need a single source of truth for "what
language is this corpus / this request" so they can either route to a
language-appropriate backend or refuse cleanly when they don't support it.

Resolution order is:
1. ``request_language`` — passed explicitly by the caller (e.g. frontend
   sends ``language="zh"``). This always wins.
2. ``Node.tokenization[*]["language"]`` — if the user has tokenised this node,
   the tokenization metadata records what language the tokeniser was configured
   for. Honor that as a fallback so the user doesn't have to re-state it.
3. Default ``"en"`` so existing English flows are unchanged when nothing
   has been specified anywhere.

Decision 4 + Phase 3.6: quotation extractor is English-only. Other tools
in Phase 3 should aim for graceful multilingual behaviour rather than
errors, but the typed exception is here so future English-only paths can
opt in without inventing their own error type.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from docworkspace import Node


DEFAULT_LANGUAGE = "en"

# Human-readable label for the language codes the analysis stack uses.
# Keep this list small and additive — anything missing falls through to
# the raw code, which is still a meaningful hint to LLM-driven tools.
_LANGUAGE_LABELS: dict[str, str] = {
    "en": "English",
    "zh": "Chinese",
    "ja": "Japanese",
    "ko": "Korean",
    "es": "Spanish",
    "fr": "French",
    "de": "German",
    "multi": "multilingual",
}


def language_label(code: str) -> str:
    """Return a human-friendly label for a language code, falling back to
    the code itself when unknown. Used by tools that surface the language
    to an end user or an LLM (e.g. AI annotation prompts)."""
    return _LANGUAGE_LABELS.get(code.lower(), code)


class UnsupportedLanguageError(Exception):
    """Raised when a tool is asked to run against a language it does not
    support. Carries ``tool`` and ``language`` fields so the API layer can
    build an informative response without parsing the message string.
    """

    def __init__(self, tool: str, language: str, *, message: str | None = None):
        self.tool = tool
        self.language = language
        super().__init__(
            message or f"{tool} does not support language {language!r} (English-only)"
        )


def effective_language(
    request_language: str | None,
    node: Node | None = None,
) -> str:
    """Resolve the language that an analysis tool should use.

    See the module docstring for the resolution order. Returns
    :data:`DEFAULT_LANGUAGE` if neither the request nor the node carries
    explicit language metadata.
    """
    if request_language:
        normalized = request_language.strip().lower()
        if normalized:
            return normalized

    if node is not None:
        tokenization = getattr(node, "tokenization", None)
        if isinstance(tokenization, dict):
            for meta in tokenization.values():
                if not isinstance(meta, dict):
                    continue
                lang = meta.get("language")
                if isinstance(lang, str) and lang.strip():
                    return lang.strip().lower()

    return DEFAULT_LANGUAGE


def require_language(
    tool: str,
    language: str,
    *,
    supported: tuple[str, ...] = (DEFAULT_LANGUAGE,),
) -> None:
    """Raise :class:`UnsupportedLanguageError` when ``language`` isn't in
    ``supported``. Use this at the boundary of an English-only tool to
    fail fast with a typed error.
    """
    if language not in supported:
        raise UnsupportedLanguageError(tool, language)


__all__ = [
    "DEFAULT_LANGUAGE",
    "UnsupportedLanguageError",
    "effective_language",
    "language_label",
    "require_language",
]
