from ldaca_web_app.api.workspaces.analyses.token_frequencies import (
    DEFAULT_TOKEN_LIMIT,
    _coerce_limit_value,
)


def test_token_frequency_defaults_to_25_when_limit_is_missing():
    assert DEFAULT_TOKEN_LIMIT == 25
    assert _coerce_limit_value(None) == 25
    assert _coerce_limit_value(0) == 25
