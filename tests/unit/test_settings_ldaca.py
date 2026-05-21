from ldaca_wordflow.settings import Settings


def test_featured_collection_ids_do_not_split_arcp_comma() -> None:
    app_settings = Settings(
        ldaca_oni_featured_collection_ids="arcp://name,hdl10.26180~23961609"
    )

    assert app_settings.get_ldaca_oni_featured_collection_ids() == [
        "arcp://name,hdl10.26180~23961609"
    ]


def test_featured_collection_ids_support_semicolon_and_json_lists() -> None:
    semicolon_settings = Settings(
        ldaca_oni_featured_collection_ids=(
            "arcp://name,hdl10.26180~23961609; arcp://name,hdl10.26180~other"
        )
    )
    json_settings = Settings(
        ldaca_oni_featured_collection_ids=(
            '["arcp://name,hdl10.26180~23961609", "arcp://name,hdl10.26180~other"]'
        )
    )

    expected = [
        "arcp://name,hdl10.26180~23961609",
        "arcp://name,hdl10.26180~other",
    ]
    assert semicolon_settings.get_ldaca_oni_featured_collection_ids() == expected
    assert json_settings.get_ldaca_oni_featured_collection_ids() == expected
