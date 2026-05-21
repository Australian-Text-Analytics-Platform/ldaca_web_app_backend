from ldaca_wordflow.core.oni_client import (
    OniClient,
    OniSearchMethod,
    build_search_body,
    extract_ldaca_identifier,
    jsonld_value,
)


def test_extract_ldaca_identifier_from_portal_collection_url() -> None:
    identifier = extract_ldaca_identifier(
        "https://data.ldaca.edu.au/collection?"
        "id=arcp%3A%2F%2Fname%2Chdl10.26180~23961609&"
        "_crateId=arcp%3A%2F%2Fname%2Chdl10.26180~23961609"
    )

    assert identifier == "arcp://name,hdl10.26180~23961609"


def test_extract_ldaca_identifier_accepts_raw_arcp_id() -> None:
    assert (
        extract_ldaca_identifier(" arcp://name,hdl10.26180~23961609 ")
        == "arcp://name,hdl10.26180~23961609"
    )


def test_jsonld_value_normalizes_common_oni_shapes() -> None:
    assert jsonld_value([{"@value": "COOEE"}]) == "COOEE"
    assert jsonld_value({"@id": "arcp://name,cooee"}) == "arcp://name,cooee"
    assert jsonld_value(["text/plain", {"@id": "https://example.org/pronom"}]) == [
        "text/plain",
        "https://example.org/pronom",
    ]
    assert jsonld_value(None) is None


def test_oni_client_uses_bearer_token_header() -> None:
    client = OniClient(base_url="https://data.ldaca.edu.au/api", token="portal-token")
    assert client._headers() == {"Authorization": "Bearer portal-token"}


def test_build_string_search_body_uses_multi_match_and_small_source() -> None:
    body = build_search_body(
        method=OniSearchMethod.KEYWORD,
        query="conversation",
        limit=12,
        offset=24,
    )

    assert body["size"] == 12
    assert body["from"] == 24
    assert "_text" not in body["_source"]
    assert body["query"] == {
        "multi_match": {
            "query": "conversation",
            "fields": ["name.@value", "description.@value", "_text", "@id"],
        }
    }


def test_build_identifier_search_body_accepts_new_identifier_method() -> None:
    body = build_search_body(
        method=OniSearchMethod.IDENTIFIER,
        query="arcp://name,hdl10.26180~23961609",
        limit=10,
        offset=0,
    )

    assert body["query"]["bool"]["minimum_should_match"] == 1
    assert {item["term"].popitem()[1] for item in body["query"]["bool"]["should"]} == {
        "arcp://name,hdl10.26180~23961609"
    }


def test_build_collection_search_body_filters_top_level_collections() -> None:
    body = build_search_body(
        method=OniSearchMethod.COLLECTION,
        query="",
        limit=5,
        offset=0,
    )

    assert body["query"] == {
        "bool": {
            "filter": [
                {"terms": {"@type.keyword": ["Dataset", "RepositoryCollection"]}},
                {"terms": {"_isTopLevel.@value.keyword": ["true"]}},
            ]
        }
    }
