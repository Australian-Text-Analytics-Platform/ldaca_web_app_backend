async def test_tokenizer_models_endpoint_returns_polars_text_inventory(
    authenticated_client,
):
    response = await authenticated_client.get("/api/workspaces/tokenizer-models")

    assert response.status_code == 200
    models = response.json()["models"]
    models_by_id = {model["model"]: model for model in models}

    assert models_by_id["native:plain_words_en"]["languages"] == ["en"]
    assert models_by_id["huggingface:bert-base-uncased"]["languages"] == ["en"]
    assert models_by_id["lindera:jieba"]["languages"] == ["zh"]
    assert all(
        len(language) == 2 for model in models for language in model["languages"]
    )


async def test_set_node_document_column_persists_node_metadata(
    authenticated_client, tiny_node_id
):
    response = await authenticated_client.put(
        f"/api/workspaces/nodes/{tiny_node_id}/document-column",
        json={"document_column": "document"},
    )

    assert response.status_code == 200
    assert response.json()["document"] == "document"

    info_response = await authenticated_client.get(
        f"/api/workspaces/nodes/{tiny_node_id}"
    )
    assert info_response.status_code == 200
    assert info_response.json()["document"] == "document"


async def test_set_node_tokenization_preference_persists_column_metadata(
    authenticated_client, tiny_node_id
):
    response = await authenticated_client.put(
        f"/api/workspaces/nodes/{tiny_node_id}/tokenization-preference",
        json={
            "source_column": "document",
            "model": "native:plain_words_en",
            "language": "en-AU",
        },
    )

    assert response.status_code == 200
    tokenization = response.json()["tokenization"]
    assert (
        tokenization["document"]["column_name"]
        == "tokenization.document.native:plain_words_en"
    )
    assert tokenization["document"]["model"] == "native:plain_words_en"
    assert tokenization["document"]["language"] == "en"

    info_response = await authenticated_client.get(
        f"/api/workspaces/nodes/{tiny_node_id}"
    )
    assert info_response.status_code == 200
    assert (
        info_response.json()["tokenization"]["document"]["model"]
        == "native:plain_words_en"
    )


async def test_set_node_tokenization_preference_can_clear_column_metadata(
    authenticated_client, tiny_node_id
):
    set_response = await authenticated_client.put(
        f"/api/workspaces/nodes/{tiny_node_id}/tokenization-preference",
        json={
            "source_column": "document",
            "model": "native:plain_words_en",
            "language": "en",
        },
    )
    assert set_response.status_code == 200
    assert "document" in set_response.json()["tokenization"]

    clear_response = await authenticated_client.put(
        f"/api/workspaces/nodes/{tiny_node_id}/tokenization-preference",
        json={"source_column": "document", "model": None},
    )

    assert clear_response.status_code == 200
    assert "document" not in clear_response.json()["tokenization"]
