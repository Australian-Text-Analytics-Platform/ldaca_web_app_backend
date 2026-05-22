"""Behavioural tests for the ``node_name`` query parameter on POST /workspaces/nodes."""


async def test_add_node_without_node_name_derives_default(
    authenticated_client, workspace_id, tiny_text_file
):
    response = await authenticated_client.post(
        "/api/workspaces/nodes",
        params={"filename": tiny_text_file.name},
    )

    assert response.status_code == 200
    payload = response.json()
    # ``tiny_text_file`` lives at the user data root, so the default is the
    # stem of the filename — the same logic the frontend's
    # ``defaultNodeNameFromFile`` helper mirrors.
    expected_stem = tiny_text_file.name.rsplit(".", 1)[0]
    assert payload["name"] == expected_stem


async def test_add_node_honours_user_supplied_node_name(
    authenticated_client, workspace_id, tiny_text_file
):
    response = await authenticated_client.post(
        "/api/workspaces/nodes",
        params={"filename": tiny_text_file.name, "node_name": "my_corpus"},
    )

    assert response.status_code == 200
    assert response.json()["name"] == "my_corpus"


async def test_add_node_falls_back_to_default_when_node_name_is_blank(
    authenticated_client, workspace_id, tiny_text_file
):
    # Whitespace-only ``node_name`` is treated the same as an absent value,
    # which keeps the frontend's "leave blank to use the suggestion"
    # affordance symmetric on both ends.
    response = await authenticated_client.post(
        "/api/workspaces/nodes",
        params={"filename": tiny_text_file.name, "node_name": "   "},
    )

    assert response.status_code == 200
    expected_stem = tiny_text_file.name.rsplit(".", 1)[0]
    assert response.json()["name"] == expected_stem
