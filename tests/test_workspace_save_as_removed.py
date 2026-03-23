async def test_workspace_save_as_route_is_removed(test_client):
    response = await test_client.post(
        "/api/workspaces/save-as",
        params={"folder_name": "workspace-copy"},
    )

    assert response.status_code == 404
