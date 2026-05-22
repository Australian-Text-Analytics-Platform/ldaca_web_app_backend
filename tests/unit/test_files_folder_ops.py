from ldaca_wordflow.core.utils import get_user_data_folder


async def test_delete_empty_folder_succeeds(authenticated_client):
    user_data_folder = get_user_data_folder("test")
    folder = user_data_folder / "empty_dir"
    folder.mkdir(parents=True, exist_ok=True)

    response = await authenticated_client.delete("/api/files/folders/empty_dir")

    assert response.status_code == 200
    assert not folder.exists()


async def test_delete_folder_with_only_hidden_entries_succeeds(authenticated_client):
    # The file tree filters hidden dotfiles, so a folder that contains only
    # ``.DS_Store`` (macOS) or similar should still count as "empty" from the
    # user's perspective and not require ``recursive=true``.
    user_data_folder = get_user_data_folder("test")
    folder = user_data_folder / "looks_empty"
    folder.mkdir(parents=True, exist_ok=True)
    (folder / ".DS_Store").write_bytes(b"\x00\x00\x00\x00")

    response = await authenticated_client.delete("/api/files/folders/looks_empty")

    assert response.status_code == 200
    assert not folder.exists()


async def test_delete_non_empty_folder_without_recursive_returns_conflict(
    authenticated_client,
):
    user_data_folder = get_user_data_folder("test")
    folder = user_data_folder / "with_files"
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "a.csv").write_text("col\n1", encoding="utf-8")

    response = await authenticated_client.delete("/api/files/folders/with_files")

    assert response.status_code == 409
    assert folder.exists()
    assert (folder / "a.csv").exists()


async def test_delete_non_empty_folder_with_recursive_removes_everything(
    authenticated_client,
):
    user_data_folder = get_user_data_folder("test")
    folder = user_data_folder / "with_tree"
    nested = folder / "nested"
    nested.mkdir(parents=True, exist_ok=True)
    (folder / "a.csv").write_text("col\n1", encoding="utf-8")
    (nested / "b.csv").write_text("col\n2", encoding="utf-8")

    response = await authenticated_client.delete(
        "/api/files/folders/with_tree?recursive=true"
    )

    assert response.status_code == 200
    assert not folder.exists()


async def test_delete_folder_rejects_traversal(authenticated_client):
    # ``%2E%2E`` keeps the traversal segment intact through URL normalisation
    # so the request still arrives at the endpoint with a path that escapes
    # the user data folder.
    response = await authenticated_client.delete(
        "/api/files/folders/legit/%2E%2E/%2E%2E"
    )

    assert response.status_code in (400, 403, 404)


async def test_delete_missing_folder_returns_not_found(authenticated_client):
    response = await authenticated_client.delete("/api/files/folders/does_not_exist")

    assert response.status_code == 404


async def test_delete_folder_rejects_file_target(authenticated_client):
    user_data_folder = get_user_data_folder("test")
    file_path = user_data_folder / "plain.csv"
    file_path.write_text("col\n1", encoding="utf-8")

    response = await authenticated_client.delete("/api/files/folders/plain.csv")

    assert response.status_code == 404
    assert file_path.exists()


async def test_move_folder_into_sibling_directory(authenticated_client):
    user_data_folder = get_user_data_folder("test")
    src = user_data_folder / "src_parent" / "movable"
    sibling = user_data_folder / "destination"
    src.mkdir(parents=True, exist_ok=True)
    sibling.mkdir(parents=True, exist_ok=True)
    (src / "data.csv").write_text("col\n1", encoding="utf-8")

    response = await authenticated_client.post(
        "/api/files/move",
        json={"source_path": "src_parent/movable", "target_directory_path": "destination"},
    )

    assert response.status_code == 200
    assert not src.exists()
    moved = sibling / "movable" / "data.csv"
    assert moved.exists()


async def test_move_folder_into_self_descendant_is_rejected(authenticated_client):
    user_data_folder = get_user_data_folder("test")
    src = user_data_folder / "root_dir"
    descendant = src / "child"
    descendant.mkdir(parents=True, exist_ok=True)

    response = await authenticated_client.post(
        "/api/files/move",
        json={"source_path": "root_dir", "target_directory_path": "root_dir/child"},
    )

    assert response.status_code == 400
    assert src.exists()
    assert descendant.exists()


async def test_move_folder_to_root_succeeds(authenticated_client):
    user_data_folder = get_user_data_folder("test")
    src = user_data_folder / "nested_parent" / "movable"
    src.mkdir(parents=True, exist_ok=True)
    (src / "data.csv").write_text("col\n1", encoding="utf-8")

    response = await authenticated_client.post(
        "/api/files/move",
        json={"source_path": "nested_parent/movable", "target_directory_path": ""},
    )

    assert response.status_code == 200
    assert (user_data_folder / "movable" / "data.csv").exists()
