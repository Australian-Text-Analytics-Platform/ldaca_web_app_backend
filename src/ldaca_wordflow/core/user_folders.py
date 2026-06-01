"""Per-user filesystem layout helpers.

Used by:
- Backend API routes, worker tasks, workspace services, and backend tests because they
  need a backend boundary that validates inputs before delegating to workspace or worker
  state.

Flow: resolve user root from settings, create (or return) the requested folder, validate
    workspace names, allocate unique workspace directories, and rename on-disk folders to
    match a desired display name.
"""

from pathlib import Path

from ..settings import settings


def _user_root_folder(user_id: str) -> Path:
    """Return the per-user root folder (``.../users/<name>/``).

    In single-user mode every caller shares ``user_root``; in multi-user mode
    the folder name is derived from the user id.

    Called by:
    - Local helpers, route handlers, or service methods in this module because they need a
      backend boundary that validates inputs before delegating to workspace or worker state.
    """
    folder_name = "user_root" if not settings.multi_user else f"user_{user_id}"
    return settings.get_data_root() / settings.user_data_folder / folder_name


def get_user_data_folder(user_id: str) -> Path:
    """Return the user's data folder, creating it if missing.

    Used by:
    - FastAPI application startup, backend API routes, backend package imports, backend
      tests, core workspace and worker services because they need a backend boundary that
      validates inputs before delegating to workspace or worker state.
    """
    user_data_folder = _user_root_folder(user_id) / "user_data"
    user_data_folder.mkdir(parents=True, exist_ok=True)
    return user_data_folder


def get_user_workspace_folder(user_id: str) -> Path:
    """Return the user's workspace folder, creating it if missing.

    Used by:
    - backend tests, core workspace and worker services because tests need the same
      observable contract that production routes and workers rely on.
    """
    workspace_folder = _user_root_folder(user_id) / "user_workspaces"
    workspace_folder.mkdir(parents=True, exist_ok=True)
    return workspace_folder


def get_user_cache_folder(user_id: str) -> Path:
    """Return the user's internal cache folder, creating it if missing.

    Lives outside ``user_data`` so it never appears in the file-tree endpoint
    that backs the data-loader UI. Subdirectories (e.g. ``embeddings/``) keep
    different cache kinds separate so each can be cleared independently.

    Used by:
    - backend API routes, core workspace and worker services because they need a backend
      boundary that validates inputs before delegating to workspace or worker state.
    """
    cache_folder = _user_root_folder(user_id) / "user_cache"
    cache_folder.mkdir(parents=True, exist_ok=True)
    return cache_folder


def validate_file_path(file_path: Path, user_folder: Path) -> bool:
    """Validate that a file path is within the user's allowed directory.

    Used by:
    - file CRUD, preview, and download routes in ``api/files/``.
    """

    try:
        file_path.resolve().relative_to(user_folder.resolve())
        return True
    except ValueError:
        return False


def validate_workspace_name(name: str) -> tuple[bool, str]:
    """Validate workspace names for safe, portable folder usage.

    Allows spaces and common punctuation but rejects path separators, control
    characters, and traversal markers.

    Used by:
    - backend API routes, core workspace and worker services because they need a backend
      boundary that validates inputs before delegating to workspace or worker state.
    """

    if name is None:
        return False, "name is required"

    trimmed = name.strip()
    if not trimmed:
        return False, "name cannot be empty"

    if ".." in trimmed:
        return False, "name cannot contain '..'"

    if "/" in trimmed or "\\" in trimmed:
        return False, "name cannot contain '/' or '\\'"

    for ch in trimmed:
        code = ord(ch)
        if code < 32 or code == 127:
            return False, "name cannot contain control characters"

    return True, ""


def allocate_workspace_folder(user_id: str, workspace_name: str) -> Path:
    """Create (and return) a unique folder for a workspace under the user's root.

    Used by:
    - core workspace and worker services because background jobs need one lifecycle owner
      for submission, progress, cancellation, and artifact cleanup.
    """

    base = get_user_workspace_folder(user_id)
    base.mkdir(parents=True, exist_ok=True)

    is_valid, reason = validate_workspace_name(workspace_name)
    if not is_valid:
        raise ValueError(reason)

    preferred = workspace_name.strip()
    candidate = preferred
    counter = 1
    while (base / candidate).exists():
        candidate = f"{preferred}_{counter}"
        counter += 1
    folder = base / candidate
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def ensure_display_folder_name(current_folder: Path, desired_name: str) -> Path:
    """Ensure the on-disk folder name matches the desired display name (with suffixes).

    If the current folder name already matches the sanitized desired name, it is
    returned unchanged. Otherwise, the folder is renamed to the first available
    ``<name>``, ``<name>_1``, ``<name>_2``, ... variant within the same parent.

    Used by:
    - core workspace and worker services because background jobs need one lifecycle owner
      for submission, progress, cancellation, and artifact cleanup.
    """

    parent = current_folder.parent
    is_valid, reason = validate_workspace_name(desired_name)
    if not is_valid:
        raise ValueError(reason)

    desired = desired_name.strip()
    target = parent / desired

    if current_folder == target:
        return current_folder

    if not target.exists():
        current_folder.rename(target)
        return target

    counter = 1
    while True:
        candidate = parent / f"{desired}_{counter}"
        if candidate == current_folder:
            return current_folder
        if not candidate.exists():
            current_folder.rename(candidate)
            return candidate
        counter += 1


def setup_user_folders(user_id: str) -> dict[str, Path]:
    """Create the complete per-user folder layout and return the paths.

    Used by the auth login/session bootstrap endpoints to guarantee every
    subsequent I/O call has a stable home. Sample data is no longer copied
    automatically — clients must hit the dedicated import endpoint.

    Used by:
    - backend API routes, backend tests because they need a backend boundary that validates
      inputs before delegating to workspace or worker state.
    """
    user_folder = _user_root_folder(user_id)
    user_data_folder = get_user_data_folder(user_id)
    user_workspaces_folder = get_user_workspace_folder(user_id)
    return {
        "user_folder": user_folder,
        "user_data": user_data_folder,
        "user_workspaces": user_workspaces_folder,
    }
