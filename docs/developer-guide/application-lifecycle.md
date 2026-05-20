# Application Lifecycle

## Startup

`src/ldaca_wordflow/main.py` defines the FastAPI app and a lifespan handler.
Startup does the operational setup that must exist before any API call:

- configure logging,
- create data, user, sample-data, and backup folders,
- initialize the async SQLite database,
- clean expired user sessions,
- start tokenizer/model prefetching,
- sweep stale token-cache files.

Shutdown closes logging resources, shuts down the shared worker pool, and cleans
expired sessions again.

## Settings

`settings.py` is the configuration object used by the app. Important values are
derived from environment variables but exposed as methods where paths depend on
other fields:

- data root defaults to `~/Documents/ldaca`,
- database URL is derived from the data root unless overridden,
- server host/port/logging configure uvicorn,
- single-user vs multi-user mode controls auth behavior,
- Google and CILogon values enable login methods.

`reload_settings()` rebuilds the settings singleton and updates the module-level
object in place. The config router uses this after setting `DATA_ROOT` so code
that imported `settings` sees the new values.

## Database And Sessions

`db.py` creates the async SQLAlchemy engine and session dependency. It stores:

- `User` rows, extending `fastapi-users` UUID user fields with display name,
  picture, Google id, folder path, creation time, and last login;
- `UserSession` rows for issued access tokens and expiry.

Single-user mode bypasses token lookup and returns the root user. Multi-user
mode validates bearer tokens through `validate_access_token()` and refreshes
`last_login` during login provisioning.

## Authentication Boundary

`core/auth.py` exposes `get_current_user()`, the dependency new protected routes
should use. It centralizes single-user behavior and bearer-token validation so
routers do not duplicate auth checks.

`api/auth.py` owns the login flows:

- `/auth/` returns auth configuration and current-user state,
- Google token and redirect callbacks verify identity tokens then create a
  backend session,
- CILogon endpoints use OIDC discovery and state cookies,
- `/me` and `/logout` expose session state and cleanup.

## Frontend Mounting

The backend can serve the production frontend from packaged resources under
`ldaca_wordflow.resources.frontend/build`. `_mount_frontend()`:

- locates the build directory or extracts `build.tar.gz`,
- injects runtime globals such as `window.__BASE_PATH__`,
  `window.__GOOGLE_CLIENT_ID__`, `window.__MULTI_USER__`, and CILogon config,
- mounts static asset directories,
- serves an SPA fallback for frontend routes.

This lets the Python package serve the full app from one process.

## Server Launcher

`start_server()` supports backend-only, frontend-only, and full-app modes. It
honors explicit host/port arguments, `LDACA_BACKEND_PORT`, `BACKEND_PORT`, and
JupyterHub-style root-path detection. The desktop launcher calls the CLI with
backend mode and sets the chosen backend port in the environment.
