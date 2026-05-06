"""
Unified authentication endpoints following Single Source of Truth principle
"""

import logging
import secrets
from typing import Optional
from urllib.parse import urlencode

import httpx
from fastapi import APIRouter, Cookie, Depends, Form, Header, HTTPException, Query, Request
from google.auth.transport import requests as grequests
from google.oauth2 import id_token
from starlette.responses import RedirectResponse

from ..core.auth import (
    get_available_auth_methods,
    get_current_user,
    get_current_user_from_token,
)
from ..core.utils import setup_user_folders
from ..db import cleanup_expired_sessions, create_user_session, get_or_create_user
from ..models import AuthInfoResponse, GoogleIn, GoogleOut, User, UserResponse
from ..settings import settings

router = APIRouter(prefix="/auth", tags=["authentication"])
logger = logging.getLogger(__name__)


@router.get("/", response_model=AuthInfoResponse)
async def get_auth_info(authorization: Optional[str] = Header(None)):
    """
    Main auth endpoint - tells frontend everything it needs to know.

    Returns:
    - In single-user mode: authenticated=True with root user info
    - In multi-user mode with valid token: authenticated=True with user info
    - In multi-user mode without token: authenticated=False with available auth methods

    Used by:
    - frontend app bootstrap auth probe

    Why:
    - Provides one canonical auth capability + identity payload per startup.
    """
    if not settings.multi_user:
        # Single-user mode - return root user directly
        logger.debug("Single-user mode: returning root user info")

        # Ensure root user folders and sample data are set up
        user_folders = setup_user_folders(settings.single_user_id)
        logger.debug(f"Root user folders ensured at: {user_folders['user_folder']}")

        return AuthInfoResponse(
            authenticated=True,
            user=User(
                id=settings.single_user_id,
                name=settings.single_user_name,
                email=settings.single_user_email,
                picture=None,
            ),
            available_auth_methods=[],
            requires_authentication=False,
            data_folder=str(settings.get_data_root()),
        )

    # Multi-user mode - check for existing authentication
    if authorization and authorization.startswith("Bearer "):
        try:
            token = authorization.split(" ")[1]
            user = await get_current_user_from_token(token)
            logger.debug(f"Multi-user mode: authenticated user {user['email']}")

            return AuthInfoResponse(
                authenticated=True,
                user=User(
                    id=user["id"],
                    name=user["name"],
                    email=user["email"],
                    picture=user["picture"],
                ),
                available_auth_methods=get_available_auth_methods(),
                requires_authentication=True,
                data_folder=str(settings.get_data_root()),
            )
        except HTTPException:
            # Invalid token - fall through to unauthenticated response
            pass

    # Multi-user mode - not authenticated, return available auth methods
    logger.debug("Multi-user mode: not authenticated, returning auth methods")
    return AuthInfoResponse(
        authenticated=False,
        user=None,
        available_auth_methods=get_available_auth_methods(),
        requires_authentication=True,
        data_folder=str(settings.get_data_root()),
    )


@router.post("/google", response_model=GoogleOut)
async def google_auth(payload: GoogleIn):
    """Authenticate user via Google OAuth and create app session tokens.

    Used by the frontend Google sign-in flow (JSON body variant). Delegates
    the verification + provisioning logic to :func:`_verify_and_create_session`
    and shapes the response into ``GoogleOut``.
    """
    result = await _verify_and_create_session(payload.id_token)
    user = result["user"]
    session = result["session"]

    return GoogleOut(
        access_token=session["access_token"],
        refresh_token=session["refresh_token"],
        expires_in=session["expires_in"],
        scope="openid email profile",
        token_type="Bearer",
        user=User(
            id=user["id"],
            email=user["email"],
            name=user["name"],
            picture=user["picture"],
        ),
    )


async def _verify_and_create_session(credential: str) -> dict:
    """Verify a Google ID token and provision a local user session.

    Shared by both the JSON API (``google_auth``) and the redirect callback
    (``google_auth_callback``).
    """
    if not settings.multi_user:
        raise HTTPException(
            status_code=400,
            detail="Google authentication not available in single-user mode",
        )
    if not settings.google_client_id:
        raise HTTPException(
            status_code=500, detail="Google authentication not configured"
        )

    try:
        info = id_token.verify_oauth2_token(
            credential, grequests.Request(), audience=settings.google_client_id
        )
        logger.info(f"Google auth successful for: {info.get('email')}")
    except ValueError as e:
        logger.error(f"Google token verification failed: {e}")
        raise HTTPException(status_code=400, detail=f"Invalid ID token: {e}")
    except Exception as e:
        logger.error(f"Unexpected error during Google token verification: {e}")
        raise HTTPException(status_code=500, detail=f"Authentication error: {str(e)}")

    if not info.get("email_verified"):
        raise HTTPException(status_code=400, detail="Email not verified")

    user = await get_or_create_user(
        email=info.get("email"),
        name=info.get("name"),
        picture=info.get("picture"),
        google_id=info.get("sub"),
    )
    user_folders = setup_user_folders(user["id"])
    from ..db import update_user_folder_path

    await update_user_folder_path(user["id"], str(user_folders["user_folder"]))

    session = await create_user_session(user["id"])
    return {"user": user, "session": session}


@router.post("/google/callback")
async def google_auth_callback(
    request: Request,
    credential: str = Form(...),
    g_csrf_token: str = Form(None),
):
    """Handle the Google Identity Services redirect callback.

    Google POSTs the ID-token ``credential`` and a CSRF token here after the
    user authenticates on Google's consent page (``ux_mode: 'redirect'``).
    We verify the token, create/find the local user, issue a session, and
    redirect back to the SPA with the access token in the URL fragment.
    """
    if g_csrf_token:
        cookie_token = request.cookies.get("g_csrf_token")
        if cookie_token != g_csrf_token:
            raise HTTPException(status_code=403, detail="CSRF token mismatch")

    result = await _verify_and_create_session(credential)
    token = result["session"]["access_token"]

    redirect_url = f"/?{urlencode({'auth_token': token})}"
    return RedirectResponse(url=redirect_url, status_code=303)


# ---------------------------------------------------------------------------
# CILogon OIDC helpers
# ---------------------------------------------------------------------------

_cilogon_config_cache: dict | None = None


async def _get_cilogon_config() -> dict:
    """Fetch (and cache for the process lifetime) the CILogon discovery document."""
    global _cilogon_config_cache
    if _cilogon_config_cache is not None:
        return _cilogon_config_cache
    async with httpx.AsyncClient() as client:
        resp = await client.get(settings.cilogon_discovery_url, timeout=10)
        resp.raise_for_status()
        _cilogon_config_cache = resp.json()
    return _cilogon_config_cache


def _cilogon_redirect_uri(request: Request) -> str:
    """Return the registered callback URL, falling back to auto-detection."""
    if settings.cilogon_redirect_uri:
        return settings.cilogon_redirect_uri
    # Auto-detect: scheme + host + /api/auth/cilogon/callback
    # Works for local dev; production deployments should set CILOGON_REDIRECT_URI.
    base = str(request.base_url).rstrip("/")
    return f"{base}/api/auth/cilogon/callback"


# ---------------------------------------------------------------------------
# CILogon OIDC endpoints
# ---------------------------------------------------------------------------


@router.get("/cilogon/login")
async def cilogon_login(request: Request):
    """Redirect the browser to the CILogon authorization endpoint.

    Generates a random ``state`` for CSRF protection (stored as a short-lived
    cookie), builds the OIDC authorization URL from the discovery document,
    and redirects the user to CILogon to authenticate.
    """
    if not settings.multi_user:
        raise HTTPException(400, "CILogon not available in single-user mode")
    if not settings.cilogon_client_id:
        raise HTTPException(500, "CILogon not configured (missing CILOGON_CLIENT_ID)")

    config = await _get_cilogon_config()
    state = secrets.token_urlsafe(32)
    redirect_uri = _cilogon_redirect_uri(request)

    params = urlencode(
        {
            "response_type": "code",
            "client_id": settings.cilogon_client_id,
            "redirect_uri": redirect_uri,
            "scope": "openid email profile org.cilogon.userinfo",
            "state": state,
        }
    )
    auth_url = f"{config['authorization_endpoint']}?{params}"

    response = RedirectResponse(url=auth_url, status_code=302)
    response.set_cookie(
        "cilogon_state",
        state,
        max_age=600,  # 10 minutes — enough for the user to complete login
        httponly=True,
        samesite="lax",
    )
    return response


@router.get("/cilogon/callback")
async def cilogon_callback(
    request: Request,
    code: Optional[str] = Query(None),
    state: Optional[str] = Query(None),
    error: Optional[str] = Query(None),
    error_description: Optional[str] = Query(None),
):
    """Handle the CILogon authorization code callback.

    CILogon redirects here after the user authenticates. We verify the CSRF
    ``state``, exchange the authorization code for tokens, fetch the user's
    profile from the userinfo endpoint, provision a local session, and
    redirect back to the SPA with the access token in the URL query string.
    """
    if error:
        detail = error_description or error
        logger.error(
            "CILogon callback error — error=%r error_description=%r all_params=%s",
            error, error_description, dict(request.query_params),
        )
        raise HTTPException(400, f"CILogon authentication failed: {detail}")

    if not code or not state:
        raise HTTPException(400, "Missing code or state parameter")

    # CSRF check
    stored_state = request.cookies.get("cilogon_state")
    if not stored_state or stored_state != state:
        raise HTTPException(403, "State mismatch — possible CSRF")

    if not settings.multi_user:
        raise HTTPException(400, "CILogon not available in single-user mode")
    if not settings.cilogon_client_id:
        raise HTTPException(500, "CILogon not configured")

    config = await _get_cilogon_config()
    redirect_uri = _cilogon_redirect_uri(request)

    # Exchange authorization code for tokens
    async with httpx.AsyncClient() as client:
        try:
            token_resp = await client.post(
                config["token_endpoint"],
                data={
                    "grant_type": "authorization_code",
                    "code": code,
                    "redirect_uri": redirect_uri,
                    "client_id": settings.cilogon_client_id,
                    "client_secret": settings.cilogon_client_secret,
                },
                timeout=15,
            )
            token_resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error(f"CILogon token exchange failed: {exc.response.text}")
            raise HTTPException(502, "Token exchange with CILogon failed")

        tokens = token_resp.json()
        access_token = tokens.get("access_token")
        if not access_token:
            raise HTTPException(502, "No access_token in CILogon token response")

        # Fetch user profile from userinfo endpoint
        try:
            userinfo_resp = await client.get(
                config["userinfo_endpoint"],
                headers={"Authorization": f"Bearer {access_token}"},
                timeout=10,
            )
            userinfo_resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error(f"CILogon userinfo fetch failed: {exc.response.text}")
            raise HTTPException(502, "Fetching user info from CILogon failed")

    userinfo = userinfo_resp.json()
    logger.info(f"CILogon auth successful for: {userinfo.get('email')}")

    if not userinfo.get("email_verified", True):
        raise HTTPException(400, "Email not verified by CILogon")

    # Resolve display name: prefer 'name', fall back to given + family
    name = userinfo.get("name") or (
        f"{userinfo.get('given_name', '')} {userinfo.get('family_name', '')}".strip()
    ) or userinfo.get("email", "Unknown")

    user = await get_or_create_user(
        email=userinfo.get("email"),
        name=name,
        picture=userinfo.get("picture"),
        google_id=userinfo.get("sub"),  # reuse google_id column for OIDC sub
    )
    user_folders = setup_user_folders(user["id"])
    from ..db import update_user_folder_path

    await update_user_folder_path(user["id"], str(user_folders["user_folder"]))

    session = await create_user_session(user["id"])
    token = session["access_token"]

    redirect_url = f"/?{urlencode({'auth_token': token})}"
    response = RedirectResponse(url=redirect_url, status_code=303)
    response.delete_cookie("cilogon_state")
    return response


@router.get("/me", response_model=UserResponse)
async def get_current_user_info(current_user: dict = Depends(get_current_user)):
    """Return normalized current user profile fields.

    Used by:
    - frontend profile/session widgets

    Why:
    - Provides stable user response shape independent of DB row types.
    """
    # Convert datetime objects to ISO format strings
    created_at_str = (
        current_user["created_at"].isoformat() if current_user["created_at"] else ""
    )
    last_login_str = (
        current_user["last_login"].isoformat() if current_user["last_login"] else ""
    )

    return {
        "id": current_user["id"],  # Already a string from validate_access_token
        "email": current_user["email"],
        "name": current_user["name"],
        "picture": current_user["picture"],
        "is_active": current_user["is_active"],
        "is_verified": current_user["is_verified"],
        "created_at": created_at_str,
        "last_login": last_login_str,
    }


@router.post("/logout")
async def logout(current_user: dict = Depends(get_current_user)):
    """Logout current user session (multi-user) or no-op (single-user).

    Used by:
    - frontend sign-out action

    Why:
    - Keeps logout behavior mode-aware while preserving shared endpoint contract.
    """
    if not settings.multi_user:
        return {"message": "Logout not applicable in single-user mode"}

    await cleanup_expired_sessions()
    logger.info(f"User {current_user['email']} logged out successfully")
    return {"message": f"User {current_user['email']} logged out successfully"}


@router.get("/status")
async def auth_status(current_user: dict = Depends(get_current_user)):
    """Return minimal authenticated status payload.

    Used by:
    - lightweight frontend auth status checks

    Why:
    - Allows cheap auth verification without full `get_auth_info` metadata.
    """
    response: dict[str, object] = {
        "authenticated": True,
        "user": {
            "id": current_user["id"],
            "email": current_user["email"],
            "name": current_user["name"],
        },
    }

    # Add data folder path in single-user mode only
    if not settings.multi_user:
        response["data_folder"] = str(settings.get_data_root())

    return response


@router.get("/health")
async def auth_health():
    """Return authentication subsystem readiness metadata.

    Used by:
    - health/status probes and diagnostics pages

    Why:
    - Exposes auth mode and endpoint availability without authentication.
    """
    return {
        "status": "healthy",
        "mode": "single-user" if not settings.multi_user else "multi-user",
        "google_configured": bool(settings.google_client_id),
        "endpoints": {
            "auth_info": "/auth/",
            "google_auth": "/auth/google",
            "user_details": "/auth/me",
            "logout": "/auth/logout",
        },
    }
