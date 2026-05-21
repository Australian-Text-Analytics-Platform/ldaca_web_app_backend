"""
Configuration management using pydantic-settings.
Settings are loaded from environment variables with sensible defaults.
Users are responsible for setting environment variables themselves.
"""

import json
import re
from pathlib import Path
from secrets import token_urlsafe
from typing import Any

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables with defaults."""

    @field_validator("debug", mode="before")
    @classmethod
    def normalize_debug_value(cls, value: Any) -> Any:
        """Normalize common deployment debug strings before bool parsing."""
        if isinstance(value, str):
            normalized_value = value.strip().lower()
            if normalized_value in {"release", "prod", "production"}:
                return False
            if normalized_value in {"debug", "dev", "development"}:
                return True
        return value

    # Root for all data-related storage (folders and DB)
    data_root: str | Path = Field(
        default=Path.home() / "Documents" / "ldaca",
        description="Root data folder",
    )

    # Database Configuration
    # If database_url is not provided, we derive it from data_root and database_file
    database_url: str | None = Field(
        default=None,
        description="Database connection URL (optional; derived from data_root if omitted)",
    )
    database_file: str = Field(
        default="users.db", description="SQLite database filename"
    )
    database_backup_folder: str = Field(
        default="backups", description="Database backup folder (relative to data_root)"
    )

    # Data Folders
    user_data_folder: str = Field(
        default="users", description="User data folder (relative to data_root)"
    )
    sample_data: str | Path | None = Field(
        default=None,
        description="Optional sample data folder override (filesystem path)",
    )
    sample_data_remote_url: str | None = Field(
        default="https://raw.githubusercontent.com/Australian-Text-Analytics-Platform/ldaca-analytics-sample-data/main",
        description=(
            "Base URL for remote sample datasets. The backend fetches catalogue.json "
            "from this URL and downloads any missing or changed files in the background "
            "after the bundled datasets are copied. Set to empty string to disable."
        ),
    )

    # Server Configuration
    server_host: str = Field(default="0.0.0.0", description="Server host")
    backend_port: int = Field(default=8001, description="Backend server port")
    debug: bool = Field(default=False, description="Debug mode")
    log_level: str = Field(
        default="INFO",
        description="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
    )
    log_file: str | None = Field(
        default=None,
        description="Log file name (relative to data_root). None disables file logging.",
    )
    quotation_service_timeout: float = Field(
        default=30.0, description="Timeout (seconds) for remote quotation services"
    )
    quotation_service_max_batch_size: int = Field(
        default=128,
        description="Maximum documents sent per request to the remote quotation service",
    )

    # CORS Configuration
    cors_allow_origin_regex: str = Field(
        default=r"http://(localhost|127\.0\.0\.1)(:\d+)?",
        description="Regex for allowed origins (dynamic localhost/127.0.0.1 with any port)",
    )
    cors_allow_credentials: bool = Field(
        default=True, description="CORS allow credentials"
    )

    # Authentication Configuration
    multi_user: bool = Field(default=False, description="Multi-user mode enabled")

    # Single user configuration (when multi_user=False)
    single_user_id: str = Field(default="root", description="Single user ID")
    single_user_name: str = Field(default="Root User", description="Single user name")
    single_user_email: str = Field(
        default="root@localhost", description="Single user email"
    )

    # Google OAuth Configuration (when multi_user=True)
    google_client_id: str = Field(default="", description="Google OAuth client ID")

    # CILogon OIDC Configuration (when multi_user=True)
    cilogon_client_id: str = Field(default="", description="CILogon OIDC client ID")
    cilogon_client_secret: str = Field(
        default="", description="CILogon OIDC client secret"
    )
    cilogon_discovery_url: str = Field(
        default="https://test.cilogon.aaf.edu.au/.well-known/openid-configuration",
        description="CILogon OIDC discovery document URL",
    )
    cilogon_redirect_uri: str = Field(
        default="",
        description=(
            "CILogon callback URL registered with the provider. "
            "Set to the full URL of /api/auth/cilogon/callback on your deployment."
        ),
    )

    # Security Configuration
    token_expire_hours: int = Field(default=24, description="Token expiration hours")
    secret_key: str = Field(
        default_factory=lambda: token_urlsafe(32),
        description=(
            "Secret key for JWT tokens (set SECRET_KEY in environment for stable deployments)"
        ),
    )
    admin_emails: str = Field(
        default="",
        description=(
            "Comma-separated admin email allowlist for admin endpoints in multi-user mode"
        ),
    )

    # LDaCA Data Portal / Oni API configuration
    ldaca_oni_api_base_url: str = Field(
        default="https://data.ldaca.edu.au/api",
        description="Base URL for the LDaCA Data Portal Oni API",
    )
    ldaca_oni_api_token: str | None = Field(
        default=None,
        description="Optional bearer token for LDaCA Oni API requests",
    )
    ldaca_oni_timeout: float = Field(
        default=30.0,
        description="Timeout (seconds) for LDaCA Oni API requests",
    )
    ldaca_oni_default_limit: int = Field(
        default=25,
        description="Default result limit for LDaCA Oni searches",
    )
    ldaca_oni_download_concurrency: int = Field(
        default=8,
        ge=1,
        le=32,
        description="Concurrent text-file downloads for LDaCA Oni imports",
    )
    ldaca_oni_featured_collection_ids: str = Field(
        default="arcp://name,hdl10.26180~23961609",
        description="Featured LDaCA collection crate ids as JSON, semicolon, or newline separated values",
    )

    model_config = SettingsConfigDict(
        case_sensitive=False,
        extra="ignore",
        env_prefix="",
        env_ignore_empty=True,
    )

    def get_data_root(self) -> Path:
        """Return configured data root path.

        Used by:
        - startup initialization, file utilities, DB URL derivation

        Why:
        - Centralizes conversion from env-config value to `Path` object.
        """
        return Path(self.data_root)

    def get_user_data_folder(self) -> Path:
        """Return user data base folder path under data root.

        Used by:
        - user/file/workspace folder helpers

        Why:
        - Keeps all user-owned storage rooted under one configurable path.
        """
        return self.get_data_root() / self.user_data_folder

    def get_sample_data_folder(self) -> Path | None:
        """Return optional sample-data override path.

        Used by:
        - sample-data import/setup utilities

        Why:
        - Supports external dataset bundles without code changes.
        """
        if not self.sample_data:
            return None
        return Path(self.sample_data)

    def get_database_backup_folder(self) -> Path:
        """Return database backup folder path under data root.

        Used by:
        - backup and maintenance tooling

        Why:
        - Keeps backup location configurable and co-located with runtime data.
        """
        return self.get_data_root() / self.database_backup_folder

    def get_database_url(self) -> str:
        """Return effective database URL, deriving SQLite path when omitted.

        Used by:
        - `db.py` engine initialization

        Why:
        - Allows simple local setup while supporting explicit DB URLs in deploys.

        Refactor note:
        - `secret_key` default value is placeholder-grade; enforce env-provided
          secret in production startup validation to reduce misconfiguration risk.
        """
        if self.database_url and self.database_url.strip():
            return self.database_url
        # Construct a sqlite URL under DATA_ROOT/database_file
        db_path = self.get_data_root() / self.database_file
        return f"sqlite+aiosqlite:///{db_path}"

    def get_admin_emails(self) -> set[str]:
        """Return normalized admin email allowlist from settings."""
        if not self.admin_emails.strip():
            return set()
        return {
            email.strip().lower()
            for email in self.admin_emails.split(",")
            if email.strip()
        }

    def get_ldaca_oni_featured_collection_ids(self) -> list[str]:
        """Return normalized staff-picked LDaCA collection ids."""
        raw_collection_ids = self.ldaca_oni_featured_collection_ids.strip()
        if not raw_collection_ids:
            return []

        if raw_collection_ids.startswith("["):
            parsed_collection_ids = json.loads(raw_collection_ids)
            return [
                str(collection_id).strip()
                for collection_id in parsed_collection_ids
                if str(collection_id).strip()
            ]

        return [
            collection_id.strip()
            for collection_id in re.split(r"[;\n]+", raw_collection_ids)
            if collection_id.strip()
        ]


# Global settings instance
settings = Settings()


def reload_settings() -> Settings:
    """Refresh the global settings singleton from current env vars.

    Re-runs ``Settings.__init__`` on the existing instance so any module that
    previously did ``from .settings import settings`` keeps the same reference
    and transparently sees the updated values. This matters because the
    package ``__init__`` imports ``main`` eagerly, which instantiates settings
    before the CLI has a chance to set env vars like ``MULTI_USER``.
    """
    settings.__init__()
    return settings
