"""API + OAuth configuration, read from environment (.env)."""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

# el.settings already loads .env at import time; import it so env is populated.
import el.settings  # noqa: F401


def _require(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise RuntimeError(f"Missing required environment variable: {key}")
    return val


@dataclass
class ApiConfig:
    host: str
    port: int


@dataclass
class OAuthConfig:
    identity_server_host: str      # authority base for discovery/introspection
    client_id: str                 # this API's introspection client id
    client_secret: str
    required_scope: Optional[str]   # optional scope the token must contain
    timeout_seconds: float


def load_api_config() -> ApiConfig:
    return ApiConfig(
        host=os.getenv("API_HOST", "0.0.0.0"),
        port=int(os.getenv("API_PORT", "8080")),
    )


def load_oauth_config() -> OAuthConfig:
    return OAuthConfig(
        identity_server_host=_require("IDENTITY_SERVER_HOST"),
        client_id=_require("IDENTITY_SERVER_CLIENT_ID"),
        client_secret=_require("IDENTITY_SERVER_CLIENT_SECRET"),
        required_scope=os.getenv("OAUTH_REQUIRED_SCOPE") or None,
        timeout_seconds=float(os.getenv("OAUTH_TIMEOUT_SECONDS", "5")),
    )
