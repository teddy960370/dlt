"""OAuth2 Token Introspection auth dependency for FastAPI.

Validates the incoming Bearer access token against the IdentityServer
introspection endpoint (RFC 7662), authenticating as this API's introspection
client using the configured client_id / client_secret.

Uses FastAPI's HTTPBearer security scheme so Swagger UI shows the "Authorize"
button and injects the `Authorization: Bearer <token>` header correctly.
"""
from __future__ import annotations

import threading
from typing import Optional

import httpx
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from api.config import OAuthConfig, load_oauth_config

# auto_error=False so we can return 401 (not 403) when the header is missing,
# and still get the Authorize button + Bearer parsing from Swagger UI.
bearer_scheme = HTTPBearer(
    auto_error=False,
    description="access token（不需加 'Bearer ' 前綴）",
)

_introspection_endpoint: Optional[str] = None
_lock = threading.Lock()


def _discover_introspection_endpoint(cfg: OAuthConfig) -> str:
    """Fetch (and cache) the introspection endpoint from OIDC discovery."""
    global _introspection_endpoint
    with _lock:
        if _introspection_endpoint:
            return _introspection_endpoint
        url = f"{cfg.identity_server_host.rstrip('/')}/.well-known/openid-configuration"
        resp = httpx.get(url, timeout=cfg.timeout_seconds)
        resp.raise_for_status()
        endpoint = resp.json().get("introspection_endpoint")
        if not endpoint:
            raise RuntimeError("discovery document has no introspection_endpoint")
        _introspection_endpoint = endpoint
        return endpoint


def require_auth(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
) -> dict:
    """FastAPI dependency: raise unless the Bearer token is active (and in scope)."""
    if credentials is None or not credentials.credentials:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "missing bearer token")
    token = credentials.credentials

    cfg = load_oauth_config()
    try:
        endpoint = _discover_introspection_endpoint(cfg)
        resp = httpx.post(
            endpoint,
            data={"token": token, "token_type_hint": "access_token"},
            auth=(cfg.client_id, cfg.client_secret),
            timeout=cfg.timeout_seconds,
        )
    except httpx.HTTPError as e:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, f"identity server unreachable: {e}")

    if resp.status_code != 200:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "token introspection failed")

    claims = resp.json()
    if not claims.get("active"):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "token is not active")

    if cfg.required_scope:
        scopes = (claims.get("scope") or "").split()
        if cfg.required_scope not in scopes:
            raise HTTPException(status.HTTP_403_FORBIDDEN, "insufficient scope")

    return claims
