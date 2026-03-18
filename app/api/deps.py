from fastapi import Depends, HTTPException, Security, status
from fastapi.security import APIKeyHeader

from app.core.config import settings

def get_api_key_scheme() -> APIKeyHeader:
    return APIKeyHeader(name=settings.api_key_header, auto_error=False)


def require_api_key(api_key: str | None = Security(get_api_key_scheme())) -> str | None:
    if settings.api_key is None:
        return None
    if not api_key or api_key != settings.api_key:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")
    return api_key
