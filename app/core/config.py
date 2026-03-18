from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict

# Resolve .env relative to the project root (workspace), not CWD
_ENV_FILE = Path(__file__).resolve().parents[3] / ".env"
load_dotenv(_ENV_FILE, override=True)


class Settings(BaseSettings):
    environment: str = "local"
    database_url: str = "postgresql+psycopg://kanen:kanen@localhost:5432/kanen"

    quickbooks_client_id: Optional[str] = None
    quickbooks_client_secret: Optional[str] = None
    quickbooks_refresh_token: Optional[str] = None
    quickbooks_realm_id: Optional[str] = None
    quickbooks_environment: str = "sandbox"  # or "production"

    gmail_client_secret_file: str = "../secrets/google-gmail-desktop.oauth.json"
    gmail_token_store: str = "../secrets/gmail-tokens.json"
    gmail_scopes: List[str] = ["https://www.googleapis.com/auth/gmail.readonly"]

    api_key_header: str = "X-API-Key"
    api_key: Optional[str] = None

    # Shopify (used by website redesign project, not backend directly)
    shopify_access_token: Optional[str] = None

    model_config = SettingsConfigDict(
        env_file=str(_ENV_FILE),
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
