from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from app.core.config import settings


class GmailClient:
    def __init__(self) -> None:
        self.secret_file = Path(settings.gmail_client_secret_file)
        self.token_store = Path(settings.gmail_token_store)
        if not self.secret_file.exists():
            raise FileNotFoundError(f"Client secret file missing: {self.secret_file}")
        if not self.token_store.exists():
            raise FileNotFoundError(f"Token store missing: {self.token_store}")
        self._tokens = json.loads(self.token_store.read_text())
        self._services: Dict[str, Any] = {}

    def _credentials_for(self, account: str) -> Credentials:
        token_bundle = self._tokens.get(account)
        if not token_bundle:
            raise ValueError(f"No Gmail token for {account}")
        creds = Credentials(
            token=token_bundle.get("token"),
            refresh_token=token_bundle.get("refresh_token"),
            token_uri=token_bundle.get("token_uri"),
            client_id=token_bundle.get("client_id"),
            client_secret=token_bundle.get("client_secret"),
            scopes=settings.gmail_scopes,
        )
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            self._tokens[account]["token"] = creds.token
            self._tokens[account]["refresh_token"] = creds.refresh_token
            self.token_store.write_text(json.dumps(self._tokens, indent=2))
        return creds

    def _service(self, account: str):
        if account not in self._services:
            creds = self._credentials_for(account)
            self._services[account] = build("gmail", "v1", credentials=creds, cache_discovery=False)
        return self._services[account]

    def list_messages(
        self,
        account: str,
        query: Optional[str] = None,
        page_token: Optional[str] = None,
        max_results: int = 50,
    ) -> Dict[str, Any]:
        service = self._service(account)
        return (
            service.users()
            .messages()
            .list(userId="me", q=query, pageToken=page_token, maxResults=max_results)
            .execute()
        )

    def get_message(self, account: str, message_id: str, fmt: str = "full") -> Dict[str, Any]:
        service = self._service(account)
        return service.users().messages().get(userId="me", id=message_id, format=fmt).execute()
