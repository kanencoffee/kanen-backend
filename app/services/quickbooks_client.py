from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import httpx

from app.core.config import settings

TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
BASE_URLS = {
    "sandbox": "https://sandbox-quickbooks.api.intuit.com/v3/company/{realm_id}",
    "production": "https://quickbooks.api.intuit.com/v3/company/{realm_id}",
}


class QuickBooksClient:
    """Lightweight wrapper around the QuickBooks Online REST API."""

    def __init__(self) -> None:
        if not settings.quickbooks_realm_id:
            raise ValueError("QUICKBOOKS_REALM_ID is required")
        if not settings.quickbooks_client_id or not settings.quickbooks_client_secret:
            raise ValueError("QuickBooks client id/secret are required")
        if not settings.quickbooks_refresh_token:
            raise ValueError("QUICKBOOKS_REFRESH_TOKEN is required")

        self.realm_id = settings.quickbooks_realm_id
        self.base_url = BASE_URLS.get(settings.quickbooks_environment, BASE_URLS["sandbox"]).format(
            realm_id=self.realm_id
        )
        self.access_token: Optional[str] = None
        self.refresh_token: str = settings.quickbooks_refresh_token
        self.token_expires_at: datetime = datetime.utcnow()
        self.client = httpx.Client(timeout=httpx.Timeout(15.0, read=30.0))

    def refresh_access_token(self) -> None:
        response = self.client.post(
            TOKEN_URL,
            data={"grant_type": "refresh_token", "refresh_token": self.refresh_token},
            auth=(settings.quickbooks_client_id, settings.quickbooks_client_secret),
        )
        response.raise_for_status()
        payload = response.json()
        self.access_token = payload["access_token"]
        self.refresh_token = payload.get("refresh_token", self.refresh_token)
        expires_in = payload.get("expires_in", 3600)
        self.token_expires_at = datetime.utcnow() + timedelta(seconds=expires_in - 60)

    def _auth_headers(self) -> Dict[str, str]:
        if not self.access_token or datetime.utcnow() >= self.token_expires_at:
            self.refresh_access_token()
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def _get(self, resource: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.base_url}/{resource}"
        response = self.client.get(url, params=params or {}, headers=self._auth_headers())
        response.raise_for_status()
        return response.json()

    def fetch_vendors(self, start_position: int = 1, max_results: int = 100) -> Dict[str, Any]:
        return self._get(
            "query",
            params={
                "query": f"select * from Vendor startposition {start_position} maxresults {max_results}",
            },
        )

    def fetch_items(self, start_position: int = 1, max_results: int = 100) -> Dict[str, Any]:
        return self._get(
            "query",
            params={
                "query": f"select * from Item startposition {start_position} maxresults {max_results}",
            },
        )

    def fetch_invoices(self, changed_since: Optional[str] = None, start_position: int = 1) -> Dict[str, Any]:
        query = "select * from Invoice"
        if changed_since:
            query += f" where Metadata.LastUpdatedTime >= '{changed_since}'"
        query += f" startposition {start_position}"
        return self._get("query", params={"query": query})

    def fetch_purchase_orders(self, changed_since: Optional[str] = None, start_position: int = 1) -> Dict[str, Any]:
        query = "select * from PurchaseOrder"
        if changed_since:
            query += f" where Metadata.LastUpdatedTime >= '{changed_since}'"
        query += f" startposition {start_position}"
        return self._get("query", params={"query": query})
