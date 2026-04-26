from __future__ import annotations

import base64
import json
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from requests import RequestException

import requests

from app.data_paths import build_raw_day_dir, build_timestamped_name
from app.config import get_ebay_credentials, parse_bool_env


class EbayAuth:
    """
    Authentication layer for eBay.

    Responsibilities:
    - Request OAuth access token
    - Cache token in memory until expiry

    This class does NOT handle:
    - eBay search requests
    - Data transformation
    - Database writes
    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        sandbox: bool = False,
    ) -> None:
        """
        Initialize the authentication client.

        Args:
            client_id:
                eBay application client ID.
            client_secret:
                eBay application client secret.
            sandbox:
                Whether to use sandbox environment.
        """
        self.client_id = client_id.strip()
        self.client_secret = client_secret.strip()
        self.sandbox = sandbox

        self.access_token: str | None = None
        self.expires_at: float = 0.0

    @property
    def token_url(self) -> str:
        """
        Return the OAuth token endpoint.
        """
        if self.sandbox:
            return "https://api.sandbox.ebay.com/identity/v1/oauth2/token"
        return "https://api.ebay.com/identity/v1/oauth2/token"

    @staticmethod
    def _raise_network_error(url: str, exc: Exception) -> None:
        raise RuntimeError(
            "Network error while contacting the eBay OAuth endpoint "
            f"{url}. This usually means DNS or internet access is unavailable "
            "on this machine, not that your eBay credentials are wrong. "
            "Live runs for today's extract_date require network access; "
            "historical replays only work from cached raw files."
        ) from exc

    def get_access_token(
        self,
        scope: str = "https://api.ebay.com/oauth/api_scope",
    ) -> str:
        """
        Get a valid access token.

        Returns cached token if it is still valid.

        Args:
            scope:
                OAuth scope for the token request.

        Returns:
            A valid eBay access token.
        """
        now = time.time()

        if self.access_token and now < (self.expires_at - 60):
            return self.access_token

        raw = f"{self.client_id}:{self.client_secret}"
        encoded = base64.b64encode(raw.encode("utf-8")).decode("utf-8")

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {encoded}",
        }

        data = {
            "grant_type": "client_credentials",
            "scope": scope,
        }

        try:
            response = requests.post(
                self.token_url,
                headers=headers,
                data=data,
                timeout=30,
            )
        except RequestException as exc:
            self._raise_network_error(self.token_url, exc)
        response.raise_for_status()

        token_data = response.json()
        access_token = token_data.get("access_token")
        if access_token is None:
            raise ValueError("Access token not found in response")

        self.access_token = access_token
        expires_in = token_data.get("expires_in", 7200)
        self.expires_at = now + expires_in

        return access_token


class EbayExtractor:
    """
    Extract layer for eBay.

    Responsibilities:
    - Send search requests to eBay Browse API
    - Return raw item summary data

    This class does NOT handle:
    - OAuth token generation logic
    - Business filtering rules
    - Database writes
    """

    def __init__(
        self,
        auth: EbayAuth,
        sandbox: bool = False,
        json_dir: str | Path | None = None,
        timeout: int = 30,
        marketplace_id: str = "EBAY_GB",
    ) -> None:
        """
        Initialize the extractor.

        Args:
            auth:
                EbayAuth instance used to get access token.
            sandbox:
                Whether to use sandbox environment.
        """
        self.auth = auth
        self.sandbox = sandbox
        self.json_dir = Path(json_dir) if json_dir else build_raw_day_dir("ebay", "search_json")
        self.json_dir.mkdir(parents=True, exist_ok=True)
        self.timeout = timeout
        self.marketplace_id = marketplace_id

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
                "Content-Type": "application/json",
            }
        )

    @property
    def base_url(self) -> str:
        """
        Return the base API URL.
        """
        if self.sandbox:
            return "https://api.sandbox.ebay.com"
        return "https://api.ebay.com"

    def build_search_url(self, keyword: str, limit: int = 50) -> str:
        query = urlencode({"q": keyword, "limit": limit})
        return f"{self.base_url}/buy/browse/v1/item_summary/search?{query}"

    @staticmethod
    def _raise_network_error(url: str, exc: Exception) -> None:
        raise RuntimeError(
            "Network error while contacting the eBay Browse API "
            f"{url}. This usually means DNS or internet access is unavailable "
            "on this machine. If you are replaying a past extract_date, make "
            "sure the raw JSON for that day already exists locally."
        ) from exc

    def fetch_search_payload(
        self,
        keyword: str,
        limit: int = 50,
        filename: str | None = None,
        save_to: str | None = None,
        marketplace_id: str | None = None,
    ) -> tuple[dict[str, Any], dict[str, str | None]]:
        """
        Fetch raw eBay search response and optionally save it to JSON.

        Args:
            keyword:
                Search keyword.
            limit:
                Maximum number of items to request.
            filename:
                Optional filename without extension when saving to json_dir.
            save_to:
                Optional explicit output path.
            marketplace_id:
                Marketplace header value. Defaults to the extractor setting.

        Returns:
            tuple:
                - payload: raw JSON response
                - context: lightweight fetch metadata
        """
        token = self.auth.get_access_token()
        resolved_marketplace_id = marketplace_id or self.marketplace_id
        url = self.build_search_url(keyword=keyword, limit=limit)

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "X-EBAY-C-MARKETPLACE-ID": resolved_marketplace_id,
        }

        try:
            response = self.session.get(url, headers=headers, timeout=self.timeout)
        except RequestException as exc:
            self._raise_network_error(url, exc)
        response.raise_for_status()

        payload = response.json()

        if save_to:
            self.save_json(payload, save_to)
        elif filename:
            path = self.json_dir / f"{filename}.json"
            self.save_json(payload, str(path))
        else:
            path = self.json_dir / build_timestamped_name(prefix="search", ext="json")
            self.save_json(payload, str(path))

        context = {
            "search_keyword": keyword,
            "marketplace_id": resolved_marketplace_id,
            "search_limit": str(limit),
            "source_url": url,
            "final_url": response.url,
        }

        return payload, context

    def search_items(
        self,
        keyword: str,
        limit: int = 50,
        marketplace_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Search items from eBay by keyword.
        """
        payload, _ = self.fetch_search_payload(
            keyword=keyword,
            limit=limit,
            marketplace_id=marketplace_id,
        )

        raw_items = payload.get("itemSummaries")
        if not isinstance(raw_items, list):
            return []
        return [item for item in raw_items if isinstance(item, dict)]

    def save_json(self, payload: dict[str, Any], path: str) -> None:
        """
        Save raw JSON to a local file.
        """
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)


def main() -> None:
    """
    Simple standalone test for this module.
    """
    client_id, client_secret = get_ebay_credentials()
    print(f"Client ID: {client_id}")
    print(f"Client Secret: {client_secret}")
    sandbox = parse_bool_env("EBAY_SANDBOX", default=False)

    auth = EbayAuth(
        client_id=client_id,
        client_secret=client_secret,
        sandbox=sandbox,
    )
    extractor = EbayExtractor(auth=auth, sandbox=sandbox)

    payload, context = extractor.fetch_search_payload(
        keyword="pokemon pikachu",
        limit=5,
        filename="pokemon_pikachu",
    )
    items = payload.get("itemSummaries", [])
    print(f"Fetched {len(items)} items.")
    print(context)


if __name__ == "__main__":
    main()
