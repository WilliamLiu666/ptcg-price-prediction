from __future__ import annotations

import base64
import time
from typing import Any

import requests

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

        response = requests.post(
            self.token_url,
            headers=headers,
            data=data,
            timeout=30,
        )
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

    def __init__(self, auth: EbayAuth, sandbox: bool = False) -> None:
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

    @property
    def base_url(self) -> str:
        """
        Return the base API URL.
        """
        if self.sandbox:
            return "https://api.sandbox.ebay.com"
        return "https://api.ebay.com"

    def search_items(self, keyword: str, limit: int = 50) -> list[dict[str, Any]]:
        """
        Search items from eBay by keyword.

        Args:
            keyword:
                Search keyword.
            limit:
                Maximum number of items to request.

        Returns:
            Raw eBay item summary list.
        """
        token = self.auth.get_access_token()

        url = f"{self.base_url}/buy/browse/v1/item_summary/search"

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "X-EBAY-C-MARKETPLACE-ID": "EBAY_GB",
        }

        params = {
            "q": keyword,
            "limit": limit,
        }

        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()
        return data.get("itemSummaries", [])


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

    items = extractor.search_items(keyword="pokemon pikachu", limit=5)
    print(f"Fetched {len(items)} items.")


if __name__ == "__main__":
    main()