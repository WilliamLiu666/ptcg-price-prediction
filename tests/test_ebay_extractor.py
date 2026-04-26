from __future__ import annotations

import unittest
from unittest.mock import patch

import requests

from app.extract.ebay_extractor import EbayAuth, EbayExtractor


class _UnusedAuth:
    def get_access_token(self, scope: str = "https://api.ebay.com/oauth/api_scope") -> str:
        return "token"


class _FailingSession:
    def get(self, *args, **kwargs):
        raise requests.exceptions.ConnectionError("dns failure")


class EbayExtractorErrorTests(unittest.TestCase):
    def test_auth_wraps_dns_errors_with_clear_message(self) -> None:
        auth = EbayAuth(
            client_id="client",
            client_secret="secret",
            sandbox=False,
        )

        with patch("app.extract.ebay_extractor.requests.post", side_effect=requests.exceptions.ConnectionError("dns failure")):
            with self.assertRaises(RuntimeError) as ctx:
                auth.get_access_token()

        self.assertIn("eBay OAuth endpoint", str(ctx.exception))
        self.assertIn("DNS or internet access", str(ctx.exception))

    def test_search_wraps_dns_errors_with_clear_message(self) -> None:
        extractor = EbayExtractor(
            auth=_UnusedAuth(),
            sandbox=False,
        )
        extractor.session = _FailingSession()

        with self.assertRaises(RuntimeError) as ctx:
            extractor.fetch_search_payload(keyword="Oddish", limit=5)

        self.assertIn("eBay Browse API", str(ctx.exception))
        self.assertIn("DNS or internet access", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
