from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from app.extract.hareruya_extractor import HareruyaExtractor


FIXTURES_DIR = Path(__file__).parent / "fixtures" / "hareruya"


class _FakeResponse:
    def __init__(self, payload: dict, url: str) -> None:
        self._payload = payload
        self.url = url

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


class _FakeSession:
    def __init__(self, page_payloads: dict[int, dict]) -> None:
        self._page_payloads = page_payloads

    def get(self, url: str, timeout: int = 30) -> _FakeResponse:
        parsed = urlparse(url)
        page = int(parse_qs(parsed.query).get("page", ["1"])[0])
        payload = self._page_payloads.get(page, {"products": []})
        return _FakeResponse(payload=payload, url=url)


class HareruyaPipelineTests(unittest.TestCase):
    def test_fetch_products_json_combines_all_pages(self) -> None:
        page_payloads = {
            1: json.loads((FIXTURES_DIR / "products_page_1.json").read_text(encoding="utf-8")),
            2: json.loads((FIXTURES_DIR / "products_page_2.json").read_text(encoding="utf-8")),
            3: json.loads((FIXTURES_DIR / "products_page_3.json").read_text(encoding="utf-8")),
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            extractor = HareruyaExtractor(html_dir=tmpdir)
            extractor.session = _FakeSession(page_payloads)

            payload, context = extractor.fetch_products_json(
                collection_url="https://www.hareruya2.com/collections/706",
                filename="collection_706_products",
                page_limit=2,
            )

            saved_path = Path(tmpdir) / "collection_706_products.json"
            saved_payload = json.loads(saved_path.read_text(encoding="utf-8"))

        self.assertEqual([product["id"] for product in payload["products"]], [101, 102, 103, 104, 105])
        self.assertEqual(saved_payload["products"], payload["products"])
        self.assertEqual(context["collection_id"], "706")
        self.assertEqual(context["page_count"], "3")
        self.assertEqual(context["product_count"], "5")


if __name__ == "__main__":
    unittest.main()
