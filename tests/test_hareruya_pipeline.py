from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from app.extract.hareruya_extractor import HareruyaExtractor
from app.load.hareruya_loader import HareruyaLoader
from app.services.hareruya_service import HareruyaService


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


class _UnexpectedHareruyaFetchExtractor:
    def fetch_html(self, *args, **kwargs):
        raise AssertionError("hareruya html should have been read from the local cache")

    def fetch_products_json(self, *args, **kwargs):
        raise AssertionError("hareruya json should have been read from the local cache")


class _FakeHareruyaTransformer:
    def transform_products(self, payload, context):
        return [
            {
                "product_id": "p-1",
                "collection_id": context["collection_id"],
                "set_code": "M2",
                "card_number": "001",
                "card_name_jp": "Sample JP",
                "card_name_en": "Oddish",
                "variant_title": "Near Mint",
                "currency": "JPY",
                "price_jpy": 30.0,
                "compare_at_price_jpy": 50.0,
                "product_url": "https://www.hareruya2.com/products/example",
            }
        ]


class _NoopHareruyaStagingLoader:
    def write_hareruya_record(self, *args, **kwargs) -> None:
        return None


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

    def test_historical_replay_requires_cached_collection_files(self) -> None:
        service = HareruyaService(
            extractor=_UnexpectedHareruyaFetchExtractor(),
            transformer=_FakeHareruyaTransformer(),
            loader=_NoopHareruyaStagingLoader(),
            db_loader=None,
            raw_base_dir=Path(tempfile.mkdtemp()),
        )

        with self.assertRaises(FileNotFoundError):
            service.run_one_collection(
                collection_url="https://www.hareruya2.com/collections/706",
                extract_date="2000-01-01",
            )

    def test_historical_replay_writes_history_without_overwriting_current(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            raw_base_dir = root / "raw"
            db_path = root / "ptcg.sqlite"

            html_path = raw_base_dir / "2000" / "01" / "01" / "collection_706_page.html"
            json_path = raw_base_dir / "2000" / "01" / "01" / "collection_706_products.json"
            html_path.parent.mkdir(parents=True, exist_ok=True)
            html_path.write_text("<html></html>", encoding="utf-8")
            json_path.write_text(json.dumps({"products": []}), encoding="utf-8")

            db_loader = HareruyaLoader(db_path=db_path)

            with closing(sqlite3.connect(db_path)) as conn:
                conn.execute(
                    """
                    CREATE TABLE prices_hareruya_current (
                      product_id TEXT PRIMARY KEY,
                      collection_id TEXT,
                      set_code TEXT,
                      card_number TEXT,
                      card_name_jp TEXT,
                      card_name_en TEXT,
                      variant_title TEXT,
                      currency TEXT NOT NULL DEFAULT 'JPY',
                      price_jpy REAL,
                      compare_at_price_jpy REAL,
                      product_url TEXT,
                      observed_at TEXT NOT NULL,
                      observed_date TEXT NOT NULL,
                      created_at TEXT NOT NULL,
                      updated_at TEXT NOT NULL
                    )
                    """
                )
                conn.execute(
                    """
                    INSERT INTO prices_hareruya_current (
                      product_id, collection_id, set_code, card_number,
                      card_name_jp, card_name_en, variant_title,
                      currency, price_jpy, compare_at_price_jpy,
                      product_url, observed_at, observed_date, created_at, updated_at
                    )
                    VALUES (
                      'p-1', '706', 'M2', '001', 'Current JP', 'Current EN', 'Near Mint',
                      'JPY', 999.0, 1200.0, 'https://www.hareruya2.com/products/example',
                      '2026-04-26T00:00:00+00:00', '2026-04-26',
                      '2026-04-26T00:00:00+00:00', '2026-04-26T00:00:00+00:00'
                    )
                    """
                )
                conn.commit()

            service = HareruyaService(
                extractor=_UnexpectedHareruyaFetchExtractor(),
                transformer=_FakeHareruyaTransformer(),
                loader=_NoopHareruyaStagingLoader(),
                db_loader=db_loader,
                raw_base_dir=raw_base_dir,
            )

            service.run_one_collection(
                collection_url="https://www.hareruya2.com/collections/706",
                extract_date="2000-01-01",
            )

            with closing(sqlite3.connect(db_path)) as conn:
                current_row = conn.execute(
                    """
                    SELECT price_jpy, observed_date
                    FROM prices_hareruya_current
                    WHERE product_id = 'p-1'
                    """
                ).fetchone()
                history_row = conn.execute(
                    """
                    SELECT price_jpy, observed_date
                    FROM prices_hareruya_history
                    WHERE product_id = 'p-1'
                    """
                ).fetchone()

            self.assertEqual(current_row, (999.0, "2026-04-26"))
            self.assertEqual(history_row, (30.0, "2000-01-01"))


if __name__ == "__main__":
    unittest.main()
