from __future__ import annotations

import json
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from app.jobs.ebay_batch_job import EbayBatchJob
from app.load.ebay_loader import EbayLoader
from app.load.ebay_staging_loader import EbayStagingLoader
from app.load.limitless_loader import LimitlessLoader
from app.services.ebay_price_service import EbayPriceService
from app.transform.ebay_transformer import EbayTransformer
from app.utils.extract_policy import today_utc_date
from tests.postgres_test_utils import connect_schema, temporary_schema


class _UnexpectedFetchExtractor:
    marketplace_id = "EBAY_GB"

    def build_search_url(self, keyword: str, limit: int = 50) -> str:
        return f"https://api.ebay.com/buy/browse/v1/item_summary/search?q={keyword}&limit={limit}"

    def fetch_search_payload(self, *args, **kwargs):
        raise AssertionError("search payload should have been read from the local cache")


class EbayPipelineTests(unittest.TestCase):
    def test_job_load_pending_cards_returns_all_eligible_english_rows_from_prices_limitless(self) -> None:
        with temporary_schema() as schema_name:
            LimitlessLoader(schema_name=schema_name).ensure_prices_limitless_schema()
            with closing(connect_schema(schema_name)) as conn:
                with conn.cursor() as cur:
                    cur.executemany(
                        """
                        INSERT INTO prices_limitless (
                          card_id, lang, set_code, card_code, card_name, ebay_price,
                          observed_at, observed_date, created_at, updated_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, NULL, NULL, NULL, NULL)
                        """,
                        [
                            (1, "en", "BLK", "2", "English Card", None),
                            (2, "jp", "BLK", "3", "Japanese Card", None),
                            (3, "en", "BLK", "4", "Already Filled", 1.5),
                            (4, "en", "BLK", "5", "", None),
                        ],
                    )
                conn.commit()

            job = EbayBatchJob(
                client_id="dummy",
                client_secret="dummy",
                sandbox=False,
                schema_name=schema_name,
            )

            rows = job.load_pending_cards(extract_date="2026-04-25")

            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["card_id"], 1)
            self.assertEqual(rows[0]["lang"], "en")
            self.assertEqual(rows[1]["card_id"], 3)

    def test_job_load_pending_cards_ignores_existing_ebay_rows_when_cards_index_exists(self) -> None:
        with temporary_schema() as schema_name:
            LimitlessLoader(schema_name=schema_name).ensure_cards_index_table()
            EbayLoader(schema_name=schema_name).ensure_ebay_columns()
            with closing(connect_schema(schema_name)) as conn:
                with conn.cursor() as cur:
                    cur.executemany(
                        """
                        INSERT INTO cards_index (card_id, lang, set_code, card_code, card_name, rarity)
                        VALUES (%s, %s, %s, %s, %s, NULL)
                        """,
                        [
                            (1, "en", "BLK", "2", "Needs Search"),
                            (2, "en", "BLK", "3", "Already Priced"),
                            (3, "jp", "BLK", "4", "Japanese Card"),
                        ],
                    )
                    cur.execute(
                        """
                        INSERT INTO prices_ebay_current (
                          card_id, lang, set_code, card_code, card_name,
                          marketplace_id, currency, ebay_price,
                          observed_at, observed_date, created_at, updated_at
                        )
                        VALUES (
                          2, 'en', 'BLK', '3', 'Already Priced',
                          'EBAY_GB', 'GBP', 2.30,
                          '2026-03-29T00:00:00+00:00', '2026-03-29',
                          '2026-03-29T00:00:00+00:00', '2026-03-29T00:00:00+00:00'
                        )
                        """
                    )
                conn.commit()

            job = EbayBatchJob(
                client_id="dummy",
                client_secret="dummy",
                sandbox=False,
                schema_name=schema_name,
            )

            rows = job.load_pending_cards(extract_date="2026-04-25")

            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["card_id"], 1)
            self.assertEqual(rows[0]["card_name"], "Needs Search")
            self.assertEqual(rows[1]["card_id"], 2)
            self.assertEqual(rows[1]["card_name"], "Already Priced")

    def test_loader_keeps_one_history_row_per_card_and_extract_date(self) -> None:
        with temporary_schema() as schema_name:
            LimitlessLoader(schema_name=schema_name).ensure_prices_limitless_schema()
            with closing(connect_schema(schema_name)) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO prices_limitless (
                          card_id, lang, set_code, card_code, card_name, ebay_price,
                          observed_at, observed_date, created_at, updated_at
                        )
                        VALUES (1, 'en', 'BLK', '2', 'Refresh Me', NULL, NULL, NULL, NULL, NULL)
                        """
                    )
                conn.commit()

            loader = EbayLoader(schema_name=schema_name)
            loader.update_ebay_price(
                lang="en",
                set_code="BLK",
                card_code="2",
                ebay_price=2.3,
                card_id=1,
                card_name="Refresh Me",
                currency="GBP",
                marketplace_id="EBAY_GB",
                observed_date="2026-04-25",
            )
            loader.update_ebay_price(
                lang="en",
                set_code="BLK",
                card_code="2",
                ebay_price=2.6,
                card_id=1,
                card_name="Refresh Me",
                currency="GBP",
                marketplace_id="EBAY_GB",
                observed_date="2026-04-25",
            )

            with closing(connect_schema(schema_name)) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT COUNT(*)
                        FROM prices_ebay_history
                        WHERE lang = 'en'
                          AND set_code = 'BLK'
                          AND card_code = '2'
                          AND marketplace_id = 'EBAY_GB'
                          AND currency = 'GBP'
                          AND ebay_observed_date = '2026-04-25'
                        """
                    )
                    history_count = cur.fetchone()[0]
                    cur.execute(
                        """
                        SELECT ebay_price, ebay_observed_date
                        FROM prices_ebay_history
                        WHERE lang = 'en'
                          AND set_code = 'BLK'
                          AND card_code = '2'
                        """
                    )
                    history_row = cur.fetchone()
                    cur.execute(
                        """
                        SELECT ebay_price, observed_date
                        FROM prices_ebay_current
                        WHERE lang = 'en'
                          AND set_code = 'BLK'
                          AND card_code = '2'
                        """
                    )
                    current_row = cur.fetchone()

            self.assertEqual(history_count, 1)
            self.assertEqual(history_row, (2.6, "2026-04-25"))
            self.assertEqual(current_row, (2.6, "2026-04-25"))

    def test_service_historical_replay_requires_cached_json(self) -> None:
        with temporary_schema() as schema_name:
            service = EbayPriceService(
                extractor=_UnexpectedFetchExtractor(),
                transformer=EbayTransformer(),
                loader=EbayLoader(schema_name=schema_name),
                staging_loader=EbayStagingLoader(data_root=Path(tempfile.mkdtemp()) / "Data"),
                raw_json_base_dir=Path(tempfile.mkdtemp()) / "raw",
            )

            with self.assertRaises(FileNotFoundError):
                service.run_one(
                    keyword="Sample Card BLK 002",
                    lang="en",
                    set_code="BLK",
                    card_code="2",
                    card_id=124,
                    card_name="Sample Card",
                    extract_date="2000-01-01",
                )

    def test_service_historical_replay_does_not_overwrite_current(self) -> None:
        payload = {
            "itemSummaries": [
                {
                    "itemId": "2",
                    "title": "Sample Card B",
                    "price": {"value": "1.80", "currency": "GBP"},
                    "shippingOptions": [
                        {"shippingCost": {"value": "0.50", "currency": "GBP"}}
                    ],
                    "itemWebUrl": "https://example.com/b",
                    "condition": "Used",
                }
            ]
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            raw_base_dir = root / "raw"
            staging_root = root / "Data"

            json_path = raw_base_dir / "2000" / "01" / "01" / "en_BLK_2.json"
            json_path.parent.mkdir(parents=True, exist_ok=True)
            json_path.write_text(json.dumps(payload), encoding="utf-8")

            with temporary_schema() as schema_name:
                loader = EbayLoader(schema_name=schema_name)
                loader.ensure_ebay_columns()

                with closing(connect_schema(schema_name)) as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO prices_limitless (
                              card_id, lang, set_code, card_code, card_name, ebay_price
                            )
                            VALUES (124, 'en', 'BLK', '2', 'Sample Card', 9.9)
                            """
                        )
                        cur.execute(
                            """
                            INSERT INTO prices_ebay_current (
                              card_id, lang, set_code, card_code, card_name,
                              marketplace_id, currency, condition,
                              selected_item_id, selected_title, selected_item_web_url,
                              ebay_price, observed_at, observed_date, created_at, updated_at
                            )
                            VALUES (
                              124, 'en', 'BLK', '2', 'Sample Card',
                              'EBAY_GB', 'GBP', 'Used',
                              'existing', 'Existing Listing', 'https://example.com/existing',
                              9.9, '2026-04-26T00:00:00+00:00', '2026-04-26',
                              '2026-04-26T00:00:00+00:00', '2026-04-26T00:00:00+00:00'
                            )
                            """
                        )
                    conn.commit()

                service = EbayPriceService(
                    extractor=_UnexpectedFetchExtractor(),
                    transformer=EbayTransformer(),
                    loader=loader,
                    staging_loader=EbayStagingLoader(data_root=staging_root),
                    raw_json_base_dir=raw_base_dir,
                )

                record = service.run_one(
                    keyword="Sample Card BLK 002",
                    lang="en",
                    set_code="BLK",
                    card_code="2",
                    card_id=124,
                    card_name="Sample Card",
                    extract_date="2000-01-01",
                )

                self.assertEqual(record["selected_total_price"], 2.3)

                with closing(connect_schema(schema_name)) as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            SELECT ebay_price
                            FROM prices_limitless
                            WHERE lang = 'en' AND set_code = 'BLK' AND card_code = '2'
                            """
                        )
                        limitless_price = cur.fetchone()[0]
                        cur.execute(
                            """
                            SELECT ebay_price, observed_date
                            FROM prices_ebay_current
                            WHERE lang = 'en' AND set_code = 'BLK' AND card_code = '2'
                            """
                        )
                        current_row = cur.fetchone()
                        cur.execute(
                            """
                            SELECT ebay_price, ebay_observed_date
                            FROM prices_ebay_history
                            WHERE lang = 'en' AND set_code = 'BLK' AND card_code = '2'
                            """
                        )
                        history_row = cur.fetchone()

                self.assertEqual(limitless_price, 9.9)
                self.assertEqual(current_row, (9.9, "2026-04-26"))
                self.assertEqual(history_row, (2.3, "2000-01-01"))

    def test_transform_search_results_from_payload(self) -> None:
        payload = {
            "itemSummaries": [
                {
                    "itemId": "1",
                    "title": "Sample Card A",
                    "price": {"value": "2.50", "currency": "GBP"},
                    "shippingOptions": [
                        {"shippingCost": {"value": "1.00", "currency": "GBP"}}
                    ],
                    "itemWebUrl": "https://example.com/a",
                    "condition": "New",
                },
                {
                    "itemId": "2",
                    "title": "Sample Card B",
                    "price": {"value": "1.80", "currency": "GBP"},
                    "shippingOptions": [
                        {"shippingCost": {"value": "0.50", "currency": "GBP"}}
                    ],
                    "itemWebUrl": "https://example.com/b",
                    "condition": "Used",
                },
            ]
        }

        record = EbayTransformer().transform_search_results(
            payload,
            {
                "card_id": 124,
                "lang": "en",
                "set_code": "BLK",
                "card_code": "2",
                "card_name": "Sample Card",
                "search_keyword": "Sample Card BLK 002",
                "marketplace_id": "EBAY_GB",
                "search_limit": 50,
                "source_url": "https://api.ebay.com/buy/browse/v1/item_summary/search?q=Sample+Card+BLK+002&limit=50",
                "final_url": "https://api.ebay.com/buy/browse/v1/item_summary/search?q=Sample+Card+BLK+002&limit=50",
                "raw_json_path": "Data/raw/ebay/search_json/2026/03/29/en_BLK_2.json",
            },
        )

        self.assertEqual(record["raw_item_count"], 2)
        self.assertEqual(record["normalized_item_count"], 2)
        self.assertEqual(record["selected_item_id"], "2")
        self.assertEqual(record["selected_title"], "Sample Card B")
        self.assertEqual(record["selected_total_price"], 2.3)
        self.assertEqual(record["currency"], "GBP")

    def test_service_reuses_local_json_and_writes_staging(self) -> None:
        payload = {
            "itemSummaries": [
                {
                    "itemId": "2",
                    "title": "Sample Card B",
                    "price": {"value": "1.80", "currency": "GBP"},
                    "shippingOptions": [
                        {"shippingCost": {"value": "0.50", "currency": "GBP"}}
                    ],
                    "itemWebUrl": "https://example.com/b",
                    "condition": "Used",
                }
            ]
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            raw_base_dir = root / "raw"
            staging_root = root / "Data"
            extract_date = today_utc_date()

            year, month, day = extract_date.split("-")
            json_path = raw_base_dir / year / month / day / "en_BLK_2.json"
            json_path.parent.mkdir(parents=True, exist_ok=True)
            json_path.write_text(json.dumps(payload), encoding="utf-8")

            with temporary_schema() as schema_name:
                LimitlessLoader(schema_name=schema_name).ensure_prices_limitless_schema()
                with closing(connect_schema(schema_name)) as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO prices_limitless (
                              card_id, lang, set_code, card_code, card_name, ebay_price,
                              observed_at, observed_date, created_at, updated_at
                            )
                            VALUES (124, 'en', 'BLK', '2', 'Sample Card', NULL, NULL, NULL, NULL, NULL)
                            """
                        )
                    conn.commit()

                service = EbayPriceService(
                    extractor=_UnexpectedFetchExtractor(),
                    transformer=EbayTransformer(),
                    loader=EbayLoader(schema_name=schema_name),
                    staging_loader=EbayStagingLoader(data_root=staging_root),
                    raw_json_base_dir=raw_base_dir,
                )

                record = service.run_one(
                    keyword="Sample Card BLK 002",
                    lang="en",
                    set_code="BLK",
                    card_code="2",
                    card_id=124,
                    card_name="Sample Card",
                    extract_date=extract_date,
                )

                self.assertEqual(record["selected_total_price"], 2.3)

                cards_normalized = (
                    staging_root
                    / "staging"
                    / "ebay"
                    / "cards_normalized"
                    / f"extract_date={extract_date}"
                    / "en_BLK_2.parquet"
                )
                card_index = staging_root / "staging" / "ebay" / "card_index" / "en_BLK_2.parquet"
                price_events = (
                    staging_root
                    / "staging"
                    / "ebay"
                    / "price_events"
                    / f"extract_date={extract_date}"
                    / "en_BLK_2.parquet"
                )

                self.assertTrue(cards_normalized.exists())
                self.assertTrue(card_index.exists())
                self.assertTrue(price_events.exists())

                with closing(connect_schema(schema_name)) as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            SELECT ebay_price
                            FROM prices_limitless
                            WHERE lang = 'en' AND set_code = 'BLK' AND card_code = '2'
                            """
                        )
                        ebay_price = cur.fetchone()[0]
                        cur.execute(
                            """
                            SELECT marketplace_id, currency, card_id, set_code, card_code, card_name, condition, ebay_price
                            FROM prices_ebay_current
                            """
                        )
                        current_row = cur.fetchone()
                        cur.execute(
                            """
                            SELECT marketplace_id, currency, card_id, set_code, card_code, card_name, condition, ebay_price, ebay_observed_date
                            FROM prices_ebay_history
                            """
                        )
                        history_row = cur.fetchone()

                self.assertEqual(ebay_price, 2.3)
                self.assertEqual(
                    current_row,
                    ("EBAY_GB", "GBP", 124, "BLK", "2", "Sample Card", "Used", 2.3),
                )
                self.assertEqual(
                    history_row,
                    ("EBAY_GB", "GBP", 124, "BLK", "2", "Sample Card", "Used", 2.3, extract_date),
                )


if __name__ == "__main__":
    unittest.main()
