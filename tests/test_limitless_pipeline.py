from __future__ import annotations

import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from app.jobs.limitless_batch_job import build_set_html_path, discover_card_codes
from app.load.limitless_loader import LimitlessLoader
from app.services.limitless_service import LimitlessService
from app.transform.limitless_transformer import LimitlessTransformer


FIXTURES_DIR = Path(__file__).parent / "fixtures" / "limitless"


class _UnexpectedFetchExtractor:
    def fetch_set_html(self, *args, **kwargs):
        raise AssertionError("set html should have been read from the local cache")

    def fetch_html(self, *args, **kwargs):
        raise AssertionError("card html should have been read from the local cache")


class _NoopLimitlessStagingLoader:
    def write_limitless_record(self, *args, **kwargs) -> None:
        return None


class LimitlessPipelineTests(unittest.TestCase):
    def test_transform_card_from_fixture(self) -> None:
        html = (FIXTURES_DIR / "card_page.html").read_text(encoding="utf-8")

        record = LimitlessTransformer().transform_card(
            html,
            {
                "lang": "en",
                "set_code": "BLK",
                "card_code": "2",
                "card_path": "/cards/en/BLK/2",
            },
        )

        self.assertEqual(record["card_id"], "124")
        self.assertEqual(record["data_id"], "456")
        self.assertEqual(record["card_name"], "Sample Card")
        self.assertEqual(record["rarity"], "Uncommon")
        self.assertEqual(record["usd_price"], 1.25)
        self.assertEqual(record["eur_price"], 1.10)

    def test_discover_card_codes_uses_set_listing_order_and_dedupes(self) -> None:
        fixture_html = (FIXTURES_DIR / "set_page.html").read_text(encoding="utf-8")

        with tempfile.TemporaryDirectory() as tmpdir:
            cache_path = build_set_html_path(
                raw_set_html_base_dir=tmpdir,
                extract_date="2026-03-29",
                lang="en",
                set_code="BLK",
            )
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(fixture_html, encoding="utf-8")

            card_codes = discover_card_codes(
                extractor=_UnexpectedFetchExtractor(),
                transformer=LimitlessTransformer(),
                lang="en",
                set_code="BLK",
                size=172,
                extract_date="2026-03-29",
                raw_set_html_base_dir=tmpdir,
            )

        self.assertEqual(card_codes, ["1", "3", "10"])

    def test_historical_replay_requires_cached_card_html(self) -> None:
        service = LimitlessService(
            extractor=_UnexpectedFetchExtractor(),
            transformer=LimitlessTransformer(),
            loader=_NoopLimitlessStagingLoader(),
            db_loader=None,
            raw_html_base_dir=Path(tempfile.mkdtemp()),
        )

        with self.assertRaises(FileNotFoundError):
            service.run_one(
                lang="en",
                set_code="BLK",
                card_code="2",
                extract_date="2000-01-01",
            )

    def test_historical_replay_writes_history_without_overwriting_current(self) -> None:
        html = (FIXTURES_DIR / "card_page.html").read_text(encoding="utf-8")

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            raw_base_dir = root / "raw"
            db_path = root / "ptcg.sqlite"

            html_path = raw_base_dir / "2000" / "01" / "01" / "en_BLK_2.html"
            html_path.parent.mkdir(parents=True, exist_ok=True)
            html_path.write_text(html, encoding="utf-8")

            db_loader = LimitlessLoader(db_path=db_path)
            db_loader.ensure_cards_index_table()
            db_loader.ensure_prices_limitless_schema()

            with closing(sqlite3.connect(db_path)) as conn:
                conn.execute(
                    """
                    INSERT INTO prices_limitless (
                      card_id, data_id, lang, set_code, card_code, card_name, rarity,
                      usd_price, eur_price, observed_at, observed_date, created_at, updated_at
                    )
                    VALUES (
                      124, 456, 'en', 'BLK', '2', 'Sample Card', 'Uncommon',
                      9.99, 8.88, '2026-04-26T00:00:00+00:00', '2026-04-26',
                      '2026-04-26T00:00:00+00:00', '2026-04-26T00:00:00+00:00'
                    )
                    """
                )
                conn.commit()

            service = LimitlessService(
                extractor=_UnexpectedFetchExtractor(),
                transformer=LimitlessTransformer(),
                loader=_NoopLimitlessStagingLoader(),
                db_loader=db_loader,
                raw_html_base_dir=raw_base_dir,
            )

            service.run_one(
                lang="en",
                set_code="BLK",
                card_code="2",
                extract_date="2000-01-01",
            )

            with closing(sqlite3.connect(db_path)) as conn:
                current_row = conn.execute(
                    """
                    SELECT usd_price, eur_price, observed_date
                    FROM prices_limitless
                    WHERE lang = 'en' AND set_code = 'BLK' AND card_code = '2'
                    """
                ).fetchone()
                history_row = conn.execute(
                    """
                    SELECT usd_price, eur_price, observed_date
                    FROM prices_limitless_history
                    WHERE lang = 'en' AND set_code = 'BLK' AND card_code = '2'
                    """
                ).fetchone()

            self.assertEqual(current_row, (9.99, 8.88, "2026-04-26"))
            self.assertEqual(history_row, (1.25, 1.1, "2000-01-01"))


if __name__ == "__main__":
    unittest.main()
