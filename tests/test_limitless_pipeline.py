from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from app.jobs.limitless_batch_job import build_set_html_path, discover_card_codes
from app.transform.limitless_transformer import LimitlessTransformer


FIXTURES_DIR = Path(__file__).parent / "fixtures" / "limitless"


class _UnexpectedFetchExtractor:
    def fetch_set_html(self, *args, **kwargs):
        raise AssertionError("set html should have been read from the local cache")


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


if __name__ == "__main__":
    unittest.main()
