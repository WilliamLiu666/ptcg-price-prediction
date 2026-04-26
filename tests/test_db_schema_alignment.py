from __future__ import annotations

import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from app.load.cardrush_loader import CardrushLoader
from app.load.ebay_loader import EbayLoader
from app.load.hareruya_loader import HareruyaLoader
from app.load.limitless_loader import LimitlessLoader
from app.scripts.migrate_sqlite_to_postgres import run_migration
from tests.postgres_test_utils import (
    column_names,
    connect_schema,
    foreign_keys,
    index_names,
    table_exists,
    temporary_schema,
)


class DbSchemaAlignmentTests(unittest.TestCase):
    def test_limitless_loader_creates_limitless_current_and_history_tables(self) -> None:
        with temporary_schema() as schema_name:
            loader = LimitlessLoader(schema_name=schema_name)
            loader.ensure_prices_limitless_schema()

            with closing(connect_schema(schema_name)) as conn:
                current_columns = column_names(conn, "prices_limitless")
                history_columns = column_names(conn, "prices_limitless_history")

            self.assertEqual(
                current_columns,
                [
                    "card_id",
                    "data_id",
                    "lang",
                    "set_code",
                    "card_code",
                    "card_name",
                    "rarity",
                    "usd_price",
                    "eur_price",
                    "ebay_price",
                    "observed_at",
                    "observed_date",
                    "created_at",
                    "updated_at",
                    "ebay_observed_at",
                    "ebay_observed_date",
                ],
            )
            self.assertEqual(
                history_columns,
                [
                    "id",
                    "card_id",
                    "lang",
                    "set_code",
                    "card_code",
                    "usd_price",
                    "eur_price",
                    "ebay_price",
                    "source",
                    "observed_at",
                    "observed_date",
                ],
            )

    def test_ebay_loader_creates_ebay_current_and_history_tables(self) -> None:
        with temporary_schema() as schema_name:
            loader = EbayLoader(schema_name=schema_name)
            loader.ensure_ebay_columns()

            with closing(connect_schema(schema_name)) as conn:
                current_columns = column_names(conn, "prices_ebay_current")
                history_columns = column_names(conn, "prices_ebay_history")
                search_result_indexes = index_names(conn, "ebay_search_results")

            self.assertEqual(
                current_columns,
                [
                    "card_id",
                    "lang",
                    "set_code",
                    "card_code",
                    "card_name",
                    "marketplace_id",
                    "currency",
                    "condition",
                    "selected_item_id",
                    "selected_title",
                    "selected_item_web_url",
                    "ebay_price",
                    "observed_at",
                    "observed_date",
                    "created_at",
                    "updated_at",
                ],
            )
            self.assertEqual(
                history_columns,
                [
                    "id",
                    "card_id",
                    "lang",
                    "set_code",
                    "card_code",
                    "card_name",
                    "marketplace_id",
                    "currency",
                    "condition",
                    "selected_item_id",
                    "selected_title",
                    "selected_item_web_url",
                    "ebay_price",
                    "ebay_observed_at",
                    "ebay_observed_date",
                ],
            )
            self.assertIn("idx_ebay_search_keyword_observed_at", search_result_indexes)

    def test_limitless_loader_writes_limitless_current_and_history_rows(self) -> None:
        with temporary_schema() as schema_name:
            loader = LimitlessLoader(schema_name=schema_name)
            loader.save_card_price(
                {
                    "card_id": "124",
                    "data_id": "456",
                    "lang": "en",
                    "set_code": "BLK",
                    "card_code": "2",
                    "card_name": "Sample Card",
                    "rarity": "Uncommon",
                    "usd_price": 1.25,
                    "eur_price": 1.10,
                }
            )

            with closing(connect_schema(schema_name)) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT card_id, lang, set_code, card_code, card_name, usd_price, eur_price
                        FROM prices_limitless
                        """
                    )
                    current_row = cur.fetchone()
                    cur.execute(
                        """
                        SELECT card_id, lang, set_code, card_code, usd_price, eur_price, source
                        FROM prices_limitless_history
                        """
                    )
                    history_row = cur.fetchone()

            self.assertEqual(
                current_row,
                (124, "en", "BLK", "2", "Sample Card", 1.25, 1.1),
            )
            self.assertEqual(
                history_row,
                (124, "en", "BLK", "2", 1.25, 1.1, "limitless"),
            )

    def test_cardrush_loader_writes_cardrush_current_and_history_tables(self) -> None:
        with temporary_schema() as schema_name:
            loader = CardrushLoader(schema_name=schema_name)
            written = loader.save_products(
                product_group="268",
                items=[
                    {
                        "product_id": "123",
                        "product_url": "https://www.cardrush-pokemon.jp/product/123",
                        "name": "Card Name",
                        "name_full": "Full Card Name",
                        "condition": "A",
                        "model_number": "001",
                        "set_size": "100",
                        "model_code": "SVJP",
                        "price": "1,280",
                    }
                ],
                parse_price_func=lambda value: 1280.0,
            )

            self.assertEqual(written, 1)

            with closing(connect_schema(schema_name)) as conn:
                history_columns = column_names(conn, "prices_cardrush")
                current_columns = column_names(conn, "prices_cardrush_current")
                relation_foreign_keys = foreign_keys(conn, "prices_cardrush")
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT product_id, observed_date, price_yen, price_text, source
                        FROM prices_cardrush
                        """
                    )
                    history_row = cur.fetchone()
                    cur.execute(
                        """
                        SELECT product_id, price_yen, price_text, source
                        FROM prices_cardrush_current
                        """
                    )
                    current_row = cur.fetchone()

            self.assertEqual(
                history_columns,
                ["product_id", "observed_at", "observed_date", "price_yen", "price_text", "source"],
            )
            self.assertEqual(
                current_columns,
                ["product_id", "price_yen", "price_text", "observed_at", "observed_date", "source", "updated_at"],
            )
            self.assertEqual(
                relation_foreign_keys,
                [("product_id", "products_cardrush", "product_id")],
            )
            self.assertEqual(history_row[0], "123")
            self.assertEqual(history_row[2:], (1280, "1,280", "cardrush"))
            self.assertEqual(current_row, ("123", 1280, "1,280", "cardrush"))

    def test_hareruya_loader_writes_hareruya_current_and_history_tables(self) -> None:
        with temporary_schema() as schema_name:
            loader = HareruyaLoader(schema_name=schema_name)
            written = loader.save_product_prices(
                [
                    {
                        "product_id": "9921823932736",
                        "collection_id": "706",
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
            )

            self.assertEqual(written, 1)

            with closing(connect_schema(schema_name)) as conn:
                self.assertTrue(table_exists(conn, "prices_hareruya_current"))
                self.assertTrue(table_exists(conn, "prices_hareruya_history"))
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT product_id, set_code, card_number, card_name_en, price_jpy, currency
                        FROM prices_hareruya_current
                        """
                    )
                    current_row = cur.fetchone()
                    cur.execute(
                        """
                        SELECT product_id, set_code, card_number, card_name_en, price_jpy, currency
                        FROM prices_hareruya_history
                        """
                    )
                    history_row = cur.fetchone()

            self.assertEqual(
                current_row,
                ("9921823932736", "M2", "001", "Oddish", 30.0, "JPY"),
            )
            self.assertEqual(
                history_row,
                ("9921823932736", "M2", "001", "Oddish", 30.0, "JPY"),
            )

    def test_sqlite_migration_script_copies_rows_into_postgres(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "ptcg.sqlite"

            with closing(sqlite3.connect(sqlite_path)) as conn:
                conn.executescript(
                    """
                    CREATE TABLE series_limitless (
                      series_code TEXT NOT NULL,
                      lang TEXT NOT NULL,
                      size INTEGER,
                      PRIMARY KEY (series_code, lang)
                    );

                    INSERT INTO series_limitless (series_code, lang, size)
                    VALUES ('BLK', 'en', 172);

                    CREATE TABLE prices_limitless (
                      card_id INTEGER PRIMARY KEY,
                      data_id INTEGER,
                      lang TEXT NOT NULL,
                      set_code TEXT NOT NULL,
                      card_code TEXT NOT NULL,
                      card_name TEXT,
                      rarity TEXT,
                      usd_price REAL,
                      eur_price REAL,
                      ebay_price REAL,
                      observed_at TEXT,
                      observed_date TEXT,
                      created_at TEXT,
                      updated_at TEXT,
                      ebay_observed_at TEXT,
                      ebay_observed_date TEXT
                    );

                    INSERT INTO prices_limitless (
                      card_id, data_id, lang, set_code, card_code, card_name, rarity,
                      usd_price, eur_price, ebay_price, observed_at, observed_date,
                      created_at, updated_at, ebay_observed_at, ebay_observed_date
                    )
                    VALUES (
                      124, 456, 'en', 'BLK', '2', 'Sample Card', 'Uncommon',
                      1.25, 1.10, 2.30, '2026-04-26T00:00:00+00:00', '2026-04-26',
                      '2026-04-26T00:00:00+00:00', '2026-04-26T00:00:00+00:00',
                      '2026-04-26T00:00:00+00:00', '2026-04-26'
                    );
                    """
                )
                conn.commit()

            with temporary_schema() as schema_name:
                counts = run_migration(
                    sqlite_path=sqlite_path,
                    schema_name=schema_name,
                    truncate_existing=True,
                )

                self.assertEqual(counts["series_limitless"], 1)
                self.assertEqual(counts["prices_limitless"], 1)

                with closing(connect_schema(schema_name)) as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            SELECT series_code, lang, size
                            FROM series_limitless
                            """
                        )
                        series_row = cur.fetchone()
                        cur.execute(
                            """
                            SELECT card_id, data_id, lang, set_code, card_code, card_name, usd_price, eur_price, ebay_price
                            FROM prices_limitless
                            """
                        )
                        price_row = cur.fetchone()

                self.assertEqual(series_row, ("BLK", "en", 172))
                self.assertEqual(
                    price_row,
                    (124, 456, "en", "BLK", "2", "Sample Card", 1.25, 1.1, 2.3),
                )


if __name__ == "__main__":
    unittest.main()
