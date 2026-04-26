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
from app.utils.sqlite_schema import backfill_source_current_tables


def _column_names(conn: sqlite3.Connection, table: str) -> list[str]:
    return [str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()]


def _foreign_keys(conn: sqlite3.Connection, table: str) -> list[tuple[str, str, str]]:
    return [
        (str(row[3]), str(row[2]), str(row[4]))
        for row in conn.execute(f"PRAGMA foreign_key_list({table})").fetchall()
    ]


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


class DbSchemaAlignmentTests(unittest.TestCase):
    def test_limitless_loader_creates_limitless_current_and_history_tables(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "ptcg.sqlite"

            loader = LimitlessLoader(db_path=db_path)
            loader.ensure_prices_limitless_schema()

            with closing(sqlite3.connect(db_path)) as conn:
                current_columns = _column_names(conn, "prices_limitless")
                history_columns = _column_names(conn, "prices_limitless_history")

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
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "ptcg.sqlite"

            loader = EbayLoader(db_path=db_path)
            loader.ensure_ebay_columns()

            with closing(sqlite3.connect(db_path)) as conn:
                current_columns = _column_names(conn, "prices_ebay_current")
                history_columns = _column_names(conn, "prices_ebay_history")
                search_result_indexes = {
                    str(row[1])
                    for row in conn.execute("PRAGMA index_list(ebay_search_results)").fetchall()
                }

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
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "ptcg.sqlite"

            loader = LimitlessLoader(db_path=db_path)
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

            with closing(sqlite3.connect(db_path)) as conn:
                current_row = conn.execute(
                    """
                    SELECT card_id, lang, set_code, card_code, card_name, usd_price, eur_price
                    FROM prices_limitless
                    """
                ).fetchone()
                history_row = conn.execute(
                    """
                    SELECT card_id, lang, set_code, card_code, usd_price, eur_price, source
                    FROM prices_limitless_history
                    """
                ).fetchone()

            self.assertEqual(
                current_row,
                (124, "en", "BLK", "2", "Sample Card", 1.25, 1.1),
            )
            self.assertEqual(
                history_row,
                (124, "en", "BLK", "2", 1.25, 1.1, "limitless"),
            )

    def test_cardrush_loader_writes_cardrush_current_and_history_tables(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "ptcg.sqlite"

            loader = CardrushLoader(db_path=str(db_path))
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

            with closing(sqlite3.connect(db_path)) as conn:
                history_columns = _column_names(conn, "prices_cardrush")
                current_columns = _column_names(conn, "prices_cardrush_current")
                foreign_keys = _foreign_keys(conn, "prices_cardrush")
                history_row = conn.execute(
                    """
                    SELECT product_id, observed_date, price_yen, price_text, source
                    FROM prices_cardrush
                    """
                ).fetchone()
                current_row = conn.execute(
                    """
                    SELECT product_id, price_yen, price_text, source
                    FROM prices_cardrush_current
                    """
                ).fetchone()

            self.assertEqual(
                history_columns,
                ["product_id", "observed_at", "observed_date", "price_yen", "price_text", "source"],
            )
            self.assertEqual(
                current_columns,
                ["product_id", "price_yen", "price_text", "observed_at", "observed_date", "source", "updated_at"],
            )
            self.assertEqual(
                foreign_keys,
                [("product_id", "products_cardrush", "product_id")],
            )
            self.assertEqual(history_row[0], "123")
            self.assertEqual(history_row[2:], (1280, "1,280", "cardrush"))
            self.assertEqual(current_row, ("123", 1280, "1,280", "cardrush"))

    def test_cardrush_loader_repairs_legacy_history_foreign_key_and_preserves_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "ptcg.sqlite"

            with closing(sqlite3.connect(db_path)) as conn:
                conn.executescript(
                    """
                    CREATE TABLE products_cardrush (
                        product_id    TEXT    PRIMARY KEY,
                        product_group TEXT    NOT NULL,
                        model_number  TEXT    NOT NULL,
                        set_size      TEXT,
                        name          TEXT    NOT NULL,
                        name_full     TEXT    NOT NULL,
                        condition     TEXT,
                        model_code    TEXT,
                        price_yen     INTEGER,
                        url           TEXT    NOT NULL,
                        created_at    TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at    TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP
                    );

                    INSERT INTO products_cardrush (
                        product_id, product_group, model_number, set_size,
                        name, name_full, condition, model_code, price_yen, url
                    )
                    VALUES (
                        'legacy-1', '268', '001', '100',
                        'Legacy Card', 'Legacy Card Full', 'A', 'SVJP', 980,
                        'https://www.cardrush-pokemon.jp/product/legacy-1'
                    );

                    CREATE TABLE prices_cardrush (
                        product_id  TEXT    NOT NULL,
                        observed_at TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        price_yen   INTEGER NOT NULL,
                        price_text  TEXT,
                        source      TEXT    NOT NULL DEFAULT 'cardrush',
                        PRIMARY KEY (product_id, observed_at),
                        FOREIGN KEY (product_id) REFERENCES products(product_id)
                    );

                    INSERT INTO prices_cardrush (
                        product_id, observed_at, price_yen, price_text, source
                    )
                    VALUES ('legacy-1', '2026-04-25T00:00:00+00:00', 980, '980', 'cardrush');
                    """
                )
                conn.commit()

            loader = CardrushLoader(db_path=str(db_path))
            written = loader.save_products(
                product_group="268",
                items=[
                    {
                        "product_id": "new-1",
                        "product_url": "https://www.cardrush-pokemon.jp/product/new-1",
                        "name": "New Card",
                        "name_full": "New Card Full",
                        "condition": "A",
                        "model_number": "002",
                        "set_size": "100",
                        "model_code": "SVJP",
                        "price": "1,280",
                    }
                ],
                parse_price_func=lambda value: 1280.0,
            )

            self.assertEqual(written, 1)

            with closing(sqlite3.connect(db_path)) as conn:
                foreign_keys = _foreign_keys(conn, "prices_cardrush")
                product_ids = [
                    str(row[0])
                    for row in conn.execute(
                        "SELECT product_id FROM prices_cardrush ORDER BY product_id, observed_at"
                    ).fetchall()
                ]

            self.assertEqual(
                foreign_keys,
                [("product_id", "products_cardrush", "product_id")],
            )
            self.assertEqual(product_ids, ["legacy-1", "new-1"])

    def test_hareruya_loader_writes_hareruya_current_and_history_tables(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "ptcg.sqlite"

            loader = HareruyaLoader(db_path=str(db_path))
            written = loader.save_product_prices(
                [
                    {
                        "product_id": "9921823932736",
                        "collection_id": "706",
                        "set_code": "M2",
                        "card_number": "001",
                        "card_name_jp": "ナゾノクサ",
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

            with closing(sqlite3.connect(db_path)) as conn:
                self.assertTrue(_table_exists(conn, "prices_hareruya_current"))
                self.assertTrue(_table_exists(conn, "prices_hareruya_history"))
                current_row = conn.execute(
                    """
                    SELECT product_id, set_code, card_number, card_name_en, price_jpy, currency
                    FROM prices_hareruya_current
                    """
                ).fetchone()
                history_row = conn.execute(
                    """
                    SELECT product_id, set_code, card_number, card_name_en, price_jpy, currency
                    FROM prices_hareruya_history
                    """
                ).fetchone()

            self.assertEqual(
                current_row,
                ("9921823932736", "M2", "001", "Oddish", 30.0, "JPY"),
            )
            self.assertEqual(
                history_row,
                ("9921823932736", "M2", "001", "Oddish", 30.0, "JPY"),
            )

    def test_backfill_source_current_tables_from_legacy_tables(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "ptcg.sqlite"

            with closing(sqlite3.connect(db_path)) as conn:
                conn.executescript(
                    """
                    CREATE TABLE prices_limitless (
                        card_id INTEGER PRIMARY KEY,
                        lang TEXT NOT NULL,
                        set_code TEXT NOT NULL,
                        card_code TEXT NOT NULL,
                        card_name TEXT,
                        usd_price REAL,
                        eur_price REAL,
                        ebay_price REAL,
                        observed_at TEXT,
                        ebay_observed_at TEXT,
                        updated_at TEXT
                    );

                    INSERT INTO prices_limitless (
                        card_id, lang, set_code, card_code, card_name,
                        usd_price, eur_price, ebay_price, observed_at, ebay_observed_at, updated_at
                    )
                    VALUES (
                        124, 'en', 'BLK', '2', 'Sample Card',
                        1.25, 1.10, 2.30, '2026-03-29T00:00:00+00:00',
                        '2026-03-30T00:00:00+00:00', '2026-03-30T00:00:00+00:00'
                    );

                    CREATE TABLE prices_ebay_history (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        card_id INTEGER,
                        lang TEXT NOT NULL,
                        set_code TEXT NOT NULL,
                        card_code TEXT NOT NULL,
                        ebay_price REAL,
                        ebay_observed_at TEXT NOT NULL,
                        ebay_observed_date TEXT NOT NULL
                    );

                    INSERT INTO prices_ebay_history (
                        card_id, lang, set_code, card_code, ebay_price, ebay_observed_at, ebay_observed_date
                    )
                    VALUES (124, 'en', 'BLK', '2', 2.30, '2026-03-30T00:00:00+00:00', '2026-03-30');

                    CREATE TABLE products_cardrush (
                        product_id TEXT PRIMARY KEY,
                        product_group TEXT NOT NULL,
                        model_number TEXT NOT NULL,
                        set_size TEXT,
                        name TEXT NOT NULL,
                        name_full TEXT NOT NULL,
                        condition TEXT,
                        model_code TEXT,
                        price_yen INTEGER,
                        url TEXT NOT NULL,
                        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                    );

                    INSERT INTO products_cardrush (
                        product_id, product_group, model_number, set_size, name, name_full,
                        condition, model_code, price_yen, url, updated_at
                    )
                    VALUES (
                        '123', '268', '001', '100', 'Card Name', 'Full Card Name',
                        'A', 'SVJP', 1280, 'https://example.com/123', '2026-03-29T00:00:00+00:00'
                    );

                    CREATE TABLE prices_cardrush (
                        product_id TEXT NOT NULL,
                        observed_at TEXT NOT NULL,
                        price_yen INTEGER NOT NULL,
                        price_text TEXT,
                        source TEXT NOT NULL DEFAULT 'cardrush',
                        PRIMARY KEY (product_id, observed_at)
                    );

                    INSERT INTO prices_cardrush (
                        product_id, observed_at, price_yen, price_text, source
                    )
                    VALUES ('123', '2026-03-29T00:00:00+00:00', 1280, '1,280', 'cardrush');
                    """
                )
                backfill_source_current_tables(conn)
                conn.commit()

            with closing(sqlite3.connect(db_path)) as conn:
                self.assertTrue(_table_exists(conn, "prices_ebay_current"))
                self.assertTrue(_table_exists(conn, "prices_cardrush_current"))
                current_rows = conn.execute(
                    """
                    SELECT marketplace_id, currency, card_id, ebay_price
                    FROM prices_ebay_current
                    """
                ).fetchall()
                cardrush_current_rows = conn.execute(
                    """
                    SELECT product_id, price_yen, price_text
                    FROM prices_cardrush_current
                    """
                ).fetchall()

            self.assertEqual(current_rows, [("EBAY_GB", "GBP", 124, 2.3)])
            self.assertEqual(cardrush_current_rows, [("123", 1280, "1,280")])


if __name__ == "__main__":
    unittest.main()
