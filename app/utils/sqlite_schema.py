from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable


PRICES_LIMITLESS_COLUMNS: tuple[tuple[str, str], ...] = (
    ("card_id", "INTEGER PRIMARY KEY"),
    ("data_id", "INTEGER"),
    ("lang", "TEXT NOT NULL"),
    ("set_code", "TEXT NOT NULL"),
    ("card_code", "TEXT NOT NULL"),
    ("card_name", "TEXT"),
    ("rarity", "TEXT"),
    ("usd_price", "REAL"),
    ("eur_price", "REAL"),
    ("ebay_price", "REAL"),
    ("observed_at", "TEXT"),
    ("observed_date", "TEXT"),
    ("created_at", "TEXT"),
    ("updated_at", "TEXT"),
    ("ebay_observed_at", "TEXT"),
    ("ebay_observed_date", "TEXT"),
)

PRICES_CARDRUSH_HISTORY_COLUMNS: tuple[tuple[str, str], ...] = (
    ("product_id", "TEXT NOT NULL"),
    ("observed_at", "TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP"),
    ("observed_date", "TEXT"),
    ("price_yen", "INTEGER NOT NULL"),
    ("price_text", "TEXT"),
    ("source", "TEXT NOT NULL DEFAULT 'cardrush'"),
)

EXPECTED_PRICES_CARDRUSH_FKS: tuple[tuple[str, str, str], ...] = (
    ("product_id", "products_cardrush", "product_id"),
)


def connect_sqlite(db_path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(Path(db_path))
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def column_names(conn: sqlite3.Connection, table: str) -> list[str]:
    return [str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()]


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def ensure_columns(
    conn: sqlite3.Connection,
    table: str,
    columns: Iterable[tuple[str, str]],
) -> None:
    existing = set(column_names(conn, table))
    for column, column_type in columns:
        if column in existing:
            continue
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}")
        existing.add(column)


def foreign_key_targets(conn: sqlite3.Connection, table: str) -> list[tuple[str, str, str]]:
    return [
        (str(row[3]), str(row[2]), str(row[4]))
        for row in conn.execute(f"PRAGMA foreign_key_list({table})").fetchall()
    ]


def ensure_cards_index_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS cards_index (
          card_id   INTEGER PRIMARY KEY,
          data_id   INTEGER,
          lang      TEXT NOT NULL,
          set_code  TEXT NOT NULL,
          card_code TEXT NOT NULL,
          card_name TEXT,
          rarity    TEXT,
          UNIQUE(lang, set_code, card_code)
        );

        CREATE INDEX IF NOT EXISTS idx_cards_index_lang_set
          ON cards_index(lang, set_code);

        CREATE INDEX IF NOT EXISTS idx_cards_index_rarity
          ON cards_index(rarity);
        """
    )


def ensure_prices_limitless_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS prices_limitless (
          card_id       INTEGER PRIMARY KEY,
          data_id       INTEGER,
          lang          TEXT NOT NULL,
          set_code      TEXT NOT NULL,
          card_code     TEXT NOT NULL,
          card_name     TEXT,
          rarity        TEXT,
          usd_price     REAL,
          eur_price     REAL,
          ebay_price    REAL,
          observed_at   TEXT,
          observed_date TEXT,
          created_at    TEXT,
          updated_at    TEXT,
          ebay_observed_at   TEXT,
          ebay_observed_date TEXT,
          UNIQUE(lang, set_code, card_code)
        );

        CREATE TABLE IF NOT EXISTS prices_limitless_history (
          id            INTEGER PRIMARY KEY AUTOINCREMENT,
          card_id       INTEGER,
          lang          TEXT NOT NULL,
          set_code      TEXT NOT NULL,
          card_code     TEXT NOT NULL,
          usd_price     REAL,
          eur_price     REAL,
          ebay_price    REAL,
          source        TEXT NOT NULL DEFAULT 'limitless',
          observed_at   TEXT NOT NULL,
          observed_date TEXT NOT NULL
        );
        """
    )

    ensure_columns(
        conn,
        "prices_limitless",
        (
            ("data_id", "INTEGER"),
            ("card_name", "TEXT"),
            ("rarity", "TEXT"),
            ("usd_price", "REAL"),
            ("eur_price", "REAL"),
            ("ebay_price", "REAL"),
            ("observed_at", "TEXT"),
            ("observed_date", "TEXT"),
            ("created_at", "TEXT"),
            ("updated_at", "TEXT"),
            ("ebay_observed_at", "TEXT"),
            ("ebay_observed_date", "TEXT"),
        ),
    )

    conn.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_prices_limitless_lang_set
          ON prices_limitless(lang, set_code);

        CREATE INDEX IF NOT EXISTS idx_prices_limitless_observed_date
          ON prices_limitless(observed_date);

        CREATE INDEX IF NOT EXISTS idx_prices_limitless_history_card_date
          ON prices_limitless_history(card_id, observed_date);
        """
    )

    conn.execute(
        """
        DELETE FROM prices_limitless_history
        WHERE id NOT IN (
          SELECT MAX(id)
          FROM prices_limitless_history
          GROUP BY lang, set_code, card_code, source, observed_date
        )
        """
    )

    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_prices_limitless_history_card_source_date
          ON prices_limitless_history(
            lang,
            set_code,
            card_code,
            source,
            observed_date
          )
        """
    )


def ensure_ebay_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS prices_ebay_current (
          card_id            INTEGER,
          lang               TEXT NOT NULL,
          set_code           TEXT NOT NULL,
          card_code          TEXT NOT NULL,
          card_name          TEXT,
          marketplace_id     TEXT NOT NULL DEFAULT 'EBAY_GB',
          currency           TEXT NOT NULL DEFAULT 'GBP',
          condition          TEXT,
          selected_item_id   TEXT,
          selected_title     TEXT,
          selected_item_web_url TEXT,
          ebay_price         REAL,
          observed_at        TEXT NOT NULL,
          observed_date      TEXT NOT NULL,
          created_at         TEXT NOT NULL,
          updated_at         TEXT NOT NULL,
          UNIQUE(lang, set_code, card_code, marketplace_id, currency)
        );

        CREATE TABLE IF NOT EXISTS prices_ebay_history (
          id                 INTEGER PRIMARY KEY AUTOINCREMENT,
          card_id            INTEGER,
          lang               TEXT NOT NULL,
          set_code           TEXT NOT NULL,
          card_code          TEXT NOT NULL,
          card_name          TEXT,
          marketplace_id     TEXT NOT NULL DEFAULT 'EBAY_GB',
          currency           TEXT NOT NULL DEFAULT 'GBP',
          condition          TEXT,
          selected_item_id   TEXT,
          selected_title     TEXT,
          selected_item_web_url TEXT,
          ebay_price         REAL,
          ebay_observed_at   TEXT NOT NULL,
          ebay_observed_date TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS ebay_search_results (
          id           INTEGER PRIMARY KEY AUTOINCREMENT,
          keyword      TEXT NOT NULL,
          item_id      TEXT,
          title        TEXT,
          price_value  REAL,
          currency     TEXT,
          item_web_url TEXT,
          condition    TEXT,
          observed_at  TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    ensure_columns(
        conn,
        "prices_ebay_current",
        (
            ("card_id", "INTEGER"),
            ("card_name", "TEXT"),
            ("marketplace_id", "TEXT NOT NULL DEFAULT 'EBAY_GB'"),
            ("currency", "TEXT NOT NULL DEFAULT 'GBP'"),
            ("condition", "TEXT"),
            ("selected_item_id", "TEXT"),
            ("selected_title", "TEXT"),
            ("selected_item_web_url", "TEXT"),
            ("ebay_price", "REAL"),
            ("observed_at", "TEXT NOT NULL"),
            ("observed_date", "TEXT NOT NULL"),
            ("created_at", "TEXT NOT NULL"),
            ("updated_at", "TEXT NOT NULL"),
        ),
    )
    ensure_columns(
        conn,
        "prices_ebay_history",
        (
            ("card_name", "TEXT"),
            ("marketplace_id", "TEXT NOT NULL DEFAULT 'EBAY_GB'"),
            ("currency", "TEXT NOT NULL DEFAULT 'GBP'"),
            ("condition", "TEXT"),
            ("selected_item_id", "TEXT"),
            ("selected_title", "TEXT"),
            ("selected_item_web_url", "TEXT"),
        ),
    )

    conn.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_prices_ebay_current_card_market
          ON prices_ebay_current(card_id, marketplace_id, currency);

        CREATE INDEX IF NOT EXISTS idx_prices_ebay_current_observed_date
          ON prices_ebay_current(observed_date);

        CREATE INDEX IF NOT EXISTS idx_prices_ebay_history_card_date
          ON prices_ebay_history(card_id, ebay_observed_date);

        CREATE INDEX IF NOT EXISTS idx_prices_ebay_history_market_date
          ON prices_ebay_history(marketplace_id, ebay_observed_date);

        CREATE INDEX IF NOT EXISTS idx_ebay_search_keyword_observed_at
          ON ebay_search_results(keyword, observed_at);
        """
    )

    conn.execute(
        """
        UPDATE prices_ebay_history
        SET marketplace_id = COALESCE(NULLIF(TRIM(marketplace_id), ''), 'EBAY_GB'),
            currency = COALESCE(NULLIF(TRIM(currency), ''), 'GBP')
        """
    )

    conn.execute(
        """
        DELETE FROM prices_ebay_history
        WHERE id NOT IN (
          SELECT MAX(id)
          FROM prices_ebay_history
          GROUP BY
            lang,
            set_code,
            card_code,
            marketplace_id,
            currency,
            ebay_observed_date
        )
        """
    )

    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_prices_ebay_history_card_market_date
          ON prices_ebay_history(
            lang,
            set_code,
            card_code,
            marketplace_id,
            currency,
            ebay_observed_date
          )
        """
    )


def ensure_hareruya_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS prices_hareruya_current (
          product_id            TEXT PRIMARY KEY,
          collection_id         TEXT,
          set_code              TEXT,
          card_number           TEXT,
          card_name_jp          TEXT,
          card_name_en          TEXT,
          variant_title         TEXT,
          currency              TEXT NOT NULL DEFAULT 'JPY',
          price_jpy             REAL,
          compare_at_price_jpy  REAL,
          product_url           TEXT,
          observed_at           TEXT NOT NULL,
          observed_date         TEXT NOT NULL,
          created_at            TEXT NOT NULL,
          updated_at            TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS prices_hareruya_history (
          id                   INTEGER PRIMARY KEY AUTOINCREMENT,
          product_id           TEXT NOT NULL,
          collection_id        TEXT,
          set_code             TEXT,
          card_number          TEXT,
          card_name_jp         TEXT,
          card_name_en         TEXT,
          variant_title        TEXT,
          currency             TEXT NOT NULL DEFAULT 'JPY',
          price_jpy            REAL,
          compare_at_price_jpy REAL,
          product_url          TEXT,
          observed_at          TEXT NOT NULL,
          observed_date        TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_prices_hareruya_current_set_code
          ON prices_hareruya_current(set_code);

        CREATE INDEX IF NOT EXISTS idx_prices_hareruya_history_product_date
          ON prices_hareruya_history(product_id, observed_date);

        CREATE INDEX IF NOT EXISTS idx_prices_hareruya_history_set_date
          ON prices_hareruya_history(set_code, observed_date);
        """
    )

    conn.execute(
        """
        DELETE FROM prices_hareruya_history
        WHERE id NOT IN (
          SELECT MAX(id)
          FROM prices_hareruya_history
          GROUP BY product_id, observed_date
        )
        """
    )

    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_prices_hareruya_history_product_date
          ON prices_hareruya_history(product_id, observed_date)
        """
    )


def ensure_cardrush_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS products_cardrush (
          product_id    TEXT PRIMARY KEY,
          product_group TEXT NOT NULL,
          model_number  TEXT NOT NULL,
          set_size      TEXT,
          name          TEXT NOT NULL,
          name_full     TEXT NOT NULL,
          condition     TEXT,
          model_code    TEXT,
          price_yen     INTEGER,
          url           TEXT NOT NULL,
          created_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS prices_cardrush_current (
          product_id    TEXT PRIMARY KEY,
          price_yen     INTEGER NOT NULL,
          price_text    TEXT,
          observed_at   TEXT NOT NULL,
          observed_date TEXT NOT NULL,
          source        TEXT NOT NULL DEFAULT 'cardrush',
          updated_at    TEXT NOT NULL,
          FOREIGN KEY (product_id) REFERENCES products_cardrush(product_id)
        );

        CREATE INDEX IF NOT EXISTS idx_products_cardrush_group
          ON products_cardrush(product_group);

        CREATE INDEX IF NOT EXISTS idx_products_cardrush_model_number
          ON products_cardrush(model_number);
        """
    )

    expected_columns = [name for name, _ in PRICES_CARDRUSH_HISTORY_COLUMNS]
    current_columns = column_names(conn, "prices_cardrush")
    current_fks = foreign_key_targets(conn, "prices_cardrush")

    if not current_columns:
        _create_prices_cardrush_history_table(conn)
    elif current_columns != expected_columns or current_fks != list(EXPECTED_PRICES_CARDRUSH_FKS):
        _rebuild_prices_cardrush_history_table(conn)

    ensure_columns(
        conn,
        "prices_cardrush_current",
        (
            ("price_text", "TEXT"),
            ("observed_date", "TEXT NOT NULL"),
            ("source", "TEXT NOT NULL DEFAULT 'cardrush'"),
            ("updated_at", "TEXT NOT NULL"),
        ),
    )

    conn.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_prices_cardrush_current_observed_date
          ON prices_cardrush_current(observed_date);

        CREATE INDEX IF NOT EXISTS idx_prices_cardrush_observed_at
          ON prices_cardrush(observed_at);
        """
    )

    conn.execute(
        """
        UPDATE prices_cardrush
        SET observed_date = COALESCE(
          observed_date,
          CASE
            WHEN LENGTH(observed_at) >= 10 THEN SUBSTR(observed_at, 1, 10)
            ELSE NULL
          END
        )
        WHERE observed_date IS NULL
        """
    )


def _create_prices_cardrush_history_table(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE prices_cardrush (
          product_id    TEXT NOT NULL,
          observed_at   TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          observed_date TEXT,
          price_yen     INTEGER NOT NULL,
          price_text    TEXT,
          source        TEXT NOT NULL DEFAULT 'cardrush',
          PRIMARY KEY (product_id, observed_at),
          FOREIGN KEY (product_id) REFERENCES products_cardrush(product_id)
        );

        CREATE INDEX IF NOT EXISTS idx_prices_cardrush_observed_at
          ON prices_cardrush(observed_at);
        """
    )


def _rebuild_prices_cardrush_history_table(conn: sqlite3.Connection) -> None:
    conn.commit()
    conn.execute("PRAGMA foreign_keys=OFF;")
    try:
        conn.execute("BEGIN")
        conn.execute("ALTER TABLE prices_cardrush RENAME TO prices_cardrush__legacy")
        _create_prices_cardrush_history_table(conn)

        legacy_columns = set(column_names(conn, "prices_cardrush__legacy"))
        observed_date_expr = (
            "observed_date"
            if "observed_date" in legacy_columns
            else "CASE WHEN LENGTH(observed_at) >= 10 THEN SUBSTR(observed_at, 1, 10) ELSE NULL END"
        )
        price_text_expr = "price_text" if "price_text" in legacy_columns else "NULL"
        source_expr = "source" if "source" in legacy_columns else "'cardrush'"

        conn.execute(
            f"""
            INSERT INTO prices_cardrush (
              product_id, observed_at, observed_date, price_yen, price_text, source
            )
            SELECT
              product_id,
              observed_at,
              {observed_date_expr},
              price_yen,
              {price_text_expr},
              COALESCE({source_expr}, 'cardrush')
            FROM prices_cardrush__legacy
            """
        )
        conn.execute("DROP TABLE prices_cardrush__legacy")
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.execute("PRAGMA foreign_keys=ON;")


def backfill_source_current_tables(conn: sqlite3.Connection) -> None:
    ensure_prices_limitless_schema(conn)
    ensure_ebay_schema(conn)
    ensure_hareruya_schema(conn)
    ensure_cardrush_schema(conn)

    if table_exists(conn, "prices_limitless"):
        conn.execute(
            """
            INSERT INTO prices_ebay_current (
              card_id, lang, set_code, card_code, card_name,
              marketplace_id, currency, condition,
              ebay_price, observed_at, observed_date, created_at, updated_at
            )
            SELECT
              card_id,
              lang,
              set_code,
              card_code,
              card_name,
              'EBAY_GB',
              'GBP',
              NULL,
              ebay_price,
              COALESCE(ebay_observed_at, updated_at, observed_at, CURRENT_TIMESTAMP),
              COALESCE(
                ebay_observed_date,
                CASE
                  WHEN LENGTH(COALESCE(ebay_observed_at, updated_at, observed_at, '')) >= 10
                    THEN SUBSTR(COALESCE(ebay_observed_at, updated_at, observed_at), 1, 10)
                  ELSE DATE('now')
                END
              ),
              COALESCE(created_at, CURRENT_TIMESTAMP),
              COALESCE(updated_at, COALESCE(ebay_observed_at, observed_at, CURRENT_TIMESTAMP))
            FROM prices_limitless
            WHERE ebay_price IS NOT NULL
            ON CONFLICT(lang, set_code, card_code, marketplace_id, currency) DO UPDATE SET
              card_id = COALESCE(excluded.card_id, prices_ebay_current.card_id),
              card_name = COALESCE(excluded.card_name, prices_ebay_current.card_name),
              condition = COALESCE(excluded.condition, prices_ebay_current.condition),
              ebay_price = excluded.ebay_price,
              observed_at = excluded.observed_at,
              observed_date = excluded.observed_date,
              updated_at = excluded.updated_at
            WHERE excluded.observed_at >= prices_ebay_current.observed_at
            """
        )
        conn.execute(
            """
            UPDATE prices_ebay_history
            SET card_name = COALESCE(card_name, (
                  SELECT p.card_name
                  FROM prices_limitless p
                  WHERE p.card_id = prices_ebay_history.card_id
              )),
              marketplace_id = COALESCE(NULLIF(TRIM(marketplace_id), ''), 'EBAY_GB'),
              currency = COALESCE(NULLIF(TRIM(currency), ''), 'GBP')
            """
        )

    if table_exists(conn, "products_cardrush"):
        conn.execute(
            """
            INSERT INTO prices_cardrush_current (
              product_id, price_yen, price_text, observed_at, observed_date, source, updated_at
            )
            SELECT
              p.product_id,
              p.price_yen,
              h.price_text,
              COALESCE(h.observed_at, p.updated_at, CURRENT_TIMESTAMP),
              COALESCE(
                h.observed_date,
                CASE
                  WHEN LENGTH(COALESCE(h.observed_at, p.updated_at, '')) >= 10
                    THEN SUBSTR(COALESCE(h.observed_at, p.updated_at), 1, 10)
                  ELSE DATE('now')
                END
              ),
              'cardrush',
              COALESCE(p.updated_at, CURRENT_TIMESTAMP)
            FROM products_cardrush p
            LEFT JOIN prices_cardrush h
              ON h.product_id = p.product_id
             AND h.observed_at = (
               SELECT MAX(h2.observed_at)
               FROM prices_cardrush h2
               WHERE h2.product_id = p.product_id
             )
            WHERE p.price_yen IS NOT NULL
            ON CONFLICT(product_id) DO UPDATE SET
              price_yen = excluded.price_yen,
              price_text = COALESCE(excluded.price_text, prices_cardrush_current.price_text),
              observed_at = excluded.observed_at,
              observed_date = excluded.observed_date,
              updated_at = excluded.updated_at
            WHERE excluded.observed_at >= prices_cardrush_current.observed_at
            """
        )


def drop_generic_price_model(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        DROP TABLE IF EXISTS price_history;
        DROP TABLE IF EXISTS price_current;
        """
    )
