from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path


class LimitlessLoader:
    """
    Load layer for Limitless.

    Responsibilities:
    - Create/ensure SQLite tables
    - Upsert card index data
    - Upsert price data

    This class does NOT handle:
    - HTTP requests
    - HTML parsing
    """

    def __init__(self, db_path: str | Path) -> None:
        """
        Initialize the loader.

        Args:
            db_path:
                SQLite database path.
        """
        self.db_path = Path(db_path)

    def _connect(self) -> sqlite3.Connection:
        """
        Open a SQLite connection with useful pragmas.
        """
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn

    def ensure_cards_index_table(self) -> None:
        """
        Ensure the cards_index table exists.

        Target schema:
        - PK: card_id
        - UNIQUE(lang, set_code, card_code)
        """
        with self._connect() as conn:
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
            conn.commit()

    def ensure_prices_limitless_schema(self) -> None:
        """
        Ensure prices_limitless has timestamp columns and history table exists.
        """
        with self._connect() as conn:
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
                  UNIQUE(lang, set_code, card_code)
                );

                CREATE INDEX IF NOT EXISTS idx_prices_limitless_lang_set
                  ON prices_limitless(lang, set_code);

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

                CREATE INDEX IF NOT EXISTS idx_prices_limitless_history_card_date
                  ON prices_limitless_history(card_id, observed_date);
                """
            )

            self._ensure_column(conn, "prices_limitless", "observed_at", "TEXT")
            self._ensure_column(conn, "prices_limitless", "observed_date", "TEXT")
            self._ensure_column(conn, "prices_limitless", "created_at", "TEXT")
            self._ensure_column(conn, "prices_limitless", "updated_at", "TEXT")
            self._ensure_column(conn, "prices_limitless", "ebay_price", "REAL")
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_prices_limitless_observed_date
                  ON prices_limitless(observed_date)
                """
            )

            conn.commit()

    @staticmethod
    def _ensure_column(conn: sqlite3.Connection, table: str, column: str, column_type: str) -> None:
        cursor = conn.execute(f"PRAGMA table_info({table})")
        existing = {str(row[1]) for row in cursor.fetchall()}
        if column not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}")

    def save_card_index(self, record: dict[str, str | float | None]) -> None:
        """
        Upsert one card record into cards_index using card_id as primary key.

        Update policy:
        - lang / set_code / card_code are always kept in sync
        - data_id / card_name / rarity only update when new value is not NULL
        """
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO cards_index
                (card_id, data_id, lang, set_code, card_code, card_name, rarity)
                VALUES (?, ?, ?, ?, ?, ?, ?)

                ON CONFLICT(card_id)
                DO UPDATE SET
                    lang = excluded.lang,
                    set_code = excluded.set_code,
                    card_code = excluded.card_code,

                    data_id = CASE
                        WHEN excluded.data_id IS NOT NULL THEN excluded.data_id
                        ELSE cards_index.data_id
                    END,

                    card_name = CASE
                        WHEN excluded.card_name IS NOT NULL THEN excluded.card_name
                        ELSE cards_index.card_name
                    END,

                    rarity = CASE
                        WHEN excluded.rarity IS NOT NULL THEN excluded.rarity
                        ELSE cards_index.rarity
                    END
                """,
                (
                    record.get("card_id"),
                    record.get("data_id"),
                    record.get("lang"),
                    record.get("set_code"),
                    record.get("card_code"),
                    record.get("card_name"),
                    record.get("rarity"),
                ),
            )
            conn.commit()

    def save_card_price(self, record: dict[str, str | float | None]) -> None:
        """
        Upsert one card record into prices_limitless using card_id as primary key.

        Update policy:
        - lang / set_code / card_code are always kept in sync
        - optional metadata only updates when new value is not NULL
        - prices only update when new values are not NULL
        """
        self.ensure_prices_limitless_schema()
        now = datetime.now(timezone.utc)
        observed_at = now.isoformat()
        observed_date = now.date().isoformat()

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO prices_limitless
                (
                  card_id, data_id, lang, set_code, card_code, card_name, rarity,
                  usd_price, eur_price, observed_at, observed_date, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)

                ON CONFLICT(card_id)
                DO UPDATE SET
                    lang = excluded.lang,
                    set_code = excluded.set_code,
                    card_code = excluded.card_code,

                    data_id = CASE
                        WHEN excluded.data_id IS NOT NULL THEN excluded.data_id
                        ELSE prices_limitless.data_id
                    END,

                    card_name = CASE
                        WHEN excluded.card_name IS NOT NULL THEN excluded.card_name
                        ELSE prices_limitless.card_name
                    END,

                    rarity = CASE
                        WHEN excluded.rarity IS NOT NULL THEN excluded.rarity
                        ELSE prices_limitless.rarity
                    END,

                    usd_price = CASE
                        WHEN excluded.usd_price IS NOT NULL THEN excluded.usd_price
                        ELSE prices_limitless.usd_price
                    END,

                    eur_price = CASE
                        WHEN excluded.eur_price IS NOT NULL THEN excluded.eur_price
                        ELSE prices_limitless.eur_price
                    END,
                    observed_at = excluded.observed_at,
                    observed_date = excluded.observed_date,
                    created_at = COALESCE(prices_limitless.created_at, excluded.created_at),
                    updated_at = excluded.updated_at
                """,
                (
                    record.get("card_id"),
                    record.get("data_id"),
                    record.get("lang"),
                    record.get("set_code"),
                    record.get("card_code"),
                    record.get("card_name"),
                    record.get("rarity"),
                    record.get("usd_price"),
                    record.get("eur_price"),
                    observed_at,
                    observed_date,
                    observed_at,
                    observed_at,
                ),
            )

            conn.execute(
                """
                INSERT INTO prices_limitless_history
                (
                  card_id, lang, set_code, card_code, usd_price, eur_price, ebay_price,
                  source, observed_at, observed_date
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, 'limitless', ?, ?)
                """,
                (
                    record.get("card_id"),
                    record.get("lang"),
                    record.get("set_code"),
                    record.get("card_code"),
                    record.get("usd_price"),
                    record.get("eur_price"),
                    None,
                    observed_at,
                    observed_date,
                ),
            )
            conn.commit()


if __name__ == "__main__":
    loader = LimitlessLoader(db_path="ptcg.sqlite")
    loader.ensure_cards_index_table()
    print("cards_index table ensured.")