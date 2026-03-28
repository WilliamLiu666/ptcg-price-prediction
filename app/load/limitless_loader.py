from __future__ import annotations

import sqlite3
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
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO prices_limitless
                (card_id, data_id, lang, set_code, card_code, card_name, rarity, usd_price, eur_price)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)

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
                    record.get("usd_price"),
                    record.get("eur_price"),
                ),
            )
            conn.commit()


if __name__ == "__main__":
    loader = LimitlessLoader(db_path="ptcg.sqlite")
    loader.ensure_cards_index_table()
    print("cards_index table ensured.")