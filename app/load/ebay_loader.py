from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path


class EbayLoader:
    """
    Load layer for eBay prices.

    Responsibilities:
    - Update eBay price in SQLite
    - Manage database connection settings

    This class does NOT handle:
    - HTTP requests
    - Search keyword generation
    - Price selection logic
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

    def ensure_ebay_columns(self) -> None:
        """
        Ensure eBay timestamp columns/history exist.
        """
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS prices_limitless (
                  card_id       INTEGER PRIMARY KEY,
                  lang          TEXT NOT NULL,
                  set_code      TEXT NOT NULL,
                  card_code     TEXT NOT NULL,
                  ebay_price    REAL
                );
                """
            )
            self._ensure_column(conn, "prices_limitless", "ebay_price", "REAL")
            self._ensure_column(conn, "prices_limitless", "ebay_observed_at", "TEXT")
            self._ensure_column(conn, "prices_limitless", "ebay_observed_date", "TEXT")
            self._ensure_column(conn, "prices_limitless", "updated_at", "TEXT")

            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS prices_ebay_history (
                  id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                  card_id            INTEGER,
                  lang               TEXT NOT NULL,
                  set_code           TEXT NOT NULL,
                  card_code          TEXT NOT NULL,
                  ebay_price         REAL,
                  ebay_observed_at   TEXT NOT NULL,
                  ebay_observed_date TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_prices_ebay_history_card_date
                  ON prices_ebay_history(card_id, ebay_observed_date);
                """
            )
            conn.commit()

    @staticmethod
    def _ensure_column(conn: sqlite3.Connection, table: str, column: str, column_type: str) -> None:
        cursor = conn.execute(f"PRAGMA table_info({table})")
        existing = {str(row[1]) for row in cursor.fetchall()}
        if column not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}")

    def update_ebay_price(
        self,
        lang: str,
        set_code: str,
        card_code: str,
        ebay_price: float | None,
    ) -> int:
        """
        Update ebay_price in prices_limitless by business key.

        Args:
            lang:
                Card language.
            set_code:
                Card set code.
            card_code:
                Card number/code.
            ebay_price:
                Selected eBay price.

        Returns:
            Number of updated rows.
        """
        self.ensure_ebay_columns()
        now = datetime.now(timezone.utc)
        observed_at = now.isoformat()
        observed_date = now.date().isoformat()

        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE prices_limitless
                SET ebay_price = ?,
                    ebay_observed_at = ?,
                    ebay_observed_date = ?,
                    updated_at = ?
                WHERE lang = ? AND set_code = ? AND card_code = ?
                """,
                (ebay_price, observed_at, observed_date, observed_at, lang, set_code, card_code),
            )
            if cursor.rowcount > 0:
                conn.execute(
                    """
                    INSERT INTO prices_ebay_history
                    (
                      card_id, lang, set_code, card_code, ebay_price, ebay_observed_at, ebay_observed_date
                    )
                    SELECT card_id, lang, set_code, card_code, ebay_price, ebay_observed_at, ebay_observed_date
                    FROM prices_limitless
                    WHERE lang = ? AND set_code = ? AND card_code = ?
                    """,
                    (lang, set_code, card_code),
                )
            conn.commit()
            return cursor.rowcount


def main() -> None:
    """
    Simple standalone test for this module.
    """
    loader = EbayLoader(db_path="ptcg.sqlite")

    updated_rows = loader.update_ebay_price(
        lang="en",
        set_code="JTG",
        card_code="1",
        ebay_price=0.99,
    )
    print(f"Updated rows: {updated_rows}")


if __name__ == "__main__":
    main()