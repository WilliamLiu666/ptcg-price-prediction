from __future__ import annotations

import sqlite3
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
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE prices_limitless
                SET ebay_price = ?
                WHERE lang = ? AND set_code = ? AND card_code = ?
                """,
                (ebay_price, lang, set_code, card_code),
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