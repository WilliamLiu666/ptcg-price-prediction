import sqlite3
from pathlib import Path


class EbayPriceRepository:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = str(db_path)

    def update_ebay_price(
        self,
        lang: str,
        set_code: str,
        card_code: str,
        ebay_price: float | None,
    ) -> int:
        """
        Update ebay_price in prices_limitless by (lang, set_code, card_code).

        Returns:
            number of updated rows
        """
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE prices_limitless
                SET ebay_price = ?
                WHERE lang = ? AND set_code = ? AND card_code = ?
                """,
                (ebay_price, lang, set_code, card_code),
            )
            conn.commit()
            return cur.rowcount


if __name__ == "__main__":
    repo = EbayPriceRepository("ptcg.sqlite")

    updated = repo.update_ebay_price(
        lang="en",
        set_code="JTG",
        card_code="1",
        ebay_price=0.08,
    )

    print(f"Updated rows: {updated}")