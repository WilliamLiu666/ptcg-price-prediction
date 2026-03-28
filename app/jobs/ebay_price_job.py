from __future__ import annotations

import sqlite3
from pathlib import Path

from app.extract.ebay_extractor import EbayAuth, EbayExtractor
from app.load.ebay_loader import EbayLoader
from app.services.ebay_price_service import EbayPriceService
from app.transform.ebay_transformer import EbayTransformer


class EbayPriceJob:
    """
    Batch job for eBay price backfill.

    Responsibilities:
    - Read pending cards from database
    - Build search keywords
    - Call service layer for each card
    - Print simple batch progress

    This class does NOT handle:
    - Detailed transformation logic
    - Direct HTTP request construction
    """

    def __init__(
        self,
        db_path: str | Path,
        client_id: str,
        client_secret: str,
        sandbox: bool = False,
    ) -> None:
        """
        Initialize the batch job.

        Args:
            db_path:
                SQLite database path.
            client_id:
                eBay application client ID.
            client_secret:
                eBay application client secret.
            sandbox:
                Whether to use sandbox environment.
        """
        self.db_path = Path(db_path)
        self.client_id = client_id
        self.client_secret = client_secret
        self.sandbox = sandbox

    def load_pending_cards(self) -> list[tuple[str, str, str, str]]:
        """
        Load cards whose eBay price is not filled yet.

        Returns:
            List of (lang, set_code, card_code, card_name).
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT lang, set_code, card_code, card_name
                FROM prices_limitless
                WHERE card_name IS NOT NULL
                  AND TRIM(card_name) <> ''
                  AND ebay_price IS NULL
                """
            )
            return cursor.fetchall()

    def run(self) -> None:
        """
        Run the batch job.
        """
        auth = EbayAuth(
            client_id=self.client_id,
            client_secret=self.client_secret,
            sandbox=self.sandbox,
        )
        extractor = EbayExtractor(auth=auth, sandbox=self.sandbox)
        loader = EbayLoader(db_path=self.db_path)
        service = EbayPriceService(extractor=extractor, loader=loader)

        cards = self.load_pending_cards()
        print(f"Found {len(cards)} rows")

        for index, (lang, set_code, card_code, card_name) in enumerate(cards, start=1):
            keyword = EbayTransformer.build_keyword(card_name, set_code, card_code)
            print(f"\n[{index}/{len(cards)}] Searching: {keyword}")

            try:
                result = service.fetch_and_save(
                    keyword=keyword,
                    lang=lang,
                    set_code=set_code,
                    card_code=card_code,
                    search_limit=50,
                )

                if result:
                    print(
                        f"Saved lowest price: {result.get('total_price')} | "
                        f"title: {result.get('title')}"
                    )
                else:
                    print("No matching items found.")

            except Exception as exc:
                print(f"Error processing {keyword}: {exc}")


def main() -> None:
    """
    Simple standalone entry point for this module.
    """
    job = EbayPriceJob(
        db_path="ptcg.sqlite",
        client_id="",
        client_secret="",
        sandbox=False,
    )
    job.run()


if __name__ == "__main__":
    main()