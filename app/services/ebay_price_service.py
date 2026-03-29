from __future__ import annotations

from typing import Any

from app.config import get_ebay_credentials, parse_bool_env
from app.extract.ebay_extractor import EbayExtractor
from app.load.ebay_loader import EbayLoader
from app.transform.ebay_transformer import EbayTransformer


class EbayPriceService:
    """
    Service layer for eBay price pipeline.

    Responsibilities:
    - Orchestrate extract, transform, and load steps
    - Apply business rule for best price selection
    - Persist selected result into database

    This class does NOT handle:
    - Batch job iteration
    - Database schema creation
    """

    def __init__(
        self,
        extractor: EbayExtractor,
        loader: EbayLoader,
    ) -> None:
        """
        Initialize the service.

        Args:
            extractor:
                Extract layer instance.
            loader:
                Load layer instance.
        """
        self.extractor = extractor
        self.loader = loader

    def fetch_and_save(
        self,
        keyword: str,
        lang: str,
        set_code: str,
        card_code: str,
        search_limit: int = 50,
    ) -> dict[str, Any] | None:
        """
        Run the full pipeline for one card.

        Steps:
        1. Search raw items from eBay
        2. Normalize and rank candidate items
        3. Pick the best item
        4. Save selected price into database

        Args:
            keyword:
                Search keyword.
            lang:
                Card language.
            set_code:
                Card set code.
            card_code:
                Card number/code.
            search_limit:
                Max number of raw items to request.

        Returns:
            Best item, or None if no valid item is found.
        """
        raw_items = self.extractor.search_items(keyword=keyword, limit=search_limit)
        normalized_items = EbayTransformer.normalize_items(raw_items)
        best_item = EbayTransformer.pick_best_item(normalized_items)

        if best_item is None:
            self.loader.update_ebay_price(
                lang=lang,
                set_code=set_code,
                card_code=card_code,
                ebay_price=None,
            )
            return None

        lowest_price = best_item["total_price"]

        self.loader.update_ebay_price(
            lang=lang,
            set_code=set_code,
            card_code=card_code,
            ebay_price=lowest_price,
        )

        return best_item


def main() -> None:
    """
    Simple standalone test for this module.
    """
    from app.extract.ebay_extractor import EbayAuth

    client_id, client_secret = get_ebay_credentials()
    sandbox = parse_bool_env("EBAY_SANDBOX", default=False)

    auth = EbayAuth(
        client_id=client_id,
        client_secret=client_secret,
        sandbox=sandbox,
    )
    extractor = EbayExtractor(auth=auth, sandbox=sandbox)
    loader = EbayLoader(db_path="ptcg.sqlite")

    service = EbayPriceService(extractor=extractor, loader=loader)

    result = service.fetch_and_save(
        keyword="pokemon Caterpie JTG 001",
        lang="en",
        set_code="JTG",
        card_code="1",
        search_limit=5,
    )

    print(result)


if __name__ == "__main__":
    main()