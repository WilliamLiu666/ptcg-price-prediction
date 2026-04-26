from __future__ import annotations

from typing import Any


class EbayTransformer:
    """
    Transform layer for eBay.

    Responsibilities:
    - Build search keywords
    - Normalize raw eBay item data
    - Calculate total price
    - Sort candidate items by total price

    This class does NOT handle:
    - HTTP requests
    - Database writes
    """

    @staticmethod
    def _to_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def build_keyword(card_name: str, set_code: str, card_code: str) -> str:
        """
        Build eBay search keyword for one card.

        Args:
            card_name:
                Card name.
            set_code:
                Set code.
            card_code:
                Card number/code.

        Returns:
            Combined search keyword.
        """
        padded_code = card_code.strip().zfill(3)

        parts = [
            card_name.strip(),
            set_code.strip(),
            padded_code,
        ]

        return " ".join(part for part in parts if part)

    @staticmethod
    def normalize_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Normalize raw eBay item summaries.

        Transformation rules:
        - Skip items without price
        - Use first shipping option if available
        - Compute total_price = price + shipping
        - Sort ascending by total_price

        Args:
            items:
                Raw item summaries returned by eBay API.

        Returns:
            Normalized and sorted item list.
        """
        results: list[dict[str, Any]] = []

        for item in items:
            price = item.get("price", {})
            shipping_options = item.get("shippingOptions", [])

            if not isinstance(price, dict):
                continue
            if not isinstance(shipping_options, list):
                shipping_options = []

            price_value = EbayTransformer._to_float(price.get("value"))
            if price_value is None:
                continue

            shipping_cost_value = 0.0
            shipping_cost_currency = None

            if shipping_options:
                first_shipping_option = shipping_options[0]
                if isinstance(first_shipping_option, dict):
                    shipping_cost = first_shipping_option.get("shippingCost", {})
                    if isinstance(shipping_cost, dict):
                        shipping_cost_value = EbayTransformer._to_float(
                            shipping_cost.get("value")
                        ) or 0.0
                        shipping_cost_currency = shipping_cost.get("currency")

            total_price = price_value + shipping_cost_value

            results.append(
                {
                    "item_id": item.get("itemId"),
                    "title": item.get("title"),
                    "price_value": price_value,
                    "shipping_cost_value": shipping_cost_value,
                    "total_price": total_price,
                    "currency": price.get("currency"),
                    "shipping_currency": shipping_cost_currency,
                    "item_web_url": item.get("itemWebUrl"),
                    "condition": item.get("condition"),
                }
            )

        results.sort(key=lambda row: row["total_price"])
        return results

    @staticmethod
    def pick_best_item(items: list[dict[str, Any]]) -> dict[str, Any] | None:
        """
        Pick the best item from normalized items.

        Current policy:
        - Select the lowest total_price item

        Args:
            items:
                Normalized item list.

        Returns:
            Best item or None if input is empty.
        """
        if not items:
            return None
        return items[0]

    def transform_search_results(
        self,
        payload: dict[str, Any],
        context: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Transform one raw eBay search payload into a normalized card record.
        """
        raw_items = payload.get("itemSummaries")
        if not isinstance(raw_items, list):
            raw_items = []

        normalized_items = self.normalize_items(
            [item for item in raw_items if isinstance(item, dict)]
        )
        best_item = self.pick_best_item(normalized_items)

        return {
            "source": "ebay",
            "card_id": context.get("card_id"),
            "lang": context.get("lang"),
            "set_code": context.get("set_code"),
            "card_code": context.get("card_code"),
            "card_name": context.get("card_name"),
            "search_keyword": context.get("search_keyword"),
            "marketplace_id": context.get("marketplace_id"),
            "search_limit": context.get("search_limit"),
            "source_url": context.get("source_url"),
            "final_url": context.get("final_url"),
            "raw_json_path": context.get("raw_json_path"),
            "raw_item_count": len(raw_items),
            "normalized_item_count": len(normalized_items),
            "selected_item_id": best_item.get("item_id") if best_item else None,
            "selected_title": best_item.get("title") if best_item else None,
            "selected_condition": best_item.get("condition") if best_item else None,
            "selected_price_value": best_item.get("price_value") if best_item else None,
            "selected_shipping_cost_value": (
                best_item.get("shipping_cost_value") if best_item else None
            ),
            "selected_total_price": best_item.get("total_price") if best_item else None,
            "currency": best_item.get("currency") if best_item else None,
            "shipping_currency": best_item.get("shipping_currency") if best_item else None,
            "selected_item_web_url": best_item.get("item_web_url") if best_item else None,
        }


def main() -> None:
    """
    Simple standalone test for this module.
    """
    sample_items = [
        {
            "itemId": "1",
            "title": "Test Card A",
            "price": {"value": "2.50", "currency": "GBP"},
            "shippingOptions": [{"shippingCost": {"value": "1.00", "currency": "GBP"}}],
            "itemWebUrl": "https://example.com/a",
            "condition": "New",
        },
        {
            "itemId": "2",
            "title": "Test Card B",
            "price": {"value": "1.80", "currency": "GBP"},
            "shippingOptions": [{"shippingCost": {"value": "0.50", "currency": "GBP"}}],
            "itemWebUrl": "https://example.com/b",
            "condition": "Used",
        },
    ]

    keyword = EbayTransformer.build_keyword("Caterpie", "JTG", "1")
    normalized = EbayTransformer.normalize_items(sample_items)
    best_item = EbayTransformer.pick_best_item(normalized)
    record = EbayTransformer().transform_search_results(
        {"itemSummaries": sample_items},
        {
            "card_id": 1,
            "lang": "en",
            "set_code": "JTG",
            "card_code": "1",
            "card_name": "Caterpie",
            "search_keyword": keyword,
            "marketplace_id": "EBAY_GB",
            "search_limit": 50,
            "source_url": "https://api.ebay.com/buy/browse/v1/item_summary/search",
            "final_url": "https://api.ebay.com/buy/browse/v1/item_summary/search",
            "raw_json_path": "Data/raw/ebay/search_json/2026/03/29/en_JTG_1.json",
        },
    )

    print(f"Keyword: {keyword}")
    print(f"Normalized items: {len(normalized)}")
    print(f"Best item: {best_item}")
    print(f"Record: {record}")


if __name__ == "__main__":
    main()
