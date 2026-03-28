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

            price_value = float(price["value"]) if price.get("value") else None
            if price_value is None:
                continue

            shipping_cost_value = 0.0
            shipping_cost_currency = None

            if shipping_options:
                shipping_cost = shipping_options[0].get("shippingCost", {})
                if shipping_cost.get("value"):
                    shipping_cost_value = float(shipping_cost["value"])
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

    print(f"Keyword: {keyword}")
    print(f"Normalized items: {len(normalized)}")
    print(f"Best item: {best_item}")


if __name__ == "__main__":
    main()