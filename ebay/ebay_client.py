import requests
from typing import Any
from ebay_auth import EbayAuth


class EbayClient:
    def __init__(self, auth: EbayAuth, sandbox: bool = False) -> None:
        self.auth = auth
        self.sandbox = sandbox

    @property
    def base_url(self) -> str:
        if self.sandbox:
            return "https://api.sandbox.ebay.com"
        return "https://api.ebay.com"

    def search_lowest_price(self, keyword: str, limit: int = 10) -> list[dict[str, Any]]:
        """
        返回 total_price（price + shipping）最低的前 N 条
        """
        token = self.auth.get_access_token()

        url = f"{self.base_url}/buy/browse/v1/item_summary/search"

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "X-EBAY-C-MARKETPLACE-ID": "EBAY_GB",
        }

        # ⚠️ 多拿一点数据再自己排序
        params = {
            "q": keyword,
            "limit": 50,  # 关键：不要只拿10条
        }

        resp = requests.get(url, headers=headers, params=params, timeout=30)

        if resp.status_code != 200:
            print("eBay API error:", resp.text)

        resp.raise_for_status()

        data = resp.json()
        items = data.get("itemSummaries", [])

        results: list[dict[str, Any]] = []

        for item in items:
            price = item.get("price", {})
            shipping_options = item.get("shippingOptions", [])

            price_value = float(price["value"]) if price.get("value") else None

            shipping_cost_value = 0.0
            shipping_cost_currency = None

            if shipping_options:
                shipping_cost = shipping_options[0].get("shippingCost", {})
                if shipping_cost.get("value"):
                    shipping_cost_value = float(shipping_cost["value"])
                shipping_cost_currency = shipping_cost.get("currency")

            # ✅ total price
            if price_value is not None:
                total_price = price_value + shipping_cost_value
            else:
                continue  # 没价格直接跳过

            results.append({
                "item_id": item.get("itemId"),
                "title": item.get("title"),
                "price_value": price_value,
                "shipping_cost_value": shipping_cost_value,
                "total_price": total_price,
                "currency": price.get("currency"),
                "shipping_currency": shipping_cost_currency,
                "item_web_url": item.get("itemWebUrl"),
                "condition": item.get("condition"),
            })

        # ✅ 核心：按 total_price 排序
        results.sort(key=lambda x: x["total_price"])

        # 返回最低的 N 条
        return results[:limit]


if __name__ == "__main__":
    CLIENT_ID = ""
    CLIENT_SECRET = ""

    auth = EbayAuth(CLIENT_ID, CLIENT_SECRET, sandbox=False)
    client = EbayClient(auth)

    print("Fetching items from eBay...")

    items = client.search_lowest_price("pokemon Caterpie JTG 001", limit=5)

    print(f"Returned {len(items)} items\n")

    for i, item in enumerate(items, start=1):
        print(f"Item {i}")
        print("Title:", item["title"])
        print("Price:", item["price_value"], item["currency"])
        print("Shipping:", item["shipping_cost_value"], item["shipping_currency"])
        print("Total:", item["total_price"], item["currency"])
        print("Condition:", item["condition"])
        print("URL:", item["item_web_url"])
        print()