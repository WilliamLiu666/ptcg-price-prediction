from typing import Any
from ebay_client import EbayClient
from ebay_repo import EbayPriceRepository
from ebay_auth import EbayAuth

class EbayPriceService:
    def __init__(self, client: EbayClient, repo: EbayPriceRepository) -> None:
        self.client = client
        self.repo = repo

    def fetch_and_save(
        self,
        keyword: str,
        lang: str,
        set_code: str,
        card_code: str,
        limit: int = 20,
    ) -> dict[str, Any] | None:
        """
        1. 搜索 eBay
        2. 找到最低 total_price（price + shipping）
        3. 更新数据库 prices_limitless.ebay_price

        Returns:
            最低价 item（或 None）
        """

        items = self.client.search_lowest_price(keyword=keyword, limit=limit)

        if not items:
            print("No items found")
            self.repo.update_ebay_price(lang, set_code, card_code, None)
            return None

        # 已经排序过，直接取第一个
        best_item = items[0]
        lowest_price = best_item["total_price"]

        # ✅ 写入数据库
        updated_rows = self.repo.update_ebay_price(
            lang=lang,
            set_code=set_code,
            card_code=card_code,
            ebay_price=lowest_price,
        )

        print(f"Updated {updated_rows} rows, lowest price = {lowest_price}")

        return best_item
    
if __name__ == "__main__":

    CLIENT_ID = ""
    CLIENT_SECRET = ""

    auth = EbayAuth(CLIENT_ID, CLIENT_SECRET, sandbox=False)
    client = EbayClient(auth)
    repo = EbayPriceRepository("ptcg.sqlite")

    service = EbayPriceService(client, repo)

    best_item = service.fetch_and_save(
        keyword="pokemon JTG 002",
        lang="en",
        set_code="JTG",
        card_code="2",
        limit=5,
    )

    if best_item:
        print("\nBest Item:")
        print("Title:", best_item["title"])
        print("Total:", best_item["total_price"], best_item["currency"])
        print("URL:", best_item["item_web_url"])