import sqlite3
import time
from pathlib import Path

from ebay_auth import EbayAuth
from ebay_client import EbayClient
from ebay_repo import EbayPriceRepository
from ebay_service import EbayPriceService


DB_PATH = Path("ptcg.sqlite")


def build_keyword(card_name: str, set_code: str, card_code: str) -> str:
    padded_code = card_code.strip().zfill(3)

    parts = [
        card_name.strip(),
        set_code.strip(),
        padded_code,
    ]

    return " ".join(p for p in parts if p)


def load_cards(db_path: str | Path) -> list[tuple[str, str, str, str]]:
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT lang, set_code, card_code, card_name
            FROM prices_limitless
            WHERE card_name IS NOT NULL
              AND TRIM(card_name) <> ''
              AND ebay_price IS NULL
            """
        )
        return cur.fetchall()


def main() -> None:
    sandbox = False 
    auth = EbayAuth(client_id="", client_secret="", sandbox=sandbox)
    client = EbayClient(auth=auth, sandbox=sandbox)
    repo = EbayPriceRepository(db_path=DB_PATH)
    service = EbayPriceService(client=client, repo=repo)

    cards = load_cards(DB_PATH)
    print(f"Found {len(cards)} rows")

    for idx, (lang, set_code, card_code, card_name) in enumerate(cards, start=1):
        keyword = build_keyword(card_name, set_code, card_code)
        print(f"\n[{idx}/{len(cards)}] Searching: {keyword}")

        try:
            result = service.fetch_and_save(
                keyword=keyword,
                lang=lang,
                set_code=set_code,
                card_code=card_code,
                limit=5,
            )

            if result:
                print(
                    f"Saved lowest price: {result.get('total_price')} | "
                    f"title: {result.get('title')}"
                )
            else:
                print("No matching items found.")

        except Exception as e:
            print(f"Error processing {keyword}: {e}")




if __name__ == "__main__":
    main()