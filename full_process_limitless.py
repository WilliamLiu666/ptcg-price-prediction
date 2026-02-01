from limitlessFetcher import LimitlessFetcher

fetcher = LimitlessFetcher(
    html_dir="Limitless",
    db_path="ptcg.sqlite"
)

for card_code in range(1, 173):  # BLK has 172 cards
    try:
        html = fetcher.fetch_html(
            lang="en",
            set_code="BLK",
            card_code=str(card_code)
        )
    except Exception as e:
        print(f"[SKIP] BLK/{card_code} fetch failed: {e}")
        continue

    # 1️⃣ 提取 rarity
    rarity = fetcher.extract_rarity(html)


    # 3️⃣ 写库（你已有的函数）
    fetcher.save_card_index()

    print(
        f"[OK] BLK/{card_code} "
        f"rarity={rarity!r} "
    )

