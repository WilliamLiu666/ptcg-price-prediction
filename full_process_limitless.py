import sqlite3
from limitlessFetcher import LimitlessFetcher

DB_PATH = "ptcg.sqlite"

fetcher = LimitlessFetcher(
    html_dir="Limitless",
    db_path=DB_PATH
)

def load_series_records(db_path: str):
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT series_code, lang, size
            FROM series_limitless
            WHERE size IS NOT NULL AND size > 0
            ORDER BY series_code, lang
        """).fetchall()
    return rows

def main():
    rows = load_series_records(DB_PATH)

    for r in rows:
        series_code = r["series_code"]
        lang = r["lang"]
        size = int(r["size"])

        print(f"\n[Series] {series_code} lang={lang} size={size}")

        for card_code in range(1, size + 1):
            try:
                html = fetcher.fetch_html(
                    lang=lang,
                    set_code=series_code,
                    card_code=str(card_code),
                    filename=f"{series_code}_{card_code}_{lang}"
                )
            except Exception as e:
                print(f"[SKIP] {series_code}/{card_code} fetch failed: {e}")
                continue

            rarity = fetcher.extract_rarity(html)

            fetcher.save_card_index()

            print(f"[OK] {series_code}/{card_code} rarity={rarity!r}")

if __name__ == "__main__":
    main()
