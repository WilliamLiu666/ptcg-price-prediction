from __future__ import annotations

import sqlite3

from app.extract.limitless_extractor import LimitlessExtractor
from app.transform.limitless_transformer import LimitlessTransformer
from app.load.limitless_loader import LimitlessLoader
from app.services.limitless_service import LimitlessService


DB_PATH = "ptcg.sqlite"


def load_series_records(db_path: str):
    """
    Load all series records from DB.

    Returns:
        list of rows with:
        - series_code
        - lang
        - size
    """
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
    """
    Batch job:
    Iterate through all series and fetch all cards.
    """

    # ---------- 初始化 ETL ----------
    extractor = LimitlessExtractor()
    transformer = LimitlessTransformer()
    loader = LimitlessLoader(db_path=DB_PATH)

    service = LimitlessService(
        extractor=extractor,
        transformer=transformer,
        loader=loader,
    )

    # ---------- 读取 series ----------
    rows = load_series_records(DB_PATH)

    # ---------- 批量处理 ----------
    for r in rows:
        series_code = r["series_code"]
        lang = r["lang"]
        size = int(r["size"])

        print(f"\n[Series] {series_code} lang={lang} size={size}")

        for card_code in range(1, size + 1):
            try:
                record = service.run_one(
                    lang=lang,
                    set_code=series_code,
                    card_code=str(card_code),
                    filename=f"{lang}_{series_code}_{card_code}",
                )

                if lang == "en":
                    print(
                        f"[OK] {series_code}/{card_code} "
                        f"name={record['card_name']!r} "
                        f"rarity={record['rarity']!r} "
                        f"usd={record['usd_price']} eur={record['eur_price']}"
                    )
                else:
                    print(
                        f"[OK] {series_code}/{card_code} "
                        f"name={record['card_name']!r} "
                        f"rarity={record['rarity']!r}"
                    )

            except Exception as e:
                print(f"[SKIP] {series_code}/{card_code} failed: {e}")
                continue


if __name__ == "__main__":
    main()