from __future__ import annotations

import argparse
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from app.extract.limitless_extractor import LimitlessExtractor
from app.transform.limitless_transformer import LimitlessTransformer
from app.load.limitless_staging_loader import LimitlessStagingLoader
from app.services.limitless_service import LimitlessService


DB_PATH = "ptcg.sqlite"
RAW_SET_HTML_BASE_DIR = "Data/raw/limitless/sets_html"


def resolve_extract_date(cli_value: str | None) -> str:
    """
    Partition date for raw HTML + staging (YYYY-MM-DD).

    Priority: CLI ``--extract-date`` > env ``EXTRACT_DATE`` > today UTC.
    """
    if cli_value:
        return datetime.fromisoformat(cli_value).date().isoformat()

    env = os.environ.get("EXTRACT_DATE", "").strip()
    if env:
        return datetime.fromisoformat(env).date().isoformat()

    return datetime.now(timezone.utc).date().isoformat()


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
        rows = conn.execute(
            """
            SELECT series_code, lang, size
            FROM series_limitless
            WHERE size IS NOT NULL AND size > 0
            ORDER BY series_code, lang
            """
        ).fetchall()

    return rows


def build_set_html_path(
    raw_set_html_base_dir: str | Path,
    extract_date: str,
    lang: str,
    set_code: str,
) -> Path:
    dt = datetime.fromisoformat(extract_date).date()
    return (
        Path(raw_set_html_base_dir)
        / f"{dt.year:04d}"
        / f"{dt.month:02d}"
        / f"{dt.day:02d}"
        / f"{lang}_{set_code}.html"
    )


def discover_card_codes(
    extractor: LimitlessExtractor,
    transformer: LimitlessTransformer,
    *,
    lang: str,
    set_code: str,
    size: int,
    extract_date: str,
    raw_set_html_base_dir: str | Path = RAW_SET_HTML_BASE_DIR,
) -> list[str]:
    """
    Discover card codes from the Limitless set listing page.

    Falls back to the historical ``1..size`` range when discovery fails or
    returns no card links.
    """
    set_html_path = build_set_html_path(
        raw_set_html_base_dir=raw_set_html_base_dir,
        extract_date=extract_date,
        lang=lang,
        set_code=set_code,
    )

    try:
        if set_html_path.exists():
            set_html = set_html_path.read_text(encoding="utf-8")
            print(f"[discover] reused local set html: {set_html_path}")
        else:
            set_html, _ = extractor.fetch_set_html(
                lang=lang,
                set_code=set_code,
                filename=set_html_path.stem,
                save_to=str(set_html_path),
            )
            print(f"[discover] fetched set html from web: {set_html_path}")

        href_rows = transformer.extract_hrefs(
            set_html,
            prefix="/cards/",
            default_lang=lang,
        )

        seen_codes: set[str] = set()
        card_codes: list[str] = []
        for row in href_rows:
            row_lang = str(row.get("lang", "")).strip()
            row_set_code = str(row.get("set_code", "")).strip()
            card_code = str(row.get("card_code", "")).strip()
            if not card_code or row_lang != lang or row_set_code != set_code:
                continue
            if card_code in seen_codes:
                continue
            seen_codes.add(card_code)
            card_codes.append(card_code)

        if card_codes:
            if size > 0 and len(card_codes) != size:
                print(
                    f"[discover] size mismatch for {lang}/{set_code}: "
                    f"db_size={size} discovered={len(card_codes)}"
                )
            return card_codes

        print(
            f"[discover] no card links found for {lang}/{set_code}; "
            f"falling back to db size"
        )
    except Exception as exc:
        print(
            f"[discover] failed for {lang}/{set_code}: {exc}; "
            f"falling back to db size"
        )

    return [str(card_code) for card_code in range(1, size + 1)]


def main() -> None:
    """
    Batch job:
    Iterate through all series and fetch all cards.

    Flow:
    1. Read series list from sqlite
    2. For each card:
       - reuse local raw html if exists for the given date
       - otherwise fetch from web and save raw html
       - transform record
       - write to staging parquet
    """
    parser = argparse.ArgumentParser(
        description="Limitless batch: raw HTML + staging parquet."
    )
    parser.add_argument(
        "--extract-date",
        default=None,
        help="Staging/raw partition date YYYY-MM-DD (default: EXTRACT_DATE env or today UTC)",
    )
    parser.add_argument(
        "--overwrite-card-index",
        action="store_true",
        help="Overwrite existing card_index parquet files",
    )
    args = parser.parse_args()

    extract_date = resolve_extract_date(args.extract_date)

    extractor = LimitlessExtractor()
    transformer = LimitlessTransformer()
    loader = LimitlessStagingLoader()

    service = LimitlessService(
        extractor=extractor,
        transformer=transformer,
        loader=loader,
        raw_html_base_dir="Data/raw/limitless/cards_html",
    )

    rows = load_series_records(DB_PATH)
    print(f"[batch] extract_date={extract_date} (staging + raw HTML partition)")
    print(f"[batch] overwrite_card_index={args.overwrite_card_index}")

    for r in rows:
        series_code = r["series_code"]
        lang = r["lang"]
        size = int(r["size"])
        card_codes = discover_card_codes(
            extractor=extractor,
            transformer=transformer,
            lang=lang,
            set_code=series_code,
            size=size,
            extract_date=extract_date,
        )

        print(
            f"\n[Series] {series_code} lang={lang} size={size} "
            f"card_codes={len(card_codes)}"
        )

        for card_code_str in card_codes:

            try:
                record = service.run_one(
                    lang=lang,
                    set_code=series_code,
                    card_code=card_code_str,
                    filename=f"{lang}_{series_code}_{card_code_str}",
                    extract_date=extract_date,
                    overwrite_card_index=args.overwrite_card_index,
                )

                if lang == "en":
                    print(
                        f"[OK] {series_code}/{card_code_str} "
                        f"name={record.get('card_name')!r} "
                        f"rarity={record.get('rarity')!r} "
                        f"usd={record.get('usd_price')} "
                        f"eur={record.get('eur_price')}"
                    )
                else:
                    print(
                        f"[OK] {series_code}/{card_code_str} "
                        f"name={record.get('card_name')!r} "
                        f"rarity={record.get('rarity')!r}"
                    )

            except Exception as e:
                print(f"[SKIP] {series_code}/{card_code_str} failed: {e}")
                continue


if __name__ == "__main__":
    main()
