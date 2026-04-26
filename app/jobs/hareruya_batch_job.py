from __future__ import annotations

import argparse
import os
from contextlib import closing
from datetime import datetime, timezone

from app.extract.hareruya_extractor import HareruyaExtractor
from app.load.hareruya_loader import HareruyaLoader
from app.transform.hareruya_transformer import HareruyaTransformer
from app.load.hareruya_staging_loader import HareruyaStagingLoader
from app.services.hareruya_service import HareruyaService
from app.utils.postgres_db import connect_postgres, dict_cursor
from app.utils.postgres_schema import ensure_app_schema
HARERUYA_COLLECTION_BASE_URL = "https://www.hareruya2.com/collections"


def resolve_extract_date(cli_value: str | None) -> str:
    """
    Partition date for raw files + staging (YYYY-MM-DD).

    Priority: CLI ``--extract-date`` > env ``EXTRACT_DATE`` > today UTC.
    """
    if cli_value:
        return datetime.fromisoformat(cli_value).date().isoformat()

    env = os.environ.get("EXTRACT_DATE", "").strip()
    if env:
        return datetime.fromisoformat(env).date().isoformat()

    return datetime.now(timezone.utc).date().isoformat()


def load_series_records() -> list[dict]:
    """
    Load all Hareruya collection records from DB.

    Returns:
        list of rows with:
        - series_code
        - collection
    """
    with closing(connect_postgres()) as conn:
        ensure_app_schema(conn)
        conn.commit()
        with dict_cursor(conn) as cur:
            cur.execute(
                """
                SELECT series_code, collection
                FROM series_hareruya
                WHERE collection IS NOT NULL
                ORDER BY series_code
                """
            )
            return list(cur.fetchall())


def build_collection_url(collection_id: int | str) -> str:
    """
    Build Hareruya collection URL from collection id.
    """
    return f"{HARERUYA_COLLECTION_BASE_URL}/{str(collection_id).strip()}"


def main() -> None:
    """
    CLI entry point for the Hareruya batch.
    """
    parser = argparse.ArgumentParser(
        description="Hareruya batch: raw HTML/JSON + staging parquet."
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

    run_batch(
        extract_date=args.extract_date,
        overwrite_card_index=args.overwrite_card_index,
    )


def run_batch(
    extract_date: str | None = None,
    overwrite_card_index: bool = False,
) -> None:
    """
    Batch job:
    Iterate through all Hareruya collections and fetch all products.

    Flow:
    1. Read collection list from PostgreSQL
    2. For each collection:
       - reuse local raw html/json if exists for the given date
       - otherwise fetch from web and save raw files
       - transform records
       - write to staging parquet
    """
    extract_date = resolve_extract_date(extract_date)

    extractor = HareruyaExtractor()
    transformer = HareruyaTransformer()
    loader = HareruyaStagingLoader()
    db_loader = HareruyaLoader()

    service = HareruyaService(
        extractor=extractor,
        transformer=transformer,
        loader=loader,
        db_loader=db_loader,
        raw_base_dir="Data/raw/hareruya/collections",
    )

    rows = load_series_records()

    print(f"[batch] extract_date={extract_date} (staging + raw partition)")
    print(f"[batch] overwrite_card_index={overwrite_card_index}")
    print(f"[batch] total collections={len(rows)}")

    for r in rows:
        series_code = str(r["series_code"]).strip()
        collection_id = r["collection"]

        if collection_id is None:
            print(f"[SKIP] {series_code} missing collection id")
            continue

        collection_url = build_collection_url(collection_id)

        print(f"\n[Collection] series_code={series_code} collection={collection_id}")
        print(f"[Collection] url={collection_url}")

        try:
            records = service.run_one_collection(
                collection_url=collection_url,
                html_filename=f"collection_{collection_id}_page",
                json_filename=f"collection_{collection_id}_products",
                extract_date=extract_date,
                overwrite_card_index=overwrite_card_index,
            )

            product_count = len(records)
            if product_count == 0:
                print(f"[OK] {series_code} collection={collection_id} products=0")
                continue

            sample = records[0]
            print(
                f"[OK] {series_code} collection={collection_id} "
                f"products={product_count} "
                f"sample_name_jp={sample.get('card_name_jp')!r} "
                f"sample_name_en={sample.get('card_name_en')!r} "
                f"sample_rarity={sample.get('rarity')!r} "
                f"sample_price_jpy={sample.get('price_jpy')}"
            )

        except FileNotFoundError:
            raise
        except Exception as e:
            print(
                f"[SKIP] {series_code} collection={collection_id} failed: {e}"
            )
            continue


if __name__ == "__main__":
    main()
