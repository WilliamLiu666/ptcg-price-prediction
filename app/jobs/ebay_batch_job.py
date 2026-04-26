from __future__ import annotations

import argparse
import os
from contextlib import closing
from datetime import datetime, timezone

from app.config import get_ebay_credentials, parse_bool_env
from app.extract.ebay_extractor import EbayAuth, EbayExtractor
from app.load.ebay_loader import EbayLoader
from app.load.ebay_staging_loader import EbayStagingLoader
from app.services.ebay_price_service import EbayPriceService
from app.transform.ebay_transformer import EbayTransformer
from app.utils.postgres_db import connect_postgres, dict_cursor
from app.utils.postgres_schema import ensure_app_schema


TARGET_LANG = "en"
TARGET_MARKETPLACE_ID = "EBAY_GB"


def resolve_extract_date(cli_value: str | None) -> str:
    """
    Partition date for raw JSON + staging (YYYY-MM-DD).

    Priority: CLI ``--extract-date`` > env ``EXTRACT_DATE`` > today UTC.
    """
    if cli_value:
        return datetime.fromisoformat(cli_value).date().isoformat()

    env = os.environ.get("EXTRACT_DATE", "").strip()
    if env:
        return datetime.fromisoformat(env).date().isoformat()

    return datetime.now(timezone.utc).date().isoformat()


class EbayBatchJob:
    """
    Batch job for eBay price backfill.

    Responsibilities:
    - Read pending cards from database
    - Build search keywords
    - Call service layer for each card
    - Print simple batch progress

    This class does NOT handle:
    - Detailed transformation logic
    - Direct HTTP request construction
    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        sandbox: bool = False,
        schema_name: str | None = None,
    ) -> None:
        """
        Initialize the batch job.

        Args:
            client_id:
                eBay application client ID.
            client_secret:
                eBay application client secret.
            sandbox:
                Whether to use sandbox environment.
            schema_name:
                Optional PostgreSQL schema/search_path override.
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.sandbox = sandbox
        self.schema_name = schema_name

    def load_pending_cards(self, extract_date: str | None = None) -> list[dict]:
        """
        Load all eligible English cards for the eBay batch.

        Returns:
            List of card rows from cards_index when available, otherwise
            from prices_limitless.
        """
        with closing(connect_postgres(schema_name=self.schema_name)) as conn:
            ensure_app_schema(conn)
            conn.commit()

            with dict_cursor(conn) as cur:
                cur.execute(
                    """
                    SELECT card_id, lang, set_code, card_code, card_name
                    FROM cards_index
                    WHERE lang = %s
                      AND card_name IS NOT NULL
                      AND BTRIM(card_name) <> ''
                    ORDER BY set_code, card_code
                    """,
                    (TARGET_LANG,),
                )
                rows = cur.fetchall()

                if rows:
                    return list(rows)

                cur.execute(
                    """
                    SELECT card_id, lang, set_code, card_code, card_name
                    FROM prices_limitless
                    WHERE lang = %s
                      AND card_name IS NOT NULL
                      AND BTRIM(card_name) <> ''
                    ORDER BY set_code, card_code
                    """,
                    (TARGET_LANG,),
                )
                return list(cur.fetchall())

    def run(
        self,
        extract_date: str | None = None,
        overwrite_card_index: bool = False,
    ) -> None:
        """
        Run the batch job.
        """
        normalized_extract_date = resolve_extract_date(extract_date)

        auth = EbayAuth(
            client_id=self.client_id,
            client_secret=self.client_secret,
            sandbox=self.sandbox,
        )
        extractor = EbayExtractor(
            auth=auth,
            sandbox=self.sandbox,
            marketplace_id=TARGET_MARKETPLACE_ID,
        )
        transformer = EbayTransformer()
        loader = EbayLoader(schema_name=self.schema_name)
        staging_loader = EbayStagingLoader()
        service = EbayPriceService(
            extractor=extractor,
            transformer=transformer,
            loader=loader,
            staging_loader=staging_loader,
            raw_json_base_dir="Data/raw/ebay/search_json",
        )

        cards = self.load_pending_cards(extract_date=normalized_extract_date)
        print(f"[batch] extract_date={normalized_extract_date} (staging + raw JSON partition)")
        print(f"[batch] overwrite_card_index={overwrite_card_index}")
        print(f"[batch] target_lang={TARGET_LANG}")
        print(f"[batch] marketplace_id={TARGET_MARKETPLACE_ID}")
        print(f"[batch] total cards={len(cards)}")

        for index, row in enumerate(cards, start=1):
            card_id = row["card_id"]
            lang = row["lang"]
            set_code = row["set_code"]
            card_code = row["card_code"]
            card_name = row["card_name"]
            keyword = EbayTransformer.build_keyword(card_name, set_code, card_code)
            print(f"\n[{index}/{len(cards)}] Searching: {keyword}")

            try:
                result = service.fetch_and_save(
                    keyword=keyword,
                    lang=lang,
                    set_code=set_code,
                    card_code=card_code,
                    card_id=card_id,
                    card_name=card_name,
                    search_limit=50,
                    extract_date=normalized_extract_date,
                    overwrite_card_index=overwrite_card_index,
                )

                if result.get("selected_total_price") is not None:
                    print(
                        f"Saved lowest price: {result.get('selected_total_price')} | "
                        f"title: {result.get('selected_title')} | "
                        f"matches={result.get('normalized_item_count')}"
                    )
                else:
                    print("No matching items found.")

            except FileNotFoundError:
                raise
            except Exception as exc:
                print(f"Error processing {keyword}: {exc}")


EbayPriceJob = EbayBatchJob


def main() -> None:
    """
    CLI entry point for the eBay batch.
    """
    parser = argparse.ArgumentParser(
        description="eBay batch: raw JSON + staging parquet + PostgreSQL price update."
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
    Run the eBay batch with configured credentials.
    """
    client_id, client_secret = get_ebay_credentials()
    sandbox = parse_bool_env("EBAY_SANDBOX", default=False)

    job = EbayBatchJob(
        client_id=client_id,
        client_secret=client_secret,
        sandbox=sandbox,
    )
    job.run(
        extract_date=extract_date,
        overwrite_card_index=overwrite_card_index,
    )


if __name__ == "__main__":
    main()
