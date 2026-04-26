from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.config import get_ebay_credentials, parse_bool_env
from app.extract.ebay_extractor import EbayExtractor
from app.load.ebay_loader import EbayLoader
from app.load.ebay_staging_loader import EbayStagingLoader
from app.transform.ebay_transformer import EbayTransformer
from app.utils.extract_policy import resolve_extract_mode


class EbayPriceService:
    """
    Service layer for eBay price pipeline.

    Responsibilities:
    - Orchestrate extract, transform, and load steps
    - Apply business rule for best price selection
    - Persist selected result into database

    This class does NOT handle:
    - Batch job iteration
    - Database schema creation
    """

    def __init__(
        self,
        extractor: EbayExtractor,
        transformer: EbayTransformer,
        loader: EbayLoader,
        staging_loader: EbayStagingLoader,
        raw_json_base_dir: str | Path = "Data/raw/ebay/search_json",
    ) -> None:
        """
        Initialize the service.

        Args:
            extractor:
                Extract layer instance.
            transformer:
                Transform layer instance.
            loader:
                SQLite load layer instance.
            staging_loader:
                Staging parquet loader.
        """
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader
        self.staging_loader = staging_loader
        self.raw_json_base_dir = Path(raw_json_base_dir)

    @staticmethod
    def _normalize_extract_date(extract_date: str | None) -> str:
        if extract_date is None:
            return datetime.now(timezone.utc).date().isoformat()
        return datetime.fromisoformat(extract_date).date().isoformat()

    @staticmethod
    def _build_json_filename(
        lang: str,
        set_code: str,
        card_code: str,
        filename: str | None = None,
    ) -> str:
        if filename:
            return filename if filename.endswith(".json") else f"{filename}.json"
        return f"{lang}_{set_code}_{card_code}.json"

    def _build_json_path(
        self,
        lang: str,
        set_code: str,
        card_code: str,
        extract_date: str,
        filename: str | None = None,
        save_to: str | None = None,
    ) -> Path:
        if save_to:
            return Path(save_to)

        dt = datetime.fromisoformat(extract_date).date()
        json_filename = self._build_json_filename(
            lang=lang,
            set_code=set_code,
            card_code=card_code,
            filename=filename,
        )

        return (
            self.raw_json_base_dir
            / f"{dt.year:04d}"
            / f"{dt.month:02d}"
            / f"{dt.day:02d}"
            / json_filename
        )

    @staticmethod
    def _read_local_json_if_exists(path: Path) -> dict[str, Any] | None:
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    def run_one(
        self,
        keyword: str,
        lang: str,
        set_code: str,
        card_code: str,
        card_id: int | str | None = None,
        card_name: str | None = None,
        search_limit: int = 50,
        filename: str | None = None,
        save_to: str | None = None,
        extract_date: str | None = None,
        overwrite_card_index: bool = False,
        marketplace_id: str | None = None,
    ) -> dict[str, Any]:
        """
        Run the full eBay ETL flow for one card.

        Flow:
        1. Reuse local raw JSON for the same extract date when available
        2. Otherwise fetch raw search JSON and save locally
        3. Transform the selected eBay result into a normalized record
        4. Write staging parquet slices
        5. Update the SQLite `prices_limitless.ebay_price` value
        """
        normalized_extract_date, update_current = resolve_extract_mode(extract_date)
        resolved_marketplace_id = marketplace_id or self.extractor.marketplace_id

        json_path = self._build_json_path(
            lang=lang,
            set_code=set_code,
            card_code=card_code,
            extract_date=normalized_extract_date,
            filename=filename,
            save_to=save_to,
        )

        payload = self._read_local_json_if_exists(json_path)
        context: dict[str, Any] = {
            "source": "ebay",
            "card_id": card_id,
            "lang": lang,
            "set_code": set_code,
            "card_code": card_code,
            "card_name": card_name,
            "search_keyword": keyword,
            "marketplace_id": resolved_marketplace_id,
            "search_limit": search_limit,
            "source_url": self.extractor.build_search_url(keyword=keyword, limit=search_limit),
            "final_url": self.extractor.build_search_url(keyword=keyword, limit=search_limit),
            "raw_json_path": str(json_path),
        }

        if payload is None:
            if not update_current:
                raise FileNotFoundError(
                    "historical replay requires cached eBay raw JSON: "
                    f"{json_path}"
                )
            payload, fetch_context = self.extractor.fetch_search_payload(
                keyword=keyword,
                limit=search_limit,
                filename=json_path.stem,
                save_to=str(json_path),
                marketplace_id=resolved_marketplace_id,
            )
            if fetch_context and isinstance(fetch_context, dict):
                context.update(fetch_context)
            print(f"[extract] fetched json from web: {json_path}")
        else:
            print(f"[extract] reused local json: {json_path}")

        record = self.transformer.transform_search_results(payload, context)

        self.staging_loader.write_ebay_record(
            record,
            extract_date=normalized_extract_date,
            overwrite_card_index=overwrite_card_index,
        )

        self.loader.update_ebay_price(
            lang=lang,
            set_code=set_code,
            card_code=card_code,
            ebay_price=record.get("selected_total_price"),
            card_id=card_id,
            card_name=card_name,
            currency=record.get("currency"),
            condition=record.get("selected_condition"),
            marketplace_id=resolved_marketplace_id,
            selected_item_id=record.get("selected_item_id"),
            selected_title=record.get("selected_title"),
            selected_item_web_url=record.get("selected_item_web_url"),
            observed_date=normalized_extract_date,
            update_current=update_current,
        )

        return record

    def fetch_and_save(
        self,
        keyword: str,
        lang: str,
        set_code: str,
        card_code: str,
        card_id: int | str | None = None,
        card_name: str | None = None,
        search_limit: int = 50,
        extract_date: str | None = None,
        overwrite_card_index: bool = False,
    ) -> dict[str, Any]:
        """
        Backward-compatible wrapper around ``run_one``.
        """
        return self.run_one(
            keyword=keyword,
            lang=lang,
            set_code=set_code,
            card_code=card_code,
            card_id=card_id,
            card_name=card_name,
            search_limit=search_limit,
            extract_date=extract_date,
            overwrite_card_index=overwrite_card_index,
        )


def main() -> None:
    """
    Simple standalone test for this module.
    """
    from app.extract.ebay_extractor import EbayAuth

    client_id, client_secret = get_ebay_credentials()
    sandbox = parse_bool_env("EBAY_SANDBOX", default=False)

    auth = EbayAuth(
        client_id=client_id,
        client_secret=client_secret,
        sandbox=sandbox,
    )
    extractor = EbayExtractor(auth=auth, sandbox=sandbox)
    transformer = EbayTransformer()
    loader = EbayLoader(db_path="ptcg.sqlite")
    staging_loader = EbayStagingLoader()

    service = EbayPriceService(
        extractor=extractor,
        transformer=transformer,
        loader=loader,
        staging_loader=staging_loader,
    )

    result = service.fetch_and_save(
        keyword="pokemon Caterpie JTG 001",
        lang="en",
        set_code="JTG",
        card_code="1",
        card_name="Caterpie",
        search_limit=5,
    )

    print(result)


if __name__ == "__main__":
    main()
