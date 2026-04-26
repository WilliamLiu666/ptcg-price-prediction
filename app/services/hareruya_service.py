from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.extract.hareruya_extractor import HareruyaExtractor
from app.load.hareruya_loader import HareruyaLoader
from app.transform.hareruya_transformer import HareruyaTransformer
from app.load.hareruya_staging_loader import HareruyaStagingLoader
from app.utils.extract_policy import resolve_extract_mode


class HareruyaService:
    """
    Service layer for Hareruya.

    Responsibilities:
    - Orchestrate extract -> transform -> load (staging)
    - Reuse local HTML / JSON cache for the same extract date when available
    - Keep the business flow simple and readable
    """

    def __init__(
        self,
        extractor: HareruyaExtractor,
        transformer: HareruyaTransformer,
        loader: HareruyaStagingLoader,
        db_loader: HareruyaLoader | None = None,
        raw_base_dir: str | Path = "Data/raw/hareruya/collections",
    ) -> None:
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader
        self.db_loader = db_loader
        self.raw_base_dir = Path(raw_base_dir)

    @staticmethod
    def _normalize_extract_date(extract_date: str | None) -> str:
        """
        Normalize extract_date to YYYY-MM-DD.
        """
        if extract_date is None:
            return datetime.now(timezone.utc).date().isoformat()
        return datetime.fromisoformat(extract_date).date().isoformat()

    @staticmethod
    def _build_html_filename(
        collection_id: str,
        filename: str | None = None,
    ) -> str:
        """
        Build html filename.
        """
        if filename:
            base = filename[:-5] if filename.endswith(".html") else filename
            return f"{base}.html"
        return f"collection_{collection_id}_page.html"

    @staticmethod
    def _build_json_filename(
        collection_id: str,
        filename: str | None = None,
    ) -> str:
        """
        Build json filename.
        """
        if filename:
            base = filename[:-5] if filename.endswith(".json") else filename
            return f"{base}.json"
        return f"collection_{collection_id}_products.json"

    def _build_html_path(
        self,
        collection_id: str,
        extract_date: str,
        html_filename: str | None = None,
        html_save_to: str | None = None,
    ) -> Path:
        """
        Resolve local html path.

        Example:
            Data/raw/hareruya/collections/2026/03/30/collection_706_page.html
        """
        if html_save_to:
            return Path(html_save_to)

        dt = datetime.fromisoformat(extract_date).date()
        filename = self._build_html_filename(
            collection_id=collection_id,
            filename=html_filename,
        )

        return (
            self.raw_base_dir
            / f"{dt.year:04d}"
            / f"{dt.month:02d}"
            / f"{dt.day:02d}"
            / filename
        )

    def _build_json_path(
        self,
        collection_id: str,
        extract_date: str,
        json_filename: str | None = None,
        json_save_to: str | None = None,
    ) -> Path:
        """
        Resolve local json path.

        Example:
            Data/raw/hareruya/collections/2026/03/30/collection_706_products.json
        """
        if json_save_to:
            return Path(json_save_to)

        dt = datetime.fromisoformat(extract_date).date()
        filename = self._build_json_filename(
            collection_id=collection_id,
            filename=json_filename,
        )

        return (
            self.raw_base_dir
            / f"{dt.year:04d}"
            / f"{dt.month:02d}"
            / f"{dt.day:02d}"
            / filename
        )

    @staticmethod
    def _read_local_html_if_exists(path: Path) -> str | None:
        """
        Read local html if it exists.
        """
        if not path.exists():
            return None
        return path.read_text(encoding="utf-8")

    @staticmethod
    def _read_local_json_if_exists(path: Path) -> dict[str, Any] | None:
        """
        Read local json if it exists.
        """
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    @staticmethod
    def _extract_collection_id_from_url(url: str) -> str:
        """
        Extract collection id from url.

        Example:
            https://www.hareruya2.com/collections/706 -> 706
        """
        parts = url.rstrip("/").split("/")
        if not parts:
            raise ValueError(f"Invalid collection url: {url}")
        collection_id = parts[-1].strip()
        if not collection_id:
            raise ValueError(f"Could not extract collection_id from url: {url}")
        return collection_id

    def run_one_collection(
        self,
        collection_url: str,
        html_filename: str | None = None,
        json_filename: str | None = None,
        html_save_to: str | None = None,
        json_save_to: str | None = None,
        extract_date: str | None = None,
        overwrite_card_index: bool = False,
    ) -> list[dict[str, Any]]:
        """
        Run the full ETL flow for a single Hareruya collection page.

        Flow:
        1. Check whether local HTML and JSON for the same date already exist
        2. If yes, reuse local files
        3. If no, fetch and save locally
        4. Transform payload into structured records
        5. Write each record to staging parquet

        Args:
            collection_url:
                Hareruya collection url.
            html_filename:
                Optional local html filename.
            json_filename:
                Optional local json filename.
            html_save_to:
                Optional explicit local html path.
            json_save_to:
                Optional explicit local json path.
            extract_date:
                Business extract date in YYYY-MM-DD.
            overwrite_card_index:
                Whether to overwrite existing card_index parquet.
                Default = False.

        Returns:
            List of transformed product records.
        """
        normalized_extract_date, update_current = resolve_extract_mode(extract_date)
        collection_id = self._extract_collection_id_from_url(collection_url)

        html_path = self._build_html_path(
            collection_id=collection_id,
            extract_date=normalized_extract_date,
            html_filename=html_filename,
            html_save_to=html_save_to,
        )
        json_path = self._build_json_path(
            collection_id=collection_id,
            extract_date=normalized_extract_date,
            json_filename=json_filename,
            json_save_to=json_save_to,
        )

        html = self._read_local_html_if_exists(html_path)
        payload = self._read_local_json_if_exists(json_path)

        context: dict[str, str | None] = {
            "collection_id": collection_id,
            "source_url": collection_url,
            "final_url": collection_url,
            "source": "hareruya",
            "html_path": str(html_path),
            "json_path": str(json_path),
        }

        if html is None:
            if not update_current:
                raise FileNotFoundError(
                    "historical replay requires cached Hareruya raw HTML: "
                    f"{html_path}"
                )
            html, html_context = self.extractor.fetch_html(
                url=collection_url,
                filename=html_path.stem,
                save_to=str(html_path),
            )
            if html_context and isinstance(html_context, dict):
                context.update(html_context)  # type: ignore
            print(f"[extract] fetched html from web: {html_path}")
        else:
            print(f"[extract] reused local html: {html_path}")

        if payload is None:
            if not update_current:
                raise FileNotFoundError(
                    "historical replay requires cached Hareruya raw JSON: "
                    f"{json_path}"
                )
            payload, json_context = self.extractor.fetch_products_json(
                collection_url=collection_url,
                filename=json_path.stem,
                save_to=str(json_path),
            )
            if json_context and isinstance(json_context, dict):
                context.update(json_context)  # type: ignore
            print(f"[extract] fetched json from web: {json_path}")
        else:
            print(f"[extract] reused local json: {json_path}")

        records = self.transformer.transform_products(payload, context)

        for record in records:
            self.loader.write_hareruya_record(
                record,
                extract_date=normalized_extract_date,
                overwrite_card_index=overwrite_card_index,
            )

        if self.db_loader is not None:
            self.db_loader.save_product_prices(
                records,
                observed_date=normalized_extract_date,
                update_current=update_current,
            )

        return records


if __name__ == "__main__":
    extractor = HareruyaExtractor()
    transformer = HareruyaTransformer()
    loader = HareruyaStagingLoader()

    service = HareruyaService(
        extractor=extractor,
        transformer=transformer,
        loader=loader,
        raw_base_dir="Data/raw/hareruya/collections",
    )

    records = service.run_one_collection(
        collection_url="https://www.hareruya2.com/collections/706",
        extract_date="2026-03-30",
        overwrite_card_index=False,
    )

    print("ETL finished.")
    print(f"Total records: {len(records)}")
    if records:
        print(records[0])
