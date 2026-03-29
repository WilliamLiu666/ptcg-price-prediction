from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from app.data_paths import build_staging_partition_dir


class LimitlessStagingLoader:
    """
    Load layer for Limitless staging.

    Responsibilities:
    - Write normalized Limitless records into staging parquet
    - Partition output by source + dataset + extract_date
    - Use business-key-based filenames for idempotent writes

    This class does NOT handle:
    - HTTP requests
    - HTML parsing
    - database writing
    """

    def __init__(self, base_data_dir: str | Path | None = None) -> None:
        """
        Initialize the staging loader.

        Args:
            base_data_dir:
                Reserved for future extension. Currently path building is delegated
                to app.data_paths.
        """
        self.base_data_dir = Path(base_data_dir) if base_data_dir else None

    @staticmethod
    def _normalize_extract_date(dt: date | datetime | str) -> date:
        """
        Normalize input into a date object.
        """
        if isinstance(dt, datetime):
            return dt.date()
        if isinstance(dt, date):
            return dt
        return datetime.fromisoformat(dt).date()

    @staticmethod
    def _prepare_records(
        records: list[dict[str, Any]],
        extract_date: date,
        dataset: str,
    ) -> list[dict[str, Any]]:
        """
        Add common staging metadata fields to each record.
        """
        prepared: list[dict[str, Any]] = []

        for record in records:
            row = dict(record)
            row["source"] = "limitless"
            row["dataset"] = dataset
            row["extract_date"] = extract_date.isoformat()
            prepared.append(row)

        return prepared

    @staticmethod
    def _build_record_filename(record: dict[str, Any]) -> str:
        """
        Build a deterministic parquet filename from business keys.

        Expected format:
            lang_setcode_cardcode.parquet
        Example:
            en_BLK_2.parquet
        """
        lang = str(record["lang"]).strip()
        set_code = str(record["set_code"]).strip()
        card_code = str(record["card_code"]).strip()

        return f"{lang}_{set_code}_{card_code}.parquet"

    @staticmethod
    def _write_parquet(df: pd.DataFrame, out_path: Path) -> Path:
        """
        Write dataframe to parquet.
        """
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out_path, index=False)
        return out_path

    def _write_records_as_individual_files(
        self,
        records: list[dict[str, Any]],
        extract_date: date | datetime | str,
        dataset: str,
    ) -> list[Path]:
        """
        Write records into staging as one parquet file per record.

        File naming convention:
            {lang}_{set_code}_{card_code}.parquet

        Returns:
            List of written parquet paths.
        """
        partition_date = self._normalize_extract_date(extract_date)
        prepared = self._prepare_records(
            records=records,
            extract_date=partition_date,
            dataset=dataset,
        )

        if dataset == "price_events":
            now_utc = datetime.now(timezone.utc).isoformat(timespec="seconds")
            for row in prepared:
                row.setdefault("observed_at", now_utc)
                row.setdefault("observed_date", partition_date.isoformat())

        out_dir = build_staging_partition_dir(
            source="limitless",
            dataset=dataset,
            dt=partition_date,
        )

        written_paths: list[Path] = []

        for row in prepared:
            df = pd.DataFrame([row])
            filename = self._build_record_filename(row)
            out_path = out_dir / filename
            self._write_parquet(df, out_path)
            written_paths.append(out_path)

        return written_paths

    def write_cards_normalized(
        self,
        records: list[dict[str, Any]],
        extract_date: date | datetime | str,
    ) -> list[Path]:
        """
        Write Limitless normalized card records into staging.

        Target path example:
            Data/staging/limitless/cards_normalized/extract_date=2026-03-29/en_BLK_2.parquet

        Args:
            records:
                Normalized card records from transformer.
            extract_date:
                Partition date.

        Returns:
            List of written parquet paths.
        """
        return self._write_records_as_individual_files(
            records=records,
            extract_date=extract_date,
            dataset="cards_normalized",
        )

    def write_card_index(
        self,
        records: list[dict[str, Any]],
        extract_date: date | datetime | str,
    ) -> list[Path]:
        """
        Write card index style records into staging.

        Target path example:
            Data/staging/limitless/card_index/extract_date=2026-03-29/en_BLK_2.parquet
        """
        return self._write_records_as_individual_files(
            records=records,
            extract_date=extract_date,
            dataset="card_index",
        )

    def write_price_events(
        self,
        records: list[dict[str, Any]],
        extract_date: date | datetime | str,
    ) -> list[Path]:
        """
        Write price-event style records into staging.

        Target path example:
            Data/staging/limitless/price_events/extract_date=2026-03-29/en_BLK_2.parquet
        """
        return self._write_records_as_individual_files(
            records=records,
            extract_date=extract_date,
            dataset="price_events",
        )


if __name__ == "__main__":
    sample_records = [
        {
            "card_id": "124",
            "data_id": "456",
            "lang": "en",
            "set_code": "BLK",
            "card_code": "2",
            "card_name": "Sample Card",
            "rarity": "Uncommon",
            "usd_price": 1.25,
            "eur_price": 1.10,
            "card_path": "/cards/en/BLK/2",
        }
    ]

    loader = LimitlessStagingLoader()
    out_paths = loader.write_cards_normalized(
        records=sample_records,
        extract_date="2026-03-29",
    )

    for path in out_paths:
        print(f"Written to staging: {path}")