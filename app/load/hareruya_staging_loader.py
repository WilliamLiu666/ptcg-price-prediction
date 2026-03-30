from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from app.data_paths import build_staging_partition_dir


def _coerce_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


class HareruyaStagingLoader:
    """
    Load layer for Hareruya staging.

    Responsibilities:
    - Write normalized Hareruya records into staging parquet
    - Keep cards_normalized and price_events partitioned by extract_date
    - Keep card_index in a fixed non-partitioned directory
    - Support optional overwrite control for card_index

    This class does NOT handle:
    - HTTP requests
    - HTML parsing
    - database writing
    """

    def __init__(self, data_root: str | Path | None = None) -> None:
        """
        Args:
            data_root:
                Root folder containing `staging/` (default: `Data`).
        """
        self._data_root: Path | None = Path(data_root) if data_root is not None else None

    def _base_data_root(self) -> Path:
        return self._data_root or Path("Data")

    def _partition_dir(self, source: str, dataset: str, partition_date: date) -> Path:
        return build_staging_partition_dir(
            source=source,
            dataset=dataset,
            dt=partition_date,
            data_root=self._data_root,
        )

    def _card_index_dir(self) -> Path:
        """
        Fixed directory for latest card index records.
        Example:
            Data/staging/hareruya/card_index/M2_001.parquet
        """
        return self._base_data_root() / "staging" / "hareruya" / "card_index"

    @staticmethod
    def _normalize_extract_date(dt: date | datetime | str) -> date:
        if isinstance(dt, datetime):
            return dt.date()
        if isinstance(dt, date):
            return dt
        return datetime.fromisoformat(dt).date()

    @staticmethod
    def _normalize_hareruya_record(record: dict[str, Any]) -> dict[str, Any]:
        """
        Coerce selected ID-like fields to int when possible so Parquet schemas stay stable.
        """
        row = dict(record)

        row["product_id"] = _coerce_optional_int(row.get("product_id"))
        row["variant_id"] = _coerce_optional_int(row.get("variant_id"))
        row["collection_id"] = _coerce_optional_int(row.get("collection_id"))
        row["card_number_int"] = _coerce_optional_int(row.get("card_number"))
        row["total_in_set_int"] = _coerce_optional_int(row.get("total_in_set"))
        row["zukan_number_int"] = _coerce_optional_int(row.get("zukan_number"))

        return row

    @staticmethod
    def _safe_str(value: Any, default: str = "unknown") -> str:
        if value is None:
            return default
        text = str(value).strip()
        return text if text else default

    def _build_record_filename(self, record: dict[str, Any]) -> str:
        """
        Prefer business key:
            set_code + card_number
        Fallback:
            handle or product_id
        """
        set_code = self._safe_str(record.get("set_code"), default="unknown")
        card_number = self._safe_str(record.get("card_number"), default="unknown")

        if set_code != "unknown" and card_number != "unknown":
            return f"{set_code}_{card_number}.parquet"

        handle = self._safe_str(record.get("handle"), default="")
        if handle:
            return f"{handle}.parquet"

        product_id = self._safe_str(record.get("product_id"), default="unknown")
        return f"{product_id}.parquet"

    @staticmethod
    def _write_parquet(df: pd.DataFrame, out_path: Path) -> Path:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            df.to_parquet(out_path, index=False, engine="pyarrow")
        except ImportError as exc:
            raise RuntimeError(
                "Staging Parquet writes require pyarrow (and pandas). "
                "Install with: pip install pyarrow pandas"
            ) from exc
        return out_path

    def _write_prepared_row(
        self,
        row: dict[str, Any],
        dataset: str,
        partition_date: date | None = None,
        overwrite: bool = True,
    ) -> Path:
        """
        Write one prepared row to its target parquet file.

        Rules:
        - card_index: fixed directory, no date partition
        - other datasets: date-partitioned
        - overwrite=False: if file already exists, keep old file unchanged
        """
        if dataset == "card_index":
            out_dir = self._card_index_dir()
        else:
            if partition_date is None:
                raise ValueError(f"partition_date is required for dataset: {dataset}")
            out_dir = self._partition_dir("hareruya", dataset, partition_date)

        filename = self._build_record_filename(row)
        out_path = out_dir / filename

        if out_path.exists() and not overwrite:
            return out_path

        self._write_parquet(pd.DataFrame([row]), out_path)
        return out_path

    @staticmethod
    def _staging_meta(dataset: str, partition_date: date, observed_at: str) -> dict[str, str]:
        return {
            "source": "hareruya",
            "dataset": dataset,
            "extract_date": partition_date.isoformat(),
            "observed_at": observed_at,
            "observed_date": partition_date.isoformat(),
        }

    def _row_card_index(self, norm: dict[str, Any], meta: dict[str, str]) -> dict[str, Any]:
        """
        Stable card identity fields.
        Usually less frequently changed than price.
        """
        return {
            **meta,
            "collection_id": norm.get("collection_id"),
            "product_id": norm.get("product_id"),
            "handle": norm.get("handle"),
            "set_code": norm.get("set_code"),
            "card_number": norm.get("card_number"),
            "card_number_int": norm.get("card_number_int"),
            "total_in_set": norm.get("total_in_set"),
            "total_in_set_int": norm.get("total_in_set_int"),
            "card_name_jp": norm.get("card_name_jp"),
            "card_name_en": norm.get("card_name_en"),
            "body_name_jp": norm.get("body_name_jp"),
            "rarity": norm.get("rarity"),
            "card_type_jp": norm.get("card_type_jp"),
            "zukan_number": norm.get("zukan_number"),
            "zukan_number_int": norm.get("zukan_number_int"),
            "product_url": norm.get("product_url"),
            "image_url": norm.get("image_url"),
        }

    def _row_price_events(self, norm: dict[str, Any], meta: dict[str, str]) -> dict[str, Any]:
        """
        Event-like commercial fields that may change over time.
        """
        return {
            **meta,
            "collection_id": norm.get("collection_id"),
            "product_id": norm.get("product_id"),
            "variant_id": norm.get("variant_id"),
            "handle": norm.get("handle"),
            "set_code": norm.get("set_code"),
            "card_number": norm.get("card_number"),
            "card_name_jp": norm.get("card_name_jp"),
            "card_name_en": norm.get("card_name_en"),
            "rarity": norm.get("rarity"),
            "price_jpy": norm.get("price_jpy"),
            "compare_at_price_jpy": norm.get("compare_at_price_jpy"),
            "available": norm.get("available"),
            "currency": norm.get("currency"),
            "product_url": norm.get("product_url"),
        }

    def write_hareruya_record(
        self,
        record: dict[str, Any],
        extract_date: date | datetime | str,
        overwrite_card_index: bool = False,
    ) -> dict[str, Path]:
        """
        Write all Hareruya staging slices for one card in one pass.

        Args:
            record:
                One normalized Hareruya card record.
            extract_date:
                Business extract date.
            overwrite_card_index:
                Whether to overwrite existing card_index parquet.
                Default = False.

        Returns:
            Map of logical dataset name -> written parquet path.
        """
        partition_date = self._normalize_extract_date(extract_date)
        observed_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        norm = self._normalize_hareruya_record(record)

        cards_row = {
            **norm,
            **self._staging_meta("cards_normalized", partition_date, observed_at),
        }
        index_row = self._row_card_index(
            norm,
            self._staging_meta("card_index", partition_date, observed_at),
        )
        price_row = self._row_price_events(
            norm,
            self._staging_meta("price_events", partition_date, observed_at),
        )

        return {
            "cards_normalized": self._write_prepared_row(
                cards_row,
                "cards_normalized",
                partition_date=partition_date,
                overwrite=True,
            ),
            "card_index": self._write_prepared_row(
                index_row,
                "card_index",
                overwrite=overwrite_card_index,
            ),
            "price_events": self._write_prepared_row(
                price_row,
                "price_events",
                partition_date=partition_date,
                overwrite=True,
            ),
        }

    def write_cards_normalized(
        self,
        records: list[dict[str, Any]],
        extract_date: date | datetime | str,
    ) -> list[Path]:
        partition_date = self._normalize_extract_date(extract_date)
        paths: list[Path] = []

        for rec in records:
            observed_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
            norm = self._normalize_hareruya_record(rec)
            meta = self._staging_meta("cards_normalized", partition_date, observed_at)
            row = {**norm, **meta}
            paths.append(
                self._write_prepared_row(
                    row,
                    "cards_normalized",
                    partition_date=partition_date,
                    overwrite=True,
                )
            )

        return paths

    def write_card_index(
        self,
        records: list[dict[str, Any]],
        extract_date: date | datetime | str,
        overwrite_card_index: bool = False,
    ) -> list[Path]:
        """
        Write card_index records into fixed non-partitioned directory.

        Default behavior:
        - keep existing file unchanged
        - only write new file if it does not already exist

        Set overwrite_card_index=True to force replacement.
        """
        partition_date = self._normalize_extract_date(extract_date)
        paths: list[Path] = []

        for rec in records:
            observed_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
            norm = self._normalize_hareruya_record(rec)
            meta = self._staging_meta("card_index", partition_date, observed_at)
            row = self._row_card_index(norm, meta)
            paths.append(
                self._write_prepared_row(
                    row,
                    "card_index",
                    overwrite=overwrite_card_index,
                )
            )

        return paths

    def write_price_events(
        self,
        records: list[dict[str, Any]],
        extract_date: date | datetime | str,
    ) -> list[Path]:
        partition_date = self._normalize_extract_date(extract_date)
        paths: list[Path] = []

        for rec in records:
            observed_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
            norm = self._normalize_hareruya_record(rec)
            meta = self._staging_meta("price_events", partition_date, observed_at)
            row = self._row_price_events(norm, meta)
            paths.append(
                self._write_prepared_row(
                    row,
                    "price_events",
                    partition_date=partition_date,
                    overwrite=True,
                )
            )

        return paths


if __name__ == "__main__":
    sample_records = [
        {
            "source": "hareruya",
            "collection_id": "706",
            "source_url": "https://www.hareruya2.com/collections/706",
            "final_url": "https://www.hareruya2.com/collections/706",
            "product_id": "9921823932736",
            "handle": "9921823932736",
            "title_raw": "ナゾノクサ(C){草}〈001/080〉[M2]",
            "card_name_jp": "ナゾノクサ",
            "card_name_en": "Oddish",
            "body_name_jp": "ナゾノクサ",
            "rarity": "C",
            "card_type_jp": "草",
            "card_number": "001",
            "total_in_set": "080",
            "set_code": "M2",
            "zukan_number": "43",
            "variant_id": "51084242288960",
            "price_jpy": 30.0,
            "compare_at_price_jpy": 50.0,
            "available": True,
            "currency": "JPY",
            "product_url": "https://www.hareruya2.com/products/9921823932736",
            "image_url": "https://cdn.shopify.com/example.webp",
            "tags": ["MEGAシリーズ", "同名検索:ナゾノクサ"],
        }
    ]

    loader = HareruyaStagingLoader()

    print("=== default: do NOT overwrite existing card_index ===")
    for rec in sample_records:
        written = loader.write_hareruya_record(
            rec,
            extract_date="2026-03-30",
        )
        for name, path in written.items():
            print(f"Written {name}: {path}")

    print("\n=== force overwrite card_index ===")
    for rec in sample_records:
        written = loader.write_hareruya_record(
            rec,
            extract_date="2026-03-30",
            overwrite_card_index=True,
        )
        for name, path in written.items():
            print(f"Written {name}: {path}")