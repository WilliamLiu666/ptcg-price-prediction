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


class EbayStagingLoader:
    """
    Load layer for eBay staging.

    Responsibilities:
    - Write normalized eBay records into staging parquet
    - Keep cards_normalized and price_events partitioned by extract_date
    - Keep card_index in a fixed non-partitioned directory
    - Support optional overwrite control for card_index
    """

    def __init__(self, data_root: str | Path | None = None) -> None:
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
        return self._base_data_root() / "staging" / "ebay" / "card_index"

    @staticmethod
    def _normalize_extract_date(dt: date | datetime | str) -> date:
        if isinstance(dt, datetime):
            return dt.date()
        if isinstance(dt, date):
            return dt
        return datetime.fromisoformat(dt).date()

    @staticmethod
    def _normalize_ebay_record(record: dict[str, Any]) -> dict[str, Any]:
        row = dict(record)
        row["card_id"] = _coerce_optional_int(row.get("card_id"))
        row["search_limit"] = _coerce_optional_int(row.get("search_limit"))
        row["raw_item_count"] = _coerce_optional_int(row.get("raw_item_count"))
        row["normalized_item_count"] = _coerce_optional_int(row.get("normalized_item_count"))
        return row

    @staticmethod
    def _safe_str(value: Any, default: str = "unknown") -> str:
        if value is None:
            return default
        text = str(value).strip()
        return text if text else default

    def _build_record_filename(self, record: dict[str, Any]) -> str:
        lang = self._safe_str(record.get("lang"))
        set_code = self._safe_str(record.get("set_code"))
        card_code = self._safe_str(record.get("card_code"))
        return f"{lang}_{set_code}_{card_code}.parquet"

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
        if dataset == "card_index":
            out_dir = self._card_index_dir()
        else:
            if partition_date is None:
                raise ValueError(f"partition_date is required for dataset: {dataset}")
            out_dir = self._partition_dir("ebay", dataset, partition_date)

        filename = self._build_record_filename(row)
        out_path = out_dir / filename

        if out_path.exists() and not overwrite:
            return out_path

        self._write_parquet(pd.DataFrame([row]), out_path)
        return out_path

    @staticmethod
    def _staging_meta(dataset: str, partition_date: date, observed_at: str) -> dict[str, str]:
        return {
            "source": "ebay",
            "dataset": dataset,
            "extract_date": partition_date.isoformat(),
            "observed_at": observed_at,
            "observed_date": partition_date.isoformat(),
        }

    def _row_card_index(self, norm: dict[str, Any], meta: dict[str, str]) -> dict[str, Any]:
        return {
            **meta,
            "card_id": norm.get("card_id"),
            "lang": norm.get("lang"),
            "set_code": norm.get("set_code"),
            "card_code": norm.get("card_code"),
            "card_name": norm.get("card_name"),
            "search_keyword": norm.get("search_keyword"),
            "marketplace_id": norm.get("marketplace_id"),
        }

    def _row_price_events(self, norm: dict[str, Any], meta: dict[str, str]) -> dict[str, Any]:
        return {
            **meta,
            "card_id": norm.get("card_id"),
            "lang": norm.get("lang"),
            "set_code": norm.get("set_code"),
            "card_code": norm.get("card_code"),
            "card_name": norm.get("card_name"),
            "search_keyword": norm.get("search_keyword"),
            "raw_item_count": norm.get("raw_item_count"),
            "normalized_item_count": norm.get("normalized_item_count"),
            "selected_item_id": norm.get("selected_item_id"),
            "selected_title": norm.get("selected_title"),
            "selected_condition": norm.get("selected_condition"),
            "selected_price_value": norm.get("selected_price_value"),
            "selected_shipping_cost_value": norm.get("selected_shipping_cost_value"),
            "selected_total_price": norm.get("selected_total_price"),
            "currency": norm.get("currency"),
            "shipping_currency": norm.get("shipping_currency"),
            "selected_item_web_url": norm.get("selected_item_web_url"),
        }

    def write_ebay_record(
        self,
        record: dict[str, Any],
        extract_date: date | datetime | str,
        overwrite_card_index: bool = False,
    ) -> dict[str, Path]:
        partition_date = self._normalize_extract_date(extract_date)
        observed_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        norm = self._normalize_ebay_record(record)

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
            norm = self._normalize_ebay_record(rec)
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
        partition_date = self._normalize_extract_date(extract_date)
        paths: list[Path] = []

        for rec in records:
            observed_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
            norm = self._normalize_ebay_record(rec)
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
            norm = self._normalize_ebay_record(rec)
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
