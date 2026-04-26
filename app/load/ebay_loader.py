from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path

from app.utils.extract_policy import resolve_observed_timestamps
from app.utils.sqlite_schema import (
    connect_sqlite,
    ensure_ebay_schema,
    ensure_prices_limitless_schema,
)


class EbayLoader:
    """
    Load layer for eBay prices.

    Responsibilities:
    - Update eBay price in SQLite
    - Manage database connection settings

    This class does NOT handle:
    - HTTP requests
    - Search keyword generation
    - Price selection logic
    """

    def __init__(self, db_path: str | Path) -> None:
        """
        Initialize the loader.

        Args:
            db_path:
                SQLite database path.
        """
        self.db_path = Path(db_path)

    def _connect(self) -> sqlite3.Connection:
        """
        Open a SQLite connection with useful pragmas.
        """
        return connect_sqlite(self.db_path)

    def ensure_ebay_columns(self) -> None:
        """
        Ensure eBay timestamp columns/history exist.
        """
        with closing(self._connect()) as conn:
            ensure_prices_limitless_schema(conn)
            ensure_ebay_schema(conn)
            conn.commit()

    def update_ebay_price(
        self,
        lang: str,
        set_code: str,
        card_code: str,
        ebay_price: float | None,
        *,
        card_id: int | str | None = None,
        card_name: str | None = None,
        currency: str | None = None,
        condition: str | None = None,
        marketplace_id: str = "EBAY_GB",
        selected_item_id: str | None = None,
        selected_title: str | None = None,
        selected_item_web_url: str | None = None,
        observed_date: str | None = None,
        observed_at: str | None = None,
        update_current: bool = True,
    ) -> int:
        """
        Write one eBay observation.

        Args:
            lang:
                Card language.
            set_code:
                Card set code.
            card_code:
                Card number/code.
            ebay_price:
                Selected eBay price.
            card_id:
                Optional canonical card id.
            card_name:
                Optional card display name.
            currency:
                Optional price currency from the selected eBay listing.
            condition:
                Optional selected listing condition.
            marketplace_id:
                eBay marketplace id, e.g. ``EBAY_GB``.
            selected_item_id:
                Optional selected listing id.
            selected_title:
                Optional selected listing title.
            selected_item_web_url:
                Optional selected listing url.
            observed_date:
                Optional business date for the observation.
            observed_at:
                Optional full observation timestamp.
            update_current:
                Whether to update current-price tables.

        Returns:
            Number of updated current rows in ``prices_limitless``.
        """
        self.ensure_ebay_columns()
        observed_at, observed_date = resolve_observed_timestamps(
            observed_date=observed_date,
            observed_at=observed_at,
        )
        resolved_currency = (currency or "GBP").strip().upper()

        with closing(self._connect()) as conn:
            updated_rows = 0

            if update_current:
                cursor = conn.execute(
                    """
                    UPDATE prices_limitless
                    SET ebay_price = ?,
                        ebay_observed_at = ?,
                        ebay_observed_date = ?,
                        updated_at = ?
                    WHERE lang = ? AND set_code = ? AND card_code = ?
                    """,
                    (ebay_price, observed_at, observed_date, observed_at, lang, set_code, card_code),
                )
                updated_rows = cursor.rowcount

                conn.execute(
                    """
                    INSERT INTO prices_ebay_current
                    (
                      card_id, lang, set_code, card_code, card_name,
                      marketplace_id, currency, condition,
                      selected_item_id, selected_title, selected_item_web_url,
                      ebay_price, observed_at, observed_date, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(lang, set_code, card_code, marketplace_id, currency)
                    DO UPDATE SET
                      card_id = COALESCE(excluded.card_id, prices_ebay_current.card_id),
                      card_name = COALESCE(excluded.card_name, prices_ebay_current.card_name),
                      condition = COALESCE(excluded.condition, prices_ebay_current.condition),
                      selected_item_id = COALESCE(excluded.selected_item_id, prices_ebay_current.selected_item_id),
                      selected_title = COALESCE(excluded.selected_title, prices_ebay_current.selected_title),
                      selected_item_web_url = COALESCE(excluded.selected_item_web_url, prices_ebay_current.selected_item_web_url),
                      ebay_price = excluded.ebay_price,
                      observed_at = excluded.observed_at,
                      observed_date = excluded.observed_date,
                      updated_at = excluded.updated_at
                    """,
                    (
                        card_id,
                        lang,
                        set_code,
                        card_code,
                        card_name,
                        marketplace_id,
                        resolved_currency,
                        condition,
                        selected_item_id,
                        selected_title,
                        selected_item_web_url,
                        ebay_price,
                        observed_at,
                        observed_date,
                        observed_at,
                        observed_at,
                    ),
                )

            conn.execute(
                """
                INSERT INTO prices_ebay_history
                (
                  card_id, lang, set_code, card_code, card_name,
                  marketplace_id, currency, condition,
                  selected_item_id, selected_title, selected_item_web_url,
                  ebay_price, ebay_observed_at, ebay_observed_date
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(
                  lang,
                  set_code,
                  card_code,
                  marketplace_id,
                  currency,
                  ebay_observed_date
                )
                DO UPDATE SET
                  card_id = COALESCE(excluded.card_id, prices_ebay_history.card_id),
                  card_name = COALESCE(excluded.card_name, prices_ebay_history.card_name),
                  condition = COALESCE(excluded.condition, prices_ebay_history.condition),
                  selected_item_id = COALESCE(excluded.selected_item_id, prices_ebay_history.selected_item_id),
                  selected_title = COALESCE(excluded.selected_title, prices_ebay_history.selected_title),
                  selected_item_web_url = COALESCE(excluded.selected_item_web_url, prices_ebay_history.selected_item_web_url),
                  ebay_price = excluded.ebay_price,
                  ebay_observed_at = excluded.ebay_observed_at
                """,
                (
                    card_id,
                    lang,
                    set_code,
                    card_code,
                    card_name,
                    marketplace_id,
                    resolved_currency,
                    condition,
                    selected_item_id,
                    selected_title,
                    selected_item_web_url,
                    ebay_price,
                    observed_at,
                    observed_date,
                ),
            )
            conn.commit()
            return updated_rows


def main() -> None:
    """
    Simple standalone test for this module.
    """
    loader = EbayLoader(db_path="ptcg.sqlite")

    updated_rows = loader.update_ebay_price(
        lang="en",
        set_code="JTG",
        card_code="1",
        ebay_price=0.99,
    )
    print(f"Updated rows: {updated_rows}")


if __name__ == "__main__":
    main()
