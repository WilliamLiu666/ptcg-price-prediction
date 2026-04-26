from __future__ import annotations

from contextlib import closing
from typing import Any

from app.utils.extract_policy import resolve_observed_timestamps
from app.utils.postgres_db import connect_postgres
from app.utils.postgres_schema import ensure_hareruya_schema
from psycopg2.extensions import connection as PgConnection


class HareruyaLoader:
    def __init__(self, schema_name: str | None = None) -> None:
        self.schema_name = schema_name

    def _connect(self) -> PgConnection:
        return connect_postgres(schema_name=self.schema_name)

    def save_product_prices(
        self,
        records: list[dict[str, Any]],
        *,
        observed_date: str | None = None,
        observed_at: str | None = None,
        update_current: bool = True,
    ) -> int:
        written = 0

        with closing(self._connect()) as conn:
            ensure_hareruya_schema(conn)

            for record in records:
                product_id = record.get("product_id")
                price_jpy = record.get("price_jpy")
                if product_id is None or price_jpy is None:
                    continue

                resolved_observed_at, resolved_observed_date = resolve_observed_timestamps(
                    observed_date=observed_date,
                    observed_at=observed_at,
                )

                with conn.cursor() as cur:
                    if update_current:
                        cur.execute(
                            """
                            INSERT INTO prices_hareruya_current
                            (
                              product_id, collection_id, set_code, card_number,
                              card_name_jp, card_name_en, variant_title,
                              currency, price_jpy, compare_at_price_jpy,
                              product_url, observed_at, observed_date, created_at, updated_at
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT(product_id) DO UPDATE SET
                              collection_id = excluded.collection_id,
                              set_code = excluded.set_code,
                              card_number = excluded.card_number,
                              card_name_jp = excluded.card_name_jp,
                              card_name_en = excluded.card_name_en,
                              variant_title = excluded.variant_title,
                              currency = excluded.currency,
                              price_jpy = excluded.price_jpy,
                              compare_at_price_jpy = excluded.compare_at_price_jpy,
                              product_url = excluded.product_url,
                              observed_at = excluded.observed_at,
                              observed_date = excluded.observed_date,
                              updated_at = excluded.updated_at
                            """,
                            (
                                product_id,
                                record.get("collection_id"),
                                record.get("set_code"),
                                record.get("card_number"),
                                record.get("card_name_jp"),
                                record.get("card_name_en"),
                                record.get("variant_title"),
                                record.get("currency") or "JPY",
                                price_jpy,
                                record.get("compare_at_price_jpy"),
                                record.get("product_url"),
                                resolved_observed_at,
                                resolved_observed_date,
                                resolved_observed_at,
                                resolved_observed_at,
                            ),
                        )

                    cur.execute(
                        """
                        INSERT INTO prices_hareruya_history
                        (
                          product_id, collection_id, set_code, card_number,
                          card_name_jp, card_name_en, variant_title,
                          currency, price_jpy, compare_at_price_jpy,
                          product_url, observed_at, observed_date
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT(product_id, observed_date)
                        DO UPDATE SET
                          collection_id = excluded.collection_id,
                          set_code = excluded.set_code,
                          card_number = excluded.card_number,
                          card_name_jp = excluded.card_name_jp,
                          card_name_en = excluded.card_name_en,
                          variant_title = excluded.variant_title,
                          currency = excluded.currency,
                          price_jpy = excluded.price_jpy,
                          compare_at_price_jpy = excluded.compare_at_price_jpy,
                          product_url = excluded.product_url,
                          observed_at = excluded.observed_at
                        """,
                        (
                            product_id,
                            record.get("collection_id"),
                            record.get("set_code"),
                            record.get("card_number"),
                            record.get("card_name_jp"),
                            record.get("card_name_en"),
                            record.get("variant_title"),
                            record.get("currency") or "JPY",
                            price_jpy,
                            record.get("compare_at_price_jpy"),
                            record.get("product_url"),
                            resolved_observed_at,
                            resolved_observed_date,
                        ),
                    )
                written += 1

            conn.commit()

        return written
