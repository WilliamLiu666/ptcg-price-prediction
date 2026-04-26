from __future__ import annotations

from contextlib import closing
from pathlib import Path

from app.utils.extract_policy import resolve_observed_timestamps
from app.utils.postgres_db import connect_postgres
from app.utils.postgres_schema import (
    ensure_cards_index_schema,
    ensure_prices_limitless_schema,
)
from psycopg2.extensions import connection as PgConnection


class LimitlessLoader:
    """
    Load layer for Limitless.

    Responsibilities:
    - Create/ensure PostgreSQL tables
    - Upsert card index data
    - Upsert price data

    This class does NOT handle:
    - HTTP requests
    - HTML parsing
    """

    def __init__(self, schema_name: str | None = None) -> None:
        """
        Initialize the loader.

        Args:
            schema_name:
                Optional PostgreSQL schema/search_path override.
        """
        self.schema_name = schema_name

    def _connect(self) -> PgConnection:
        """
        Open a PostgreSQL connection.
        """
        return connect_postgres(schema_name=self.schema_name)

    def ensure_cards_index_table(self) -> None:
        """
        Ensure the cards_index table exists.

        Target schema:
        - PK: card_id
        - UNIQUE(lang, set_code, card_code)
        """
        with closing(self._connect()) as conn:
            ensure_cards_index_schema(conn)
            conn.commit()

    def ensure_prices_limitless_schema(self) -> None:
        """
        Ensure prices_limitless has timestamp columns and history table exists.
        """
        with closing(self._connect()) as conn:
            ensure_prices_limitless_schema(conn)
            conn.commit()

    def save_card_index(self, record: dict[str, str | float | None]) -> None:
        """
        Upsert one card record into cards_index using card_id as primary key.

        Update policy:
        - lang / set_code / card_code are always kept in sync
        - data_id / card_name / rarity only update when new value is not NULL
        """
        with closing(self._connect()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO cards_index
                    (card_id, data_id, lang, set_code, card_code, card_name, rarity)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)

                    ON CONFLICT(card_id)
                    DO UPDATE SET
                        lang = excluded.lang,
                        set_code = excluded.set_code,
                        card_code = excluded.card_code,

                        data_id = CASE
                            WHEN excluded.data_id IS NOT NULL THEN excluded.data_id
                            ELSE cards_index.data_id
                        END,

                        card_name = CASE
                            WHEN excluded.card_name IS NOT NULL THEN excluded.card_name
                            ELSE cards_index.card_name
                        END,

                        rarity = CASE
                            WHEN excluded.rarity IS NOT NULL THEN excluded.rarity
                            ELSE cards_index.rarity
                        END
                    """,
                    (
                        record.get("card_id"),
                        record.get("data_id"),
                        record.get("lang"),
                        record.get("set_code"),
                        record.get("card_code"),
                        record.get("card_name"),
                        record.get("rarity"),
                    ),
                )
            conn.commit()

    def save_card_price(
        self,
        record: dict[str, str | float | None],
        *,
        observed_date: str | None = None,
        observed_at: str | None = None,
        update_current: bool = True,
    ) -> None:
        """
        Write one Limitless observation.

        Update policy:
        - current table updates only when ``update_current=True``
        - history is always written/upserted for the resolved business date
        """
        self.ensure_prices_limitless_schema()
        observed_at, observed_date = resolve_observed_timestamps(
            observed_date=observed_date,
            observed_at=observed_at,
        )

        with closing(self._connect()) as conn:
            with conn.cursor() as cur:
                if update_current:
                    cur.execute(
                        """
                        INSERT INTO prices_limitless
                        (
                          card_id, data_id, lang, set_code, card_code, card_name, rarity,
                          usd_price, eur_price, observed_at, observed_date, created_at, updated_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)

                        ON CONFLICT(card_id)
                        DO UPDATE SET
                            lang = excluded.lang,
                            set_code = excluded.set_code,
                            card_code = excluded.card_code,

                            data_id = CASE
                                WHEN excluded.data_id IS NOT NULL THEN excluded.data_id
                                ELSE prices_limitless.data_id
                            END,

                            card_name = CASE
                                WHEN excluded.card_name IS NOT NULL THEN excluded.card_name
                                ELSE prices_limitless.card_name
                            END,

                            rarity = CASE
                                WHEN excluded.rarity IS NOT NULL THEN excluded.rarity
                                ELSE prices_limitless.rarity
                            END,

                            usd_price = CASE
                                WHEN excluded.usd_price IS NOT NULL THEN excluded.usd_price
                                ELSE prices_limitless.usd_price
                            END,

                            eur_price = CASE
                                WHEN excluded.eur_price IS NOT NULL THEN excluded.eur_price
                                ELSE prices_limitless.eur_price
                            END,
                            observed_at = excluded.observed_at,
                            observed_date = excluded.observed_date,
                            created_at = COALESCE(prices_limitless.created_at, excluded.created_at),
                            updated_at = excluded.updated_at
                        """,
                        (
                            record.get("card_id"),
                            record.get("data_id"),
                            record.get("lang"),
                            record.get("set_code"),
                            record.get("card_code"),
                            record.get("card_name"),
                            record.get("rarity"),
                            record.get("usd_price"),
                            record.get("eur_price"),
                            observed_at,
                            observed_date,
                            observed_at,
                            observed_at,
                        ),
                    )

                cur.execute(
                    """
                    INSERT INTO prices_limitless_history
                    (
                      card_id, lang, set_code, card_code, usd_price, eur_price, ebay_price,
                      source, observed_at, observed_date
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, 'limitless', %s, %s)
                    ON CONFLICT(lang, set_code, card_code, source, observed_date)
                    DO UPDATE SET
                      card_id = COALESCE(excluded.card_id, prices_limitless_history.card_id),
                      usd_price = excluded.usd_price,
                      eur_price = excluded.eur_price,
                      ebay_price = excluded.ebay_price,
                      observed_at = excluded.observed_at
                    """,
                    (
                        record.get("card_id"),
                        record.get("lang"),
                        record.get("set_code"),
                        record.get("card_code"),
                        record.get("usd_price"),
                        record.get("eur_price"),
                        None,
                        observed_at,
                        observed_date,
                    ),
                )

            conn.commit()


if __name__ == "__main__":
    loader = LimitlessLoader()
    loader.ensure_cards_index_table()
    print("cards_index table ensured.")
