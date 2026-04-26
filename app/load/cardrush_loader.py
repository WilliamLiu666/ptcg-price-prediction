from contextlib import closing
from datetime import datetime, timezone

from app.utils.postgres_db import connect_postgres
from app.utils.postgres_schema import ensure_cardrush_schema
from psycopg2.extensions import connection as PgConnection


class CardrushLoader:
    def __init__(self, schema_name: str | None = None):
        self.schema_name = schema_name

    def _connect(self) -> PgConnection:
        return connect_postgres(schema_name=self.schema_name)

    def save_products(self, product_group: str, items: list[dict], parse_price_func) -> int:
        if not product_group:
            raise RuntimeError("product_group is required")

        now = datetime.now(timezone.utc)
        observed_at = now.isoformat()
        observed_date = now.date().isoformat()
        written = 0

        with closing(self._connect()) as conn:
            self._ensure_schema(conn)
            cur = conn.cursor()

            for it in items:
                product_id = it.get("product_id")
                url = it.get("product_url") or it.get("url")
                name = it.get("name")
                name_full = it.get("name_full")
                condition = it.get("condition")
                model_number = it.get("model_number")
                set_size = it.get("set_size")
                model_code = it.get("model_code")
                price_yen = parse_price_func(it.get("price"))
                price_text = it.get("price")

                if not product_id or not url or not name or not name_full:
                    continue
                if not model_number:
                    continue
                if price_yen is None:
                    continue

                normalized_price_yen = int(price_yen)

                cur.execute("""
                INSERT INTO products_cardrush (
                    product_id, product_group, model_number, set_size,
                    name, name_full, condition, model_code,
                    price_yen, url,
                    created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT(product_id) DO UPDATE SET
                    product_group  = excluded.product_group,
                    model_number = excluded.model_number,
                    set_size     = excluded.set_size,
                    name         = excluded.name,
                    name_full    = excluded.name_full,
                    condition    = excluded.condition,
                    model_code   = excluded.model_code,
                    price_yen    = excluded.price_yen,
                    url          = excluded.url,
                    updated_at   = excluded.updated_at
                """, (
                    product_id, product_group, model_number, set_size,
                    name, name_full, condition, model_code,
                    normalized_price_yen, url,
                    observed_at, observed_at
                ))

                cur.execute("""
                INSERT INTO prices_cardrush (
                    product_id, observed_at, observed_date, price_yen, price_text, source
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    product_id,
                    observed_at,
                    observed_date,
                    normalized_price_yen,
                    price_text,
                    "cardrush",
                ))

                cur.execute("""
                INSERT INTO prices_cardrush_current (
                    product_id, price_yen, price_text, observed_at, observed_date, source, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT(product_id) DO UPDATE SET
                    price_yen = excluded.price_yen,
                    price_text = excluded.price_text,
                    observed_at = excluded.observed_at,
                    observed_date = excluded.observed_date,
                    source = excluded.source,
                    updated_at = excluded.updated_at
                """, (
                    product_id,
                    normalized_price_yen,
                    price_text,
                    observed_at,
                    observed_date,
                    "cardrush",
                    observed_at,
                ))

                written += 1

            conn.commit()

        return written

    @staticmethod
    def _ensure_schema(conn: PgConnection) -> None:
        ensure_cardrush_schema(conn)
