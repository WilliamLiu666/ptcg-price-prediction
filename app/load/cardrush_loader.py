import sqlite3
from datetime import datetime, timezone


class CardrushLoader:
    def __init__(self, db_path: str = "ptcg.sqlite"):
        self.db_path = db_path

    def save_products(self, product_group: str, items: list[dict], parse_price_func) -> int:
        if not product_group:
            raise RuntimeError("product_group is required")

        now = datetime.now(timezone.utc)
        observed_at = now.isoformat()
        written = 0

        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
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

                if not product_id or not url or not name or not name_full:
                    continue
                if not model_number:
                    continue
                if price_yen is None:
                    continue

                cur.execute("""
                INSERT INTO products_cardrush (
                    product_id, product_group, model_number, set_size,
                    name, name_full, condition, model_code,
                    price_yen, url,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    price_yen, url,
                    observed_at, observed_at
                ))

                cur.execute("""
                INSERT INTO prices_cardrush (
                    product_id, observed_at, price_yen
                )
                VALUES (?, ?, ?)
                """, (product_id, observed_at, price_yen))

                written += 1

            conn.commit()

        return written