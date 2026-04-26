from __future__ import annotations

import argparse
import sqlite3
from contextlib import closing
from pathlib import Path

from psycopg2 import sql
from psycopg2.extras import execute_values

from app.utils.postgres_db import connect_postgres
from app.utils.postgres_schema import ensure_app_schema


SQLITE_TABLES_IN_LOAD_ORDER: list[tuple[str, list[str], str | None]] = [
    (
        "series_limitless",
        ["series_code", "lang", "size"],
        """
        ON CONFLICT (series_code, lang) DO UPDATE SET
          size = excluded.size
        """,
    ),
    (
        "series_hareruya",
        ["series_code", "collection"],
        """
        ON CONFLICT (series_code) DO UPDATE SET
          collection = excluded.collection
        """,
    ),
    (
        "series_url_jp",
        ["series_code", "series_name", "source", "list_url", "active"],
        """
        ON CONFLICT (series_code) DO UPDATE SET
          series_name = excluded.series_name,
          source = excluded.source,
          list_url = excluded.list_url,
          active = excluded.active
        """,
    ),
    (
        "cards_index",
        ["card_id", "data_id", "lang", "set_code", "card_code", "card_name", "rarity"],
        """
        ON CONFLICT (card_id) DO UPDATE SET
          data_id = excluded.data_id,
          lang = excluded.lang,
          set_code = excluded.set_code,
          card_code = excluded.card_code,
          card_name = excluded.card_name,
          rarity = excluded.rarity
        """,
    ),
    (
        "prices_limitless",
        [
            "card_id",
            "data_id",
            "lang",
            "set_code",
            "card_code",
            "card_name",
            "rarity",
            "usd_price",
            "eur_price",
            "ebay_price",
            "observed_at",
            "observed_date",
            "created_at",
            "updated_at",
            "ebay_observed_at",
            "ebay_observed_date",
        ],
        """
        ON CONFLICT (card_id) DO UPDATE SET
          data_id = excluded.data_id,
          lang = excluded.lang,
          set_code = excluded.set_code,
          card_code = excluded.card_code,
          card_name = excluded.card_name,
          rarity = excluded.rarity,
          usd_price = excluded.usd_price,
          eur_price = excluded.eur_price,
          ebay_price = excluded.ebay_price,
          observed_at = excluded.observed_at,
          observed_date = excluded.observed_date,
          created_at = excluded.created_at,
          updated_at = excluded.updated_at,
          ebay_observed_at = excluded.ebay_observed_at,
          ebay_observed_date = excluded.ebay_observed_date
        """,
    ),
    (
        "prices_limitless_history",
        [
            "card_id",
            "lang",
            "set_code",
            "card_code",
            "usd_price",
            "eur_price",
            "ebay_price",
            "source",
            "observed_at",
            "observed_date",
        ],
        """
        ON CONFLICT (lang, set_code, card_code, source, observed_date) DO UPDATE SET
          card_id = excluded.card_id,
          usd_price = excluded.usd_price,
          eur_price = excluded.eur_price,
          ebay_price = excluded.ebay_price,
          observed_at = excluded.observed_at
        """,
    ),
    (
        "prices_ebay_current",
        [
            "card_id",
            "lang",
            "set_code",
            "card_code",
            "card_name",
            "marketplace_id",
            "currency",
            "condition",
            "selected_item_id",
            "selected_title",
            "selected_item_web_url",
            "ebay_price",
            "observed_at",
            "observed_date",
            "created_at",
            "updated_at",
        ],
        """
        ON CONFLICT (lang, set_code, card_code, marketplace_id, currency) DO UPDATE SET
          card_id = excluded.card_id,
          card_name = excluded.card_name,
          condition = excluded.condition,
          selected_item_id = excluded.selected_item_id,
          selected_title = excluded.selected_title,
          selected_item_web_url = excluded.selected_item_web_url,
          ebay_price = excluded.ebay_price,
          observed_at = excluded.observed_at,
          observed_date = excluded.observed_date,
          created_at = excluded.created_at,
          updated_at = excluded.updated_at
        """,
    ),
    (
        "prices_ebay_history",
        [
            "card_id",
            "lang",
            "set_code",
            "card_code",
            "card_name",
            "marketplace_id",
            "currency",
            "condition",
            "selected_item_id",
            "selected_title",
            "selected_item_web_url",
            "ebay_price",
            "ebay_observed_at",
            "ebay_observed_date",
        ],
        """
        ON CONFLICT (lang, set_code, card_code, marketplace_id, currency, ebay_observed_date) DO UPDATE SET
          card_id = excluded.card_id,
          card_name = excluded.card_name,
          condition = excluded.condition,
          selected_item_id = excluded.selected_item_id,
          selected_title = excluded.selected_title,
          selected_item_web_url = excluded.selected_item_web_url,
          ebay_price = excluded.ebay_price,
          ebay_observed_at = excluded.ebay_observed_at
        """,
    ),
    (
        "ebay_search_results",
        [
            "keyword",
            "item_id",
            "title",
            "price_value",
            "currency",
            "item_web_url",
            "condition",
            "observed_at",
        ],
        None,
    ),
    (
        "products_cardrush",
        [
            "product_id",
            "product_group",
            "model_number",
            "set_size",
            "name",
            "name_full",
            "condition",
            "model_code",
            "price_yen",
            "url",
            "created_at",
            "updated_at",
        ],
        """
        ON CONFLICT (product_id) DO UPDATE SET
          product_group = excluded.product_group,
          model_number = excluded.model_number,
          set_size = excluded.set_size,
          name = excluded.name,
          name_full = excluded.name_full,
          condition = excluded.condition,
          model_code = excluded.model_code,
          price_yen = excluded.price_yen,
          url = excluded.url,
          created_at = excluded.created_at,
          updated_at = excluded.updated_at
        """,
    ),
    (
        "prices_cardrush_current",
        [
            "product_id",
            "price_yen",
            "price_text",
            "observed_at",
            "observed_date",
            "source",
            "updated_at",
        ],
        """
        ON CONFLICT (product_id) DO UPDATE SET
          price_yen = excluded.price_yen,
          price_text = excluded.price_text,
          observed_at = excluded.observed_at,
          observed_date = excluded.observed_date,
          source = excluded.source,
          updated_at = excluded.updated_at
        """,
    ),
    (
        "prices_cardrush",
        [
            "product_id",
            "observed_at",
            "observed_date",
            "price_yen",
            "price_text",
            "source",
        ],
        """
        ON CONFLICT (product_id, observed_at) DO UPDATE SET
          observed_date = excluded.observed_date,
          price_yen = excluded.price_yen,
          price_text = excluded.price_text,
          source = excluded.source
        """,
    ),
    (
        "prices_hareruya_current",
        [
            "product_id",
            "collection_id",
            "set_code",
            "card_number",
            "card_name_jp",
            "card_name_en",
            "variant_title",
            "currency",
            "price_jpy",
            "compare_at_price_jpy",
            "product_url",
            "observed_at",
            "observed_date",
            "created_at",
            "updated_at",
        ],
        """
        ON CONFLICT (product_id) DO UPDATE SET
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
          created_at = excluded.created_at,
          updated_at = excluded.updated_at
        """,
    ),
    (
        "prices_hareruya_history",
        [
            "product_id",
            "collection_id",
            "set_code",
            "card_number",
            "card_name_jp",
            "card_name_en",
            "variant_title",
            "currency",
            "price_jpy",
            "compare_at_price_jpy",
            "product_url",
            "observed_at",
            "observed_date",
        ],
        """
        ON CONFLICT (product_id, observed_date) DO UPDATE SET
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
    ),
]

POSTGRES_TRUNCATE_ORDER = [
    "prices_hareruya_history",
    "prices_hareruya_current",
    "prices_cardrush_current",
    "prices_cardrush",
    "products_cardrush",
    "ebay_search_results",
    "prices_ebay_history",
    "prices_ebay_current",
    "prices_limitless_history",
    "prices_limitless",
    "cards_index",
    "series_url_jp",
    "series_hareruya",
    "series_limitless",
]


def sqlite_table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def sqlite_columns(conn: sqlite3.Connection, table_name: str) -> list[str]:
    return [str(row[1]) for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()]


def fetch_sqlite_rows(
    conn: sqlite3.Connection,
    table_name: str,
    expected_columns: list[str],
) -> tuple[list[str], list[tuple]]:
    if not sqlite_table_exists(conn, table_name):
        return [], []

    present_columns = set(sqlite_columns(conn, table_name))
    selected_columns = [column for column in expected_columns if column in present_columns]
    if not selected_columns:
        return [], []

    column_sql = ", ".join(selected_columns)
    rows = conn.execute(f"SELECT {column_sql} FROM {table_name}").fetchall()
    return selected_columns, [tuple(row) for row in rows]


def ensure_named_schema(schema_name: str) -> None:
    if not schema_name or schema_name == "public":
        return

    with closing(connect_postgres(schema_name=None)) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                    sql.Identifier(schema_name)
                )
            )
        conn.commit()


def truncate_postgres_tables(schema_name: str | None = None) -> None:
    table_idents = [
        sql.SQL("{}.{}").format(
            sql.Identifier(schema_name or "public"),
            sql.Identifier(table_name),
        )
        for table_name in POSTGRES_TRUNCATE_ORDER
    ]

    with closing(connect_postgres(schema_name=schema_name)) as conn:
        ensure_app_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("TRUNCATE {} RESTART IDENTITY CASCADE").format(
                    sql.SQL(", ").join(table_idents)
                )
            )
        conn.commit()


def migrate_table(
    sqlite_conn: sqlite3.Connection,
    table_name: str,
    expected_columns: list[str],
    conflict_clause: str | None,
    *,
    schema_name: str | None = None,
) -> int:
    columns, rows = fetch_sqlite_rows(sqlite_conn, table_name, expected_columns)
    if not rows:
        return 0

    column_sql = ", ".join(columns)
    insert_sql = (
        f"INSERT INTO {table_name} ({column_sql}) VALUES %s"
        f"{conflict_clause or ''}"
    )

    with closing(connect_postgres(schema_name=schema_name)) as conn:
        ensure_app_schema(conn)
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, rows)
        conn.commit()

    return len(rows)


def run_migration(
    sqlite_path: str | Path,
    *,
    schema_name: str | None = None,
    truncate_existing: bool = False,
) -> dict[str, int]:
    sqlite_db_path = Path(sqlite_path).resolve()
    if not sqlite_db_path.exists():
        raise FileNotFoundError(f"SQLite database not found: {sqlite_db_path}")

    resolved_schema = (schema_name or "").strip() or None
    ensure_named_schema(resolved_schema or "public")

    with closing(connect_postgres(schema_name=resolved_schema)) as conn:
        ensure_app_schema(conn)
        conn.commit()

    if truncate_existing:
        truncate_postgres_tables(schema_name=resolved_schema)

    sqlite_conn = sqlite3.connect(sqlite_db_path)
    try:
        counts: dict[str, int] = {}
        for table_name, columns, conflict_clause in SQLITE_TABLES_IN_LOAD_ORDER:
            count = migrate_table(
                sqlite_conn,
                table_name,
                columns,
                conflict_clause,
                schema_name=resolved_schema,
            )
            counts[table_name] = count
        return counts
    finally:
        sqlite_conn.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Migrate project data from SQLite into PostgreSQL."
    )
    parser.add_argument(
        "--sqlite-path",
        default="ptcg.sqlite",
        help="Path to the source SQLite database (default: ptcg.sqlite)",
    )
    parser.add_argument(
        "--schema-name",
        default=None,
        help="Optional PostgreSQL schema name (default: DB_SCHEMA or public)",
    )
    parser.add_argument(
        "--truncate-existing",
        action="store_true",
        help="Truncate managed PostgreSQL tables before importing SQLite data",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    counts = run_migration(
        sqlite_path=args.sqlite_path,
        schema_name=args.schema_name,
        truncate_existing=args.truncate_existing,
    )

    print("SQLite -> PostgreSQL migration complete.")
    for table_name, count in counts.items():
        print(f"- {table_name}: {count} rows")


if __name__ == "__main__":
    main()
