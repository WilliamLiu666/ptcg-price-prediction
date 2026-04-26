from __future__ import annotations

import uuid
from contextlib import closing, contextmanager
from typing import Iterator

from psycopg2 import sql
from psycopg2.extensions import connection as PgConnection

from app.utils.postgres_db import connect_postgres


@contextmanager
def temporary_schema() -> Iterator[str]:
    schema_name = f"test_{uuid.uuid4().hex}"

    with closing(connect_postgres(schema_name=None)) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name))
            )
        conn.commit()

    try:
        yield schema_name
    finally:
        with closing(connect_postgres(schema_name=None)) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                        sql.Identifier(schema_name)
                    )
                )
            conn.commit()


def connect_schema(schema_name: str) -> PgConnection:
    return connect_postgres(schema_name=schema_name)


def column_names(conn: PgConnection, table_name: str) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table_name,),
        )
        return [str(row[0]) for row in cur.fetchall()]


def foreign_keys(conn: PgConnection, table_name: str) -> list[tuple[str, str, str]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              kcu.column_name,
              ccu.table_name AS foreign_table_name,
              ccu.column_name AS foreign_column_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
              ON ccu.constraint_name = tc.constraint_name
             AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_schema = current_schema()
              AND tc.table_name = %s
            ORDER BY kcu.ordinal_position
            """,
            (table_name,),
        )
        return [
            (str(row[0]), str(row[1]), str(row[2]))
            for row in cur.fetchall()
        ]


def table_exists(conn: PgConnection, table_name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (table_name,))
        row = cur.fetchone()
    return bool(row and row[0])


def index_names(conn: PgConnection, table_name: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT indexname
            FROM pg_indexes
            WHERE schemaname = current_schema()
              AND tablename = %s
            """,
            (table_name,),
        )
        return {str(row[0]) for row in cur.fetchall()}
