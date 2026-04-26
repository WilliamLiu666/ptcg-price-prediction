from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import connection as PgConnection

from app.config import PostgresSettings, get_postgres_settings


def connect_postgres(
    settings: PostgresSettings | None = None,
    *,
    schema_name: str | None = None,
) -> PgConnection:
    resolved_settings = settings or get_postgres_settings()
    resolved_schema = schema_name or resolved_settings.schema

    options = None
    if resolved_schema:
        options = f"-c search_path={resolved_schema},public"

    conn = psycopg2.connect(
        host=resolved_settings.host,
        port=resolved_settings.port,
        dbname=resolved_settings.dbname,
        user=resolved_settings.user,
        password=resolved_settings.password,
        options=options,
    )
    conn.autocommit = False
    return conn


@contextmanager
def dict_cursor(conn: PgConnection) -> Iterator[RealDictCursor]:
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        yield cursor
    finally:
        cursor.close()


def table_exists(conn: PgConnection, table_name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (table_name,))
        row = cur.fetchone()
    return bool(row and row[0])
