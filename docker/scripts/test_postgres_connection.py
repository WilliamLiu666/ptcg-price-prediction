from __future__ import annotations

import sys
from contextlib import closing
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.utils.postgres_db import connect_postgres
from app.utils.postgres_schema import ensure_app_schema


with closing(connect_postgres()) as conn:
    ensure_app_schema(conn)
    conn.commit()

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT current_database(), current_user, current_schema()
            """
        )
        print(cur.fetchone())

        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = current_schema()
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """
        )
        print([row[0] for row in cur.fetchall()])
