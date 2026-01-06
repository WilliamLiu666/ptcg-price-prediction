import os
import sqlite3

DB_PATH = "ptcg.sqlite"

SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE products (
  product_id  TEXT PRIMARY KEY,
  url         TEXT NOT NULL,
  name        TEXT NOT NULL,
  created_at  TEXT NOT NULL,
  updated_at  TEXT NOT NULL
);

CREATE TABLE price_history (
  product_id     TEXT NOT NULL,
  captured_date  TEXT NOT NULL,  -- YYYY-MM-DD (UTC)
  captured_at    TEXT NOT NULL,  -- UTC ISO timestamp
  price          REAL,
  currency       TEXT DEFAULT 'JPY',
  stock_status   TEXT,
  PRIMARY KEY (product_id, captured_date),
  FOREIGN KEY (product_id) REFERENCES products(product_id)
);

CREATE INDEX idx_price_history_pid_date
ON price_history (product_id, captured_date);
"""

def reset_db():
    # 1️⃣ 删旧库（如果存在）
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print("🗑️ 已删除旧数据库")

    # 2️⃣ 新建数据库 + 建表
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.executescript(SCHEMA_SQL)

    print("✅ 新数据库初始化完成")

if __name__ == "__main__":
    reset_db()
