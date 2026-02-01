CREATE TABLE IF NOT EXISTS products (
  product_id  TEXT PRIMARY KEY,
  url         TEXT NOT NULL,
  name        TEXT NOT NULL,
  created_at  TEXT NOT NULL,
  updated_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS price_history (
  product_id     TEXT NOT NULL,
  captured_date  TEXT NOT NULL,  -- YYYY-MM-DD，用于“每天一条”
  captured_at    TEXT NOT NULL,  -- 真正抓取时间（UTC ISO）
  price          REAL,
  currency       TEXT DEFAULT 'JPY',
  stock_status   TEXT,
  PRIMARY KEY (product_id, captured_date),
  FOREIGN KEY (product_id) REFERENCES products(product_id)
);

CREATE INDEX IF NOT EXISTS idx_price_history_pid_date
ON price_history (product_id, captured_date);
