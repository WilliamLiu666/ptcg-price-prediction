CREATE TABLE IF NOT EXISTS cards_index (
  card_id BIGINT PRIMARY KEY,
  data_id BIGINT,
  lang TEXT NOT NULL,
  set_code TEXT NOT NULL,
  card_code TEXT NOT NULL,
  card_name TEXT,
  rarity TEXT,
  UNIQUE(lang, set_code, card_code)
);

CREATE INDEX IF NOT EXISTS idx_cards_index_lang_set
  ON cards_index(lang, set_code);

CREATE INDEX IF NOT EXISTS idx_cards_index_rarity
  ON cards_index(rarity);

CREATE TABLE IF NOT EXISTS prices_limitless (
  card_id BIGINT PRIMARY KEY,
  data_id BIGINT,
  lang TEXT NOT NULL,
  set_code TEXT NOT NULL,
  card_code TEXT NOT NULL,
  card_name TEXT,
  rarity TEXT,
  usd_price DOUBLE PRECISION,
  eur_price DOUBLE PRECISION,
  ebay_price DOUBLE PRECISION,
  observed_at TEXT,
  observed_date TEXT,
  created_at TEXT,
  updated_at TEXT,
  ebay_observed_at TEXT,
  ebay_observed_date TEXT,
  UNIQUE(lang, set_code, card_code)
);

CREATE TABLE IF NOT EXISTS prices_limitless_history (
  id BIGSERIAL PRIMARY KEY,
  card_id BIGINT,
  lang TEXT NOT NULL,
  set_code TEXT NOT NULL,
  card_code TEXT NOT NULL,
  usd_price DOUBLE PRECISION,
  eur_price DOUBLE PRECISION,
  ebay_price DOUBLE PRECISION,
  source TEXT NOT NULL DEFAULT 'limitless',
  observed_at TEXT NOT NULL,
  observed_date TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_prices_limitless_lang_set
  ON prices_limitless(lang, set_code);

CREATE INDEX IF NOT EXISTS idx_prices_limitless_observed_date
  ON prices_limitless(observed_date);

CREATE INDEX IF NOT EXISTS idx_prices_limitless_history_card_date
  ON prices_limitless_history(card_id, observed_date);

CREATE UNIQUE INDEX IF NOT EXISTS uq_prices_limitless_history_card_source_date
  ON prices_limitless_history(lang, set_code, card_code, source, observed_date);

CREATE TABLE IF NOT EXISTS prices_ebay_current (
  card_id BIGINT,
  lang TEXT NOT NULL,
  set_code TEXT NOT NULL,
  card_code TEXT NOT NULL,
  card_name TEXT,
  marketplace_id TEXT NOT NULL DEFAULT 'EBAY_GB',
  currency TEXT NOT NULL DEFAULT 'GBP',
  condition TEXT,
  selected_item_id TEXT,
  selected_title TEXT,
  selected_item_web_url TEXT,
  ebay_price DOUBLE PRECISION,
  observed_at TEXT NOT NULL,
  observed_date TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(lang, set_code, card_code, marketplace_id, currency)
);

CREATE TABLE IF NOT EXISTS prices_ebay_history (
  id BIGSERIAL PRIMARY KEY,
  card_id BIGINT,
  lang TEXT NOT NULL,
  set_code TEXT NOT NULL,
  card_code TEXT NOT NULL,
  card_name TEXT,
  marketplace_id TEXT NOT NULL DEFAULT 'EBAY_GB',
  currency TEXT NOT NULL DEFAULT 'GBP',
  condition TEXT,
  selected_item_id TEXT,
  selected_title TEXT,
  selected_item_web_url TEXT,
  ebay_price DOUBLE PRECISION,
  ebay_observed_at TEXT NOT NULL,
  ebay_observed_date TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ebay_search_results (
  id BIGSERIAL PRIMARY KEY,
  keyword TEXT NOT NULL,
  item_id TEXT,
  title TEXT,
  price_value DOUBLE PRECISION,
  currency TEXT,
  item_web_url TEXT,
  condition TEXT,
  observed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_prices_ebay_current_card_market
  ON prices_ebay_current(card_id, marketplace_id, currency);

CREATE INDEX IF NOT EXISTS idx_prices_ebay_current_observed_date
  ON prices_ebay_current(observed_date);

CREATE INDEX IF NOT EXISTS idx_prices_ebay_history_card_date
  ON prices_ebay_history(card_id, ebay_observed_date);

CREATE INDEX IF NOT EXISTS idx_prices_ebay_history_market_date
  ON prices_ebay_history(marketplace_id, ebay_observed_date);

CREATE INDEX IF NOT EXISTS idx_ebay_search_keyword_observed_at
  ON ebay_search_results(keyword, observed_at);

CREATE UNIQUE INDEX IF NOT EXISTS uq_prices_ebay_history_card_market_date
  ON prices_ebay_history(
    lang,
    set_code,
    card_code,
    marketplace_id,
    currency,
    ebay_observed_date
  );

CREATE TABLE IF NOT EXISTS prices_hareruya_current (
  product_id TEXT PRIMARY KEY,
  collection_id TEXT,
  set_code TEXT,
  card_number TEXT,
  card_name_jp TEXT,
  card_name_en TEXT,
  variant_title TEXT,
  currency TEXT NOT NULL DEFAULT 'JPY',
  price_jpy DOUBLE PRECISION,
  compare_at_price_jpy DOUBLE PRECISION,
  product_url TEXT,
  observed_at TEXT NOT NULL,
  observed_date TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS prices_hareruya_history (
  id BIGSERIAL PRIMARY KEY,
  product_id TEXT NOT NULL,
  collection_id TEXT,
  set_code TEXT,
  card_number TEXT,
  card_name_jp TEXT,
  card_name_en TEXT,
  variant_title TEXT,
  currency TEXT NOT NULL DEFAULT 'JPY',
  price_jpy DOUBLE PRECISION,
  compare_at_price_jpy DOUBLE PRECISION,
  product_url TEXT,
  observed_at TEXT NOT NULL,
  observed_date TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_prices_hareruya_current_set_code
  ON prices_hareruya_current(set_code);

CREATE INDEX IF NOT EXISTS idx_prices_hareruya_history_product_date
  ON prices_hareruya_history(product_id, observed_date);

CREATE INDEX IF NOT EXISTS idx_prices_hareruya_history_set_date
  ON prices_hareruya_history(set_code, observed_date);

CREATE UNIQUE INDEX IF NOT EXISTS uq_prices_hareruya_history_product_date
  ON prices_hareruya_history(product_id, observed_date);

CREATE TABLE IF NOT EXISTS products_cardrush (
  product_id TEXT PRIMARY KEY,
  product_group TEXT NOT NULL,
  model_number TEXT NOT NULL,
  set_size TEXT,
  name TEXT NOT NULL,
  name_full TEXT NOT NULL,
  condition TEXT,
  model_code TEXT,
  price_yen BIGINT,
  url TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS prices_cardrush_current (
  product_id TEXT PRIMARY KEY,
  price_yen BIGINT NOT NULL,
  price_text TEXT,
  observed_at TEXT NOT NULL,
  observed_date TEXT NOT NULL,
  source TEXT NOT NULL DEFAULT 'cardrush',
  updated_at TEXT NOT NULL,
  FOREIGN KEY (product_id) REFERENCES products_cardrush(product_id)
);

CREATE TABLE IF NOT EXISTS prices_cardrush (
  product_id TEXT NOT NULL,
  observed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  observed_date TEXT,
  price_yen BIGINT NOT NULL,
  price_text TEXT,
  source TEXT NOT NULL DEFAULT 'cardrush',
  PRIMARY KEY (product_id, observed_at),
  FOREIGN KEY (product_id) REFERENCES products_cardrush(product_id)
);

CREATE INDEX IF NOT EXISTS idx_products_cardrush_group
  ON products_cardrush(product_group);

CREATE INDEX IF NOT EXISTS idx_products_cardrush_model_number
  ON products_cardrush(model_number);

CREATE INDEX IF NOT EXISTS idx_prices_cardrush_current_observed_date
  ON prices_cardrush_current(observed_date);

CREATE INDEX IF NOT EXISTS idx_prices_cardrush_observed_at
  ON prices_cardrush(observed_at);

CREATE TABLE IF NOT EXISTS series_limitless (
  series_code VARCHAR(50) NOT NULL,
  lang VARCHAR(10) NOT NULL,
  size INTEGER,
  PRIMARY KEY (series_code, lang)
);

CREATE TABLE IF NOT EXISTS series_hareruya (
  series_code VARCHAR(50) PRIMARY KEY,
  collection VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS series_url_jp (
  series_code TEXT PRIMARY KEY,
  series_name TEXT NOT NULL,
  source TEXT NOT NULL,
  list_url TEXT NOT NULL,
  active INTEGER DEFAULT 1
);
