# PTCG Price Prediction

## Raw data layout

Raw extractor responses are stored as immutable snapshots under:

`Data/raw/<source>/<dataset>/YYYY/MM/DD/`

Examples:

- `Data/raw/limitless/cards_html/2026/03/29/`
- `Data/raw/limitless/sets_html/2026/03/29/`
- `Data/raw/ebay/search_json/2026/03/29/`
- `Data/raw/cardrush/product_group_html/2026/03/29/`
- `Data/raw/hareruya/collections/2026/03/29/`

## Staging (Parquet)

Normalized source rows are written under:

`Data/staging/<source>/<dataset>/extract_date=YYYY-MM-DD/<business-key>.parquet`

Sources currently using this layout: `limitless`, `hareruya`, `ebay`.

Datasets: `cards_normalized`, `card_index`, `price_events`. Each row includes `source`, `dataset`, `extract_date`, `observed_at`, and `observed_date`.

Install Parquet dependencies:

```bash
pip install -r requirements.txt
```

Run the Limitless batch with a partition date (defaults to `EXTRACT_DATE` env or today UTC):

```bash
python -m app.jobs.limitless_batch_job --extract-date 2026-03-29
```

Run all three source batches from one main entry point:

```bash
python -m app --extract-date 2026-03-29
```

You can also run only a subset of sources:

```bash
python -m app --extract-date 2026-03-29 --sources limitless hareruya
```

`--extract-date` now has two modes:

- If `extract_date` is today in UTC, the pipeline may fetch missing raw files from the web and will update current price tables.
- If `extract_date` is earlier than today in UTC, the pipeline becomes a historical replay: it only uses cached raw files under `Data/raw/...` and does not overwrite current price tables. If the raw files for that date do not exist locally, the run will fail instead of fetching newer live data into an older date partition.

The Limitless batch now also caches the set listing page for each series under
`Data/raw/limitless/sets_html/...` so it can discover the actual card codes for
that set instead of assuming the codes are always a perfect `1..size` range.

## eBay credentials setup

This project now auto-loads a local `.env` file from the repository root.

Create a `.env` file (same folder as `README.md`) with:

```env
EBAY_CLIENT_ID=your-client-id
EBAY_CLIENT_SECRET=your-client-secret
EBAY_SANDBOX=false
```

Environment variables set in your shell still work and take priority over `.env`.

Supported keys:

- `EBAY_CLIENT_ID`
- `EBAY_CLIENT_SECRET`
- `EBAY_SANDBOX` (optional, `true`/`false`, default is `false`)

Then run the eBay batch job:

```bash
python -m app.jobs.ebay_batch_job --extract-date 2026-03-29
```

The eBay batch now also caches raw search responses under
`Data/raw/ebay/search_json/...` and writes normalized staging parquet under
`Data/staging/ebay/...`, while still updating `prices_limitless.ebay_price`.

## SQLite price tables

SQLite keeps source-specific current/history tables instead of one shared price table:

- `prices_limitless` and `prices_limitless_history`
- `prices_ebay_current` and `prices_ebay_history`
- `prices_cardrush_current` and `prices_cardrush`
- `prices_hareruya_current` and `prices_hareruya_history`

### Optional: set directly in shell (overrides `.env`)

PowerShell:

```powershell
$env:EBAY_CLIENT_ID="your-client-id"
$env:EBAY_CLIENT_SECRET="your-client-secret"
$env:EBAY_SANDBOX="false"
```

Git Bash:

```bash
export EBAY_CLIENT_ID="your-client-id"
export EBAY_CLIENT_SECRET="your-client-secret"
export EBAY_SANDBOX="false"
```

If credentials are missing, the job now fails early with a clear error.

## Tests

There is a small `unittest` fixture suite under `tests/` covering:

- Limitless card-page parsing
- Limitless set-listing card-code discovery
- Hareruya paginated `products.json` aggregation

Run it with:

```bash
python -m unittest discover -s tests
```
