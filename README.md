# PTCG Price Prediction

## Raw data layout

Raw extractor responses are stored as immutable snapshots under:

`Data/raw/<source>/<dataset>/YYYY/MM/DD/`

Examples:

- `Data/raw/limitless/cards_html/2026/03/29/`
- `Data/raw/limitless/sets_html/2026/03/29/`
- `Data/raw/cardrush/product_group_html/2026/03/29/`
- `Data/raw/hareruya/collections/2026/03/29/`

## Staging (Parquet)

Normalized Limitless rows are written under:

`Data/staging/limitless/<dataset>/extract_date=YYYY-MM-DD/<lang>_<set>_<card>.parquet`

Datasets: `cards_normalized`, `card_index`, `price_events`. Each row includes `source`, `dataset`, `extract_date`, `observed_at`, and `observed_date`.

Install Parquet dependencies:

```bash
pip install -r requirements.txt
```

Run the Limitless batch with a partition date (defaults to `EXTRACT_DATE` env or today UTC):

```bash
python -m app.jobs.limitless_batch_job --extract-date 2026-03-29
```

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
python -m app.jobs.ebay_price_job
```

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
