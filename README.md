# PTCG Price Prediction

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