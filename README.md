# Kanen Coffee Backend

FastAPI-based API service that powers the Repair Intelligence & Inventory Planning platform.

## Features (planned)
- Scheduled ingestion jobs for QuickBooks + Gmail data sources
- Canonical inventory + repairs database (PostgreSQL)
- Analytics endpoints for dashboards (failure modes, stock recommendations, vendor KPIs)
- Auth via Cloudflare Access headers + API tokens

## Local Development
1. Install dependencies (using [uv](https://github.com/astral-sh/uv) or pip):
   ```bash
   cd backend
   uv pip install -r pyproject.toml
   ```
2. Copy `.env.example` to `.env` and fill in the required settings (database URL, OAuth credentials, etc.).
3. Run the API locally:
   ```bash
   uvicorn app.main:app --reload
   ```

## Structure
```
backend/
├── app/
│   ├── api/        # APIRouter definitions
│   ├── core/       # config, logging, settings
│   ├── services/   # QBO, Gmail, forecasting logic (coming soon)
│   ├── models/     # SQLModel definitions (coming soon)
│   └── main.py     # FastAPI entrypoint
├── pyproject.toml
└── README.md
```

## Migrations

```bash
cd backend
../.venv/bin/alembic upgrade head  # apply
../.venv/bin/alembic revision --autogenerate -m "message"  # new rev
```

## Ingestion helper

To run a full data pull (QuickBooks + Gmail) from the repo root:

```bash
./scripts/run_sync.py --quickbooks --changed-since 2025-12-01T00:00:00-08:00 \
  --gmail --gmail-pages 3
```

Use `--gmail-account` to target a specific mailbox (flag can be repeated).

Or trigger jobs remotely with the new API endpoint (requires API key):

```bash
curl -X POST http://localhost:8000/v1/system/run-sync \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"quickbooks": true, "gmail": true}'
```

## Deployment

- Procfile defines `web` (FastAPI) and `worker` (ingestion job) entries.
- `railway.toml` is pre-populated for Nixpacks so you can run `railway up` and get both services.
- Healthcheck path: `/v1/status`.

## Testing

```bash
cd backend
../.venv/bin/pytest
```

## Security

Set `API_KEY` (and optionally `API_KEY_HEADER`, default `X-API-Key`) in `.env` to protect sensitive endpoints like `/v1/system/sync-runs`. Add the same header in frontend fetches or ops tooling when you enable it.
