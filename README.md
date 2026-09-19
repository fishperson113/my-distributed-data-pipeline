# My Distributed Data Pipeline

Self-hosted Dagster application for a multi-source data pipeline running on a personal VPS.

## Current phase

Phase 1 establishes the deployment foundation only:

- Dagster webserver, daemon, and gRPC code server.
- PostgreSQL-backed Dagster metadata.
- One deployment healthcheck asset.
- Docker Compose deployment for local verification and the VPS.

Stock and fund raw landing assets are now available. Bronze database migrations,
notebooks, and dbt remain deferred.

Standalone source probes call the same reusable ingestion modules as the Dagster
assets, allowing source connectivity to be tested without running Dagster.

## Local setup

```bash
uv sync
uv run pytest
```

Run Dagster without containers:

```bash
uv run dagster dev -m data_pipeline.definitions
```

Run the production-shaped stack locally:

```bash
cp .env.example .env
docker compose up --build -d
docker compose ps
```

Dagster UI is available at `http://localhost:3000` by default.

## Test the upstream data sources

Fetch daily stock OHLCV through `vnstock` (KBS by default):

```bash
uv run python scripts/test_stock_source.py --symbol FPT --start 2026-09-01 --end 2026-09-19
```

Use `--symbol` more than once to test multiple tickers, or pass `--provider vci` to test the VCI provider.

Fetch daily ETF data for `E1VFVN30` through the SSI public-facing history endpoint:

```bash
uv run python scripts/test_fund_source.py --symbol E1VFVN30 --start 2026-09-01 --end 2026-09-19
```

The scripts write JSON envelopes under `storage/raw/vnstock/` and `storage/raw/ssi/`. Runtime data in these directories is ignored by Git.

Source defaults are managed in `.env`: vnstock provider and telemetry, SSI endpoint,
HTTP timeout/retry policy, and the raw storage root. Copy `.env.example` to `.env`
for local use. Explicit CLI arguments such as `--provider`, `--timeout`, `--attempts`,
and `--output` override those defaults for an individual run; symbols and date ranges
remain run parameters rather than environment configuration.

## Scheduled market ingestion

Dagster registers two daily-partitioned raw landing assets:

- `raw/stock_daily`, using the comma-separated `STOCK_SYMBOLS` setting.
- `raw/fund_daily`, using the `FUND_SYMBOL` setting.

Configure their symbols in `.env`:

```dotenv
STOCK_SYMBOLS=FPT,VNM,HPG
FUND_SYMBOL=E1VFVN30
```

`STOCK_SYMBOLS` is a comma-separated list. For each daily partition, the stock
asset normalizes the values to uppercase, removes duplicates, then asks vnstock
for every configured ticker. For example, `FPT,VNM,HPG` crawls all three stocks
and stores their records together in that partition's raw JSON envelope. Spaces
around values are allowed, but an empty list is rejected.

The project's stock adapter exposes an inclusive date contract. Internally it
adds one day to the requested end date because the vnstock/KBS endpoint treats
that boundary as exclusive, then filters the response back to the requested
partition. A partition such as `2026-09-18` therefore requests the provider
window `[2026-09-18, 2026-09-19)` and stores only records for `2026-09-18`.

`FUND_SYMBOL` is deliberately singular in the current implementation. It accepts
one fund/ETF ticker, so `E1VFVN30` crawls only that ticker through SSI. Supplying
`E1VFVN30,FUEVFVND` does not request two funds; it is treated as one invalid
symbol. Supporting multiple funds later should use a separate plural setting
such as `FUND_SYMBOLS` and preserve the same per-source raw contract.

The `daily_market_ingestion` job selects both assets. Its
`daily_market_ingestion_schedule` runs at 06:00 in `Asia/Ho_Chi_Minh` and targets
the previous day's partition. The schedule is intentionally deployed in the
stopped state; review it in Dagster UI and enable it when ready.

Raw files are bind-mounted to `storage/raw/` in the repository on the VPS, with
the same path mounted at `/opt/dagster/app/storage/raw` inside Dagster containers.
They therefore remain directly inspectable and can be backed up with ordinary
host filesystem tools. Runtime payloads remain ignored by Git; only `.gitkeep`
is tracked. These are landing payloads, not Bronze database tables; Bronze
persistence remains a later implementation phase.

## Documentation

- [Implementation plan](artifacts/devlog/implementation_plan.md)
- [Architecture](docs/architecture.md)
- [Local development](docs/local-development.md)
- [VPS deployment](docs/vps-deployment.md)
- [Module 4 proposal](artifacts/devlog/module-4-proposal.md)
- [Bronze design review](artifacts/devlog/module-4-bronze-design-review.md)
