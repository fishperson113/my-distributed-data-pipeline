# My Distributed Data Pipeline

Self-hosted Dagster application for a multi-source data pipeline running on a personal VPS.

## Current phase

Phase 1 prioritizes a reliable self-hosted Dagster deployment on the VPS:

- Dagster webserver, daemon, and gRPC code server.
- PostgreSQL-backed Dagster metadata.
- One deployment healthcheck asset.
- Docker Compose deployment for local verification and the VPS.

Stock and fund raw landing assets are available as an explicit scope expansion.
Standalone source probes call the same reusable ingestion modules as the Dagster
assets, allowing source connectivity to be tested without running Dagster.

Postgres Bronze is deployed separately from Dagster metadata. Daily Dagster
assets persist raw stock and fund envelopes into its `bronze` schema after the
filesystem landing step. dbt transforms directly from Postgres Bronze into
Postgres staging and marts. Mongo remains an optional compatibility utility,
outside the production pipeline.

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

Deployment and infrastructure settings are managed in `.env`: database access,
ports, external endpoint, HTTP timeout/retry policy, telemetry, raw storage path,
and the path to `config.yml`. Copy `.env.example` to `.env` for local use. The
versioned `config.yml` contains non-secret operating policy: what to crawl,
partition boundaries, schedule time, timezone, and default schedule status.
Explicit CLI arguments still override source defaults for an individual probe.

## Scheduled market ingestion

Dagster registers four daily-partitioned assets:

- `raw/stock_daily`, using the stock batch in `config.yml`.
- `raw/fund_daily`, using the fund symbol in `config.yml`.
- `bronze/stock_daily`, which writes the landed stock envelope to Postgres.
- `bronze/fund_daily`, which writes the landed fund envelope to Postgres.

Configure operating policy in `config.yml`:

```yaml
ingestion:
  stock:
    provider: kbs
    symbols:
      - FPT
      - VNM
      - HPG
  fund:
    symbol: E1VFVN30

partitions:
  daily_market:
    start_date: "2020-01-01"
    timezone: Asia/Ho_Chi_Minh

schedules:
  daily_market_ingestion:
    hour: 6
    minute: 0
    enabled_by_default: false
```

For each daily partition, the stock asset normalizes `symbols` to uppercase,
removes duplicates, then asks vnstock for every configured ticker. The example
crawls `FPT`, `VNM`, and `HPG`, storing their records together in that partition's
raw JSON envelope. An empty list is rejected when Dagster loads the code location.

The project's stock adapter exposes an inclusive date contract. Internally it
adds one day to the requested end date because the vnstock/KBS endpoint treats
that boundary as exclusive, then filters the response back to the requested
partition. A partition such as `2026-09-18` therefore requests the provider
window `[2026-09-18, 2026-09-19)` and stores only records for `2026-09-18`.

`ingestion.fund.symbol` is deliberately singular in the current implementation.
It accepts one fund/ETF ticker, so `E1VFVN30` crawls only that ticker through SSI.

The `daily_market_ingestion` job selects all four assets. Its
`daily_market_ingestion_schedule` reads its hour/minute from `config.yml`; its
timezone comes from the daily partition policy. With the example above it runs
at 06:00 in `Asia/Ho_Chi_Minh` and targets the previous day's partition.
`enabled_by_default: false` deploys it stopped for review in Dagster UI; set it
to `true` to make a fresh Dagster schedule default to running. Changes to this
file require redeploying/reloading the Dagster code location.

Raw files are bind-mounted to `storage/raw/` in the repository on the VPS, with
the same path mounted at `/opt/dagster/app/storage/raw` inside Dagster containers.
They therefore remain directly inspectable and can be backed up with ordinary
host filesystem tools. Runtime payloads remain ignored by Git; only `.gitkeep`
is tracked. The production compose stack also runs a dedicated warehouse
Postgres service. It applies migrations in `migrations/` before Dagster starts,
then writes append-only raw records to `bronze.stock` and `bronze.fund`, linked
to `bronze.ingestion_batch`. Dagster metadata and
warehouse data therefore remain in separate databases. The legacy local ELT
flow can still mirror landing payloads into MongoDB.

`storage/` is reserved for durable pipeline files only: `storage/raw/` for
source envelopes and `storage/backups/` for operator-managed backups. Postgres
owns warehouse data in a Docker volume.

## Documentation

- [Implementation plan](artifacts/devlog/implementation_plan.md)
- [Architecture](docs/architecture.md)
- [Local development](docs/local-development.md)
- [VPS deployment](docs/vps-deployment.md)
- [Local ELT development](docs/local-elt.md)
- [Module 4 proposal](artifacts/devlog/module-4-proposal.md)
- [Bronze design review](artifacts/devlog/module-4-bronze-design-review.md)
- [Bronze design supersession](artifacts/devlog/module-4-bronze-design-supersession.md)
