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
make prod-up
make prod-ps
```

On Windows PowerShell, use the checked-in PowerShell wrapper instead:

```powershell
Copy-Item .env.example .env
./make.ps1 prod-up
./make.ps1 prod-ps
```

Dagster UI is available at `http://localhost:3000` by default.

## Infrastructure wrappers

`Makefile` and `make.ps1` are thin wrappers around Docker Compose.
They exist to make full and partial infrastructure startup consistent across Unix-like shells and Windows PowerShell.
They do not replace the Compose files as the source of truth.

The production-shaped stack uses `compose.yml`.
It includes Dagster metadata Postgres, warehouse Postgres, warehouse migrations, the Dagster gRPC code server, the Dagster webserver, and the Dagster daemon.

The local ELT compatibility stack uses `compose.dev.yml`.
It includes MongoDB and a host-accessible warehouse Postgres for local dbt and compatibility work.
It is independent from the Dagster production-shaped stack.

Common Make targets:

```bash
make help
make prod-up
make prod-down
make prod-ps
make prod-logs
make prod-config
make dev-up
make dev-down
make dev-ps
make dev-logs
make dev-config
make all-up
make all-down
make all-ps
make config
make ingest-stock
make ingest-fund
make ingest-market
```

Common PowerShell targets:

```powershell
./make.ps1 help
./make.ps1 prod-up
./make.ps1 prod-down
./make.ps1 prod-ps
./make.ps1 prod-logs
./make.ps1 prod-config
./make.ps1 dev-up
./make.ps1 dev-down
./make.ps1 dev-ps
./make.ps1 dev-logs
./make.ps1 dev-config
./make.ps1 all-up
./make.ps1 all-down
./make.ps1 all-ps
./make.ps1 config
./make.ps1 ingest-stock
./make.ps1 ingest-fund
./make.ps1 ingest-market
```

Start only part of a stack by passing service names.
This is useful when you only need databases or a local utility service.

```bash
make prod-up SERVICES="postgres warehouse-postgres"
make dev-up SERVICES=mongo
```

```powershell
./make.ps1 prod-up -Services postgres,warehouse-postgres
./make.ps1 dev-up -Services mongo
```

The Make wrapper can be configured with environment variables.
`SERVICES` limits a target to selected services.
`BUILD` controls the production `up --build` flag, so use `BUILD=` to skip building.
`DETACH` controls the `-d` flag, so use `DETACH=` to run attached.
`LOG_ARGS` passes extra arguments to `docker compose logs`.
`COMPOSE`, `PROD_COMPOSE_FILE`, and `DEV_COMPOSE_FILE` override the compose command or compose file paths.

```bash
make prod-up BUILD=
make prod-up DETACH=
make prod-logs LOG_ARGS="-f --tail=100" SERVICES=dagster-webserver
```

The PowerShell wrapper has equivalent parameters.
Startup commands do not rebuild images by default.
Use `BUILD=--build` with Make or `-Build` with PowerShell when images must be rebuilt.
Use `-NoDetach` to run attached.
Use `-LogArgs` to pass extra log options.
Use `-Compose`, `-ProdComposeFile`, and `-DevComposeFile` to override the compose command or compose file paths.

```powershell
./make.ps1 prod-up -Build
./make.ps1 prod-up -NoDetach
./make.ps1 prod-logs -LogArgs '-f','--tail=100' -Services dagster-webserver
```

Run `make config` or `./make.ps1 config` after Compose-related changes to render both Compose configurations.
These commands validate configuration shape without starting containers.

## Test the upstream data sources

Fetch daily stock OHLCV through `vnstock` (KBS by default):

```bash
uv run python scripts/test_stock_source.py --symbol FPT --start 2026-09-01 --end 2026-09-19
```

Use `--symbol` more than once to test multiple tickers, or pass `--provider vci` to test the VCI provider.

Run the same stock probe through the wrappers:

```bash
make ingest-stock STOCK_ARGS="--symbol FPT --start 2026-09-01 --end 2026-09-19"
```

```powershell
./make.ps1 ingest-stock -StockArgs @('--symbol','FPT','--start','2026-09-01','--end','2026-09-19')
```

Fetch daily ETF data for `E1VFVN30` through the SSI public-facing history endpoint:

```bash
uv run python scripts/test_fund_source.py --symbol E1VFVN30 --start 2026-09-01 --end 2026-09-19
```

Or use wrapper targets for fund extraction or both source probes:

```bash
make ingest-fund FUND_ARGS="--symbol E1VFVN30 --start 2026-09-01 --end 2026-09-19"
make ingest-market STOCK_ARGS="--symbol FPT --start 2026-09-01 --end 2026-09-19" FUND_ARGS="--symbol E1VFVN30 --start 2026-09-01 --end 2026-09-19"
```

```powershell
./make.ps1 ingest-fund -FundArgs @('--symbol','E1VFVN30','--start','2026-09-01','--end','2026-09-19')
./make.ps1 ingest-market -StockArgs @('--symbol','FPT','--start','2026-09-01','--end','2026-09-19') -FundArgs @('--symbol','E1VFVN30','--start','2026-09-01','--end','2026-09-19')
```

The scripts write JSON envelopes under `storage/raw/vnstock/` and `storage/raw/ssi/`. Runtime data in these directories is ignored by Git.

### Load raw envelopes into Postgres Bronze

After the warehouse migrations have been applied and `WAREHOUSE_POSTGRES_DSN` is configured, load a raw JSON file or a designated raw-data directory into the corresponding `bronze` table:

```bash
uv run python scripts/load_postgres_raw.py --path storage/raw/ssi
```

Repeat `--path` to load multiple files or directories.
The script scans directories recursively, derives the partition date from a `date=YYYY-MM-DD` path component or the envelope's `request.end` field, and preserves the raw file path in the Bronze ingestion batch.

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
