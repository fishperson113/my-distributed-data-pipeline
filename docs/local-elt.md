# Postgres dbt development

The production transformation path is Postgres only:

```text
Dagster raw landing -> Postgres bronze -> dbt-postgres -> Postgres staging/marts
```

`compose.yml` runs the production-shaped warehouse Postgres service and applies
the migrations before Dagster starts. For a local dbt session from the host,
`compose.dev.yml` exposes an equivalent warehouse Postgres instance on port
`5433` and applies `migrations/` when its volume is initialized.

## Local ELT flow (raw -> bronze -> dbt)

Run every command from the repository root so the relative paths resolve.

```bash
uv sync --group elt

# 1) Start the local warehouse (publishes localhost:5433)
docker compose -f compose.dev.yml up -d warehouse-postgres

# 2) Apply the bronze migrations (idempotent)
uv run python scripts/migrate_warehouse.py

# 3) Load raw JSON envelopes into bronze.*
#    Without --path it scans storage/raw/<source>/<dataset>/date=YYYY-MM-DD/*.json
uv run python scripts/load_postgres_raw.py \
  --path storage/raw/vnstock/stock_daily_2026-09-01_2026-09-19.json \
  --path storage/raw/ssi/E1VFVN30_daily_2026-09-01_2026-09-19.json

# 4) Build and test the dbt models
uv run dbt run  --project-dir src/data_pipeline/dbt --profiles-dir src/data_pipeline/dbt
uv run dbt test --project-dir src/data_pipeline/dbt --profiles-dir src/data_pipeline/dbt
```

### Or use the make wrappers

```bash
make load-raw LOAD_ARGS="--path storage/raw/vnstock/stock_daily_2026-09-01_2026-09-19.json"
make dbt-run
make dbt-test
```

```powershell
./make.ps1 load-raw -LoadArgs @('--path','storage/raw/vnstock/stock_daily_2026-09-01_2026-09-19.json')
./make.ps1 dbt-run
./make.ps1 dbt-test
```

Set `WAREHOUSE_POSTGRES_USER`, `WAREHOUSE_POSTGRES_PASSWORD`,
`WAREHOUSE_POSTGRES_DB`, and `WAREHOUSE_POSTGRES_PORT` in `.env`. The dbt
profile reads those values directly.

## Output schemas

`macros/generate_schema_name.sql` overrides the dbt default, so models land in
bare schemas (no `<target>_` prefix):

| Model | Materialization | Relation |
| --- | --- | --- |
| `stg_vnstock__stock_daily` | view | `staging.stg_vnstock__stock_daily` |
| `stg_ssi__fund_daily` | view | `staging.stg_ssi__fund_daily` |
| `exp_vn30_vs_fund_daily` | table | `marts.exp_vn30_vs_fund_daily` |

Inspect results with:

```bash
docker compose -f compose.dev.yml exec warehouse-postgres \
  psql -U warehouse -d warehouse -c "select * from marts.exp_vn30_vs_fund_daily order by trade_date limit 5;"
```

## Loading into the production warehouse

The prod `warehouse-postgres` in `compose.yml` is not published to the host, so
run the loader inside the running `dagster-code` container (it shares the docker
network and mounts `storage/raw`):

```bash
make load-raw-prod            # or: ./make.ps1 load-raw-prod
```

> Never edit a migration that has already been applied. `migrations/001_*`
> records itself in `bronze.schema_migrations`, so a rewritten file will not
> re-run. Add a new `migrations/002_*.sql` for schema changes instead.

## Optional Mongo utility

Mongo is not used by Dagster or dbt. It remains available only to mirror raw
files for compatibility experiments:

```bash
docker compose -f compose.dev.yml up -d mongo
uv run python scripts/load_mongo_raw.py
```
