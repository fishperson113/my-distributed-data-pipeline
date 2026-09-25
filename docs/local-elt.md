# Postgres dbt development

The production transformation path is Postgres only:

```text
Dagster raw landing -> Postgres bronze -> dbt-postgres -> Postgres staging/marts
```

`compose.yml` runs the production-shaped warehouse Postgres service and applies
the migrations before Dagster starts. For a local dbt session from the host,
`compose.dev.yml` exposes an equivalent warehouse Postgres instance on port
`5433` and applies `migrations/` when its volume is initialized.

## Run dbt locally

```bash
uv sync --group elt
docker compose -f compose.dev.yml up -d warehouse-postgres
uv run python scripts/migrate_warehouse.py
uv run dbt run --project-dir src/data_pipeline/dbt --profiles-dir src/data_pipeline/dbt
uv run dbt test --project-dir src/data_pipeline/dbt --profiles-dir src/data_pipeline/dbt
```

Set `WAREHOUSE_POSTGRES_USER`, `WAREHOUSE_POSTGRES_PASSWORD`,
`WAREHOUSE_POSTGRES_DB`, and `WAREHOUSE_POSTGRES_PORT` in `.env`. The dbt
profile reads those values directly and materializes models into the `staging`
and `marts` schemas.

## Optional Mongo utility

Mongo is not used by Dagster or dbt. It remains available only to mirror raw
files for compatibility experiments:

```bash
docker compose -f compose.dev.yml up -d mongo
uv run python scripts/load_mongo_raw.py
```
