# Local ELT Development

Local-only dbt/DuckDB/Mongo/warehouse-Postgres stack for building Bronze/Silver/Gold on top of the raw landing files.
This is separate from `compose.yml`, which runs Dagster's own Postgres metadata database.
The two Postgres instances must never share a schema.
This stack is not wired into `compose.yml`, `scripts/deploy.sh`, or the VPS deployment.

## Roles

- MongoDB is Bronze: dataset-aligned raw record collections (`stock_daily`, `fund_daily`), mirrored from `storage/raw` JSON envelopes.
- DuckDB is the transform engine: dbt reads Mongo's mirrored records (loaded as `raw.*` tables) and builds staging/marts models.
- The warehouse Postgres is the serving layer: dbt's DuckDB marts get synced into it for querying.

`storage/raw` JSON files remain the source of truth for raw landing.
Mongo is a queryable mirror, not a replacement.

## Prerequisites

```bash
uv sync --group elt
```

This installs `dbt-duckdb`, `duckdb`, and `pymongo`.
It is a separate dependency group from the production image, so it does not affect `docker/Dockerfile` or the VPS deployment.

## Bring up Mongo and the warehouse Postgres

```bash
cp .env.example .env
docker compose -f compose.dev.yml up -d
```

Set `MONGO_ROOT_PASSWORD`, `MONGO_URI`, and `WAREHOUSE_POSTGRES_PASSWORD` in
`.env` first. The password in `MONGO_URI` must match `MONGO_ROOT_PASSWORD`; the
URI should authenticate against Mongo's `admin` database, for example:

```dotenv
MONGO_ROOT_USER=root
MONGO_ROOT_PASSWORD=change-me
MONGO_URI=mongodb://root:change-me@localhost:27017/?authSource=admin
```

If a Mongo volume already exists, changing `MONGO_ROOT_PASSWORD` does not change
the password stored in that volume. Either use the original password in
`MONGO_URI`, or recreate the local ELT volumes (this deletes local ELT data):

```bash
docker compose -f compose.dev.yml down -v
docker compose -f compose.dev.yml up -d
```

## Run the ELT flow

Run scripts from the repository root, after materializing at least one raw landing partition (see the main `README.md`).

```bash
uv run python scripts/load_mongo_raw.py
uv run python scripts/load_duckdb_raw.py
uv run dbt run --project-dir src/data_pipeline/dbt --profiles-dir src/data_pipeline/dbt
uv run dbt test --project-dir src/data_pipeline/dbt --profiles-dir src/data_pipeline/dbt
uv run python scripts/sync_warehouse_postgres.py
```

By default, `load_mongo_raw.py` scans `RAW_STORAGE_PATH` from `.env`. You can
instead scan specific JSON files or directories, repeating `--path` as needed:

```bash
uv run python scripts/load_mongo_raw.py --path storage/raw/ssi
uv run python scripts/load_mongo_raw.py --path storage/raw/ssi/fund_daily/date=2026-09-18/extraction.json --path storage/raw/vnstock
```

A directory is scanned recursively for `.json` files.

Or run the whole flow in one step:

```bash
uv run python scripts/run_elt.py
```

## dbt project layout

The dbt project lives at `src/data_pipeline/dbt/`.

- `models/staging/` has one model per source (`stg_vnstock__stock_daily`, `stg_ssi__fund_daily`), each reading a `raw.*` DuckDB table and typing its `payload` JSON column.
- `models/marts/` has `exp_vn30_vs_fund_daily`, an exploratory join of stock closes against the fund on `trade_date`.

`profiles.yml` is self-contained inside the project directory and reads `DUCKDB_PATH` from the environment, so no `~/.dbt` setup is required.

## Not yet done

This flow is standalone, matching the existing standalone source probes.
It is not wired into Dagster assets, jobs, or schedules.
Orchestrating it through Dagster is a separate, later task.
