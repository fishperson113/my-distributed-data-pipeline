# VPS Deployment

## Prerequisites

- Linux VPS with Git, Docker Engine, and Docker Compose.
- Repository access from the VPS.
- Firewall rules that do not expose PostgreSQL publicly.

## Initial deployment

```bash
git clone <repository-url> my-distributed-data-pipeline
cd my-distributed-data-pipeline
cp .env.example .env
```

Set a strong `POSTGRES_PASSWORD` in `.env`, then run:

```bash
sh ./scripts/deploy.sh
sh ./scripts/smoke-test.sh
```

Open the Dagster UI only from an allowed network or through an SSH tunnel:

```bash
ssh -L 3000:127.0.0.1:3000 <vps-user>@<vps-host>
```

Then browse to `http://localhost:3000` and materialize `deployment_healthcheck`.

## Inspect services

```bash
docker compose ps
sh ./scripts/logs.sh
```

## Deploy an update

```bash
git pull --ff-only
sh ./scripts/deploy.sh
```

Application containers may be recreated safely because Dagster metadata is stored in the PostgreSQL volume.

## Persistence check

After a successful healthcheck materialization:

```bash
docker compose restart dagster-webserver dagster-daemon dagster-code
```

Confirm that the previous run remains visible in the UI.

Do not delete the `dagster-postgres-data` volume during normal deployment.
