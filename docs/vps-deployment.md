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

The smoke test waits for up to 60 seconds for the webserver to become ready.
If Docker requires elevated access for the current user, run both scripts with
`sudo sh` or configure membership in the local `docker` group.

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

## Raw landing files

Dagster bind-mounts the repository's `storage/raw/` directory to
`/opt/dagster/app/storage/raw` in its containers. The deploy script creates the
directory and, when run with `sudo`, assigns it to the container's non-root UID
`10001`. Inspect raw payloads directly from the repository:

```bash
find storage/raw -type f
```

If this repository previously used the `dagster-raw-data` named volume, migrate
its existing files once before removing that volume:

```bash
sudo mkdir -p storage/raw
sudo docker run --rm \
  -v my-distributed-data-pipeline_dagster-raw-data:/from:ro \
  -v "$PWD/storage/raw:/to" \
  alpine sh -c 'cp -a /from/. /to/'
sudo chown -R 10001:10001 storage/raw
```

After verifying the copied files, the old named volume is no longer used by
Compose. Do not remove it until the migration has been verified.
