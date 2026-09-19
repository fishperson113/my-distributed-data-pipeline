#!/bin/sh
set -eu

port="${DAGSTER_WEBSERVER_PORT:-3000}"

docker compose ps

attempt=1
max_attempts=30
while ! curl --fail --silent --show-error "http://127.0.0.1:${port}/server_info" >/dev/null 2>&1; do
  if [ "$attempt" -ge "$max_attempts" ]; then
    echo "Dagster webserver did not become ready on port ${port}." >&2
    echo "Inspect logs with: sudo docker compose logs --tail=200 dagster-webserver dagster-daemon" >&2
    exit 1
  fi

  echo "Waiting for Dagster webserver (${attempt}/${max_attempts})..."
  attempt=$((attempt + 1))
  sleep 2
done

echo "Dagster webserver responded successfully on port ${port}."
