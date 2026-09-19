#!/bin/sh
set -eu

port="${DAGSTER_WEBSERVER_PORT:-3000}"

docker compose ps
curl --fail --silent --show-error "http://127.0.0.1:${port}/server_info" >/dev/null

echo "Dagster webserver responded successfully on port ${port}."

