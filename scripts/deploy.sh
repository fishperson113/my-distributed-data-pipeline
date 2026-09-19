#!/bin/sh
set -eu

if [ ! -f .env ]; then
  echo "Missing .env. Copy .env.example to .env and set a strong password." >&2
  exit 1
fi

mkdir -p storage/raw

# The application image runs as the non-root `dagster` user with UID 10001.
# Ensure the host bind mount is writable when this script is run with sudo.
if [ "$(id -u)" -eq 0 ]; then
  chown 10001:10001 storage/raw
fi

docker compose build
docker compose up -d
docker compose ps

echo "Run sh ./scripts/smoke-test.sh after the services become healthy."
