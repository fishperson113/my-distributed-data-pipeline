#!/bin/sh
set -eu

if [ ! -f .env ]; then
  echo "Missing .env. Copy .env.example to .env and set a strong password." >&2
  exit 1
fi

docker compose build
docker compose up -d
docker compose ps

echo "Run sh ./scripts/smoke-test.sh after the services become healthy."
