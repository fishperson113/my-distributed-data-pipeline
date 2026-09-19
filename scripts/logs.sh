#!/bin/sh
set -eu

docker compose logs --follow postgres dagster-code dagster-webserver dagster-daemon

