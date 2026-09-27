FROM python:3.11-slim

COPY --from=ghcr.io/astral-sh/uv:0.12.1 /uv /uvx /bin/

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PATH="/opt/dagster/app/.venv/bin:${PATH}" \
    PYTHONPATH="/opt/dagster/app/src"

WORKDIR /opt/dagster/app

COPY pyproject.toml uv.lock README.md config.yml ./
COPY src ./src
COPY migrations ./migrations
COPY scripts ./scripts

RUN uv sync --frozen --no-dev \
    && useradd --create-home --uid 10001 dagster \
    && mkdir -p /opt/dagster/dagster_home /opt/dagster/compute_logs /opt/dagster/app/storage/raw \
    && chown -R dagster:dagster /opt/dagster

# Build the dbt manifest so dagster-dbt can load assets in production, where
# prepare_if_dev() is a no-op. A dummy password satisfies the profile; dbt parse
# does not connect to the database. Re-own the project for runtime dbt writes.
RUN WAREHOUSE_POSTGRES_PASSWORD=build \
    dbt parse --no-partial-parse \
    --project-dir src/data_pipeline/dbt --profiles-dir src/data_pipeline/dbt \
    && chown -R dagster:dagster /opt/dagster/app/src/data_pipeline/dbt

COPY --chown=dagster:dagster docker ./docker

RUN chmod +x /opt/dagster/app/docker/entrypoint.sh

USER dagster

ENTRYPOINT ["/opt/dagster/app/docker/entrypoint.sh"]
CMD ["dagster", "api", "grpc", "--host", "0.0.0.0", "--port", "4000", "--module-name", "data_pipeline.definitions"]
