.PHONY: help prod-up prod-down prod-ps prod-logs prod-config dev-up dev-down dev-ps dev-logs dev-config all-up all-down all-ps config ingest-stock ingest-fund ingest-market load-raw load-raw-prod dbt-debug dbt-run dbt-test dbt-debug-prod dbt-run-prod dbt-test-prod

COMPOSE ?= docker compose
PROD_COMPOSE_FILE ?= compose.yml
DEV_COMPOSE_FILE ?= compose.dev.yml
SERVICES ?=
BUILD ?=
DETACH ?= -d
LOG_ARGS ?=
UV ?= uv
INGEST_ARGS ?=
STOCK_ARGS ?= $(INGEST_ARGS)
FUND_ARGS ?= $(INGEST_ARGS)
LOAD_ARGS ?=
DBT_DIR ?= src/data_pipeline/dbt
DBT_ARGS ?=

help:
	@echo "Usage: make <target> [SERVICES=\"service ...\"] [BUILD=--build]"
	@echo ""
	@echo "Production-shaped stack from $(PROD_COMPOSE_FILE):"
	@echo "  prod-up       Start all services, or SERVICES=\"...\" for a partial stack"
	@echo "  prod-down     Stop and remove the production-shaped stack"
	@echo "  prod-ps       Show production-shaped stack status"
	@echo "  prod-logs     Show logs, or SERVICES=\"...\" for selected services"
	@echo "  prod-config   Render the production-shaped compose configuration"
	@echo ""
	@echo "Local ELT compatibility stack from $(DEV_COMPOSE_FILE):"
	@echo "  dev-up        Start all services, or SERVICES=\"...\" for a partial stack"
	@echo "  dev-down      Stop and remove the local ELT stack"
	@echo "  dev-ps        Show local ELT stack status"
	@echo "  dev-logs      Show logs, or SERVICES=\"...\" for selected services"
	@echo "  dev-config    Render the local ELT compose configuration"
	@echo ""
	@echo "Combined helpers:"
	@echo "  all-up        Start both complete stacks"
	@echo "  all-down      Stop both stacks"
	@echo "  all-ps        Show both stack statuses"
	@echo "  config        Render both compose configurations"
	@echo ""
	@echo "Manual ingestion through uv:"
	@echo "  ingest-stock  Fetch stock data and write a raw JSON dump"
	@echo "  ingest-fund   Fetch fund data and write a raw JSON dump"
	@echo "  ingest-market Run stock ingestion, then fund ingestion"
	@echo ""
	@echo "ELT into the warehouse (load raw -> Bronze, then dbt transforms):"
	@echo "  load-raw      Load raw JSON into the LOCAL dev Bronze (localhost:5433)"
	@echo "  load-raw-prod Load raw JSON into the PROD Bronze (inside the dagster-code container)"
	@echo "  dbt-debug     Check the dbt connection to the LOCAL dev warehouse (5433)"
	@echo "  dbt-run       Build dbt models against LOCAL dev (staging views + marts tables)"
	@echo "  dbt-test      Run dbt tests against LOCAL dev"
	@echo "  dbt-debug-prod  Check dbt connection inside the prod stack (target=prod)"
	@echo "  dbt-run-prod    Build dbt models against PROD (inside the dagster-code container)"
	@echo "  dbt-test-prod   Run dbt tests against PROD (inside the dagster-code container)"
	@echo ""
	@echo "Examples:"
	@echo "  make prod-up"
	@echo "  make prod-up SERVICES=\"postgres warehouse-postgres\""
	@echo "  make all-up BUILD=--build"
	@echo "  make ingest-stock STOCK_ARGS=\"--symbol FPT --start 2026-09-01 --end 2026-09-19\""
	@echo "  make ingest-fund FUND_ARGS=\"--symbol E1VFVN30 --start 2026-09-01 --end 2026-09-19\""
	@echo "  make ingest-market STOCK_ARGS=\"--symbol FPT --start 2026-09-01 --end 2026-09-19\" FUND_ARGS=\"--symbol E1VFVN30 --start 2026-09-01 --end 2026-09-19\""
	@echo "  make load-raw LOAD_ARGS=\"--path storage/raw/vnstock/stock_daily_2026-09-01_2026-09-19.json\""
	@echo "  make load-raw-prod LOAD_ARGS=\"--path storage/raw/vnstock/stock_daily_2026-09-01_2026-09-19.json\""
	@echo "  make dbt-run DBT_ARGS=\"--select staging\""

prod-up:
	$(COMPOSE) -f $(PROD_COMPOSE_FILE) up $(BUILD) $(DETACH) $(SERVICES)

prod-down:
	$(COMPOSE) -f $(PROD_COMPOSE_FILE) down

prod-ps:
	$(COMPOSE) -f $(PROD_COMPOSE_FILE) ps $(SERVICES)

prod-logs:
	$(COMPOSE) -f $(PROD_COMPOSE_FILE) logs $(LOG_ARGS) $(SERVICES)

prod-config:
	$(COMPOSE) -f $(PROD_COMPOSE_FILE) config

dev-up:
	$(COMPOSE) -f $(DEV_COMPOSE_FILE) up $(BUILD) $(DETACH) $(SERVICES)

dev-down:
	$(COMPOSE) -f $(DEV_COMPOSE_FILE) down

dev-ps:
	$(COMPOSE) -f $(DEV_COMPOSE_FILE) ps $(SERVICES)

dev-logs:
	$(COMPOSE) -f $(DEV_COMPOSE_FILE) logs $(LOG_ARGS) $(SERVICES)

dev-config:
	$(COMPOSE) -f $(DEV_COMPOSE_FILE) config

all-up:
	$(COMPOSE) -f $(PROD_COMPOSE_FILE) up $(BUILD) $(DETACH)
	$(COMPOSE) -f $(DEV_COMPOSE_FILE) up $(BUILD) $(DETACH)

all-down:
	$(COMPOSE) -f $(DEV_COMPOSE_FILE) down
	$(COMPOSE) -f $(PROD_COMPOSE_FILE) down

all-ps:
	$(COMPOSE) -f $(PROD_COMPOSE_FILE) ps
	$(COMPOSE) -f $(DEV_COMPOSE_FILE) ps

config: prod-config dev-config

ingest-stock:
	$(UV) run python scripts/test_stock_source.py $(STOCK_ARGS)

ingest-fund:
	$(UV) run python scripts/test_fund_source.py $(FUND_ARGS)

ingest-market:
	$(UV) run python scripts/test_stock_source.py $(STOCK_ARGS)
	$(UV) run python scripts/test_fund_source.py $(FUND_ARGS)

load-raw:
	$(UV) run python scripts/load_postgres_raw.py $(LOAD_ARGS)

load-raw-prod:
	$(COMPOSE) -f $(PROD_COMPOSE_FILE) exec dagster-code python scripts/load_postgres_raw.py $(LOAD_ARGS)

dbt-debug:
	$(UV) run dbt debug --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR) $(DBT_ARGS)

dbt-run:
	$(UV) run dbt run --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR) $(DBT_ARGS)

dbt-test:
	$(UV) run dbt test --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR) $(DBT_ARGS)

dbt-debug-prod:
	$(COMPOSE) -f $(PROD_COMPOSE_FILE) exec dagster-code dbt debug --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR) --target prod $(DBT_ARGS)

dbt-run-prod:
	$(COMPOSE) -f $(PROD_COMPOSE_FILE) exec dagster-code dbt run --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR) --target prod $(DBT_ARGS)

dbt-test-prod:
	$(COMPOSE) -f $(PROD_COMPOSE_FILE) exec dagster-code dbt test --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR) --target prod $(DBT_ARGS)
