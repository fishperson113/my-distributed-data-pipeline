# Module 4: Multi-source Ingestion and Basic EDA with dbt

## Overview

Module 4 focuses on validating an end-to-end data engineering workflow using two independent data sources: stock market data and ETF/fund certificate data.

Dagster will orchestrate the ingestion workloads and land each dataset in its own dataset-aligned table within the Bronze layer. The stock and fund datasets remain separate in Bronze; they are not joined or unioned during ingestion. dbt will then be used to perform basic data preparation, quality validation, exploratory transformations, and initial EDA.

The final data architecture and dimensional model are not included in this module. They will be considered after the source data structure, quality, and compatibility have been evaluated.

## Objectives

- Ingest stock and ETF data from two independent sources.
- Orchestrate ingestion workloads with Dagster.
- Store each ingested source independently in the Bronze layer.
- Connect dbt to the Bronze datasets.
- Perform basic cleaning, normalization, and data-quality testing.
- Create exploratory models that combine the two data sources.
- Conduct basic EDA to assess data coverage and analytical feasibility.

## Three-week Milestones

### Week 1: Multi-source ingestion

- Complete the Dagster ingestion workflow for both sources.
- Load the collected data into Bronze datasets.
- Validate pipeline execution, reruns, and basic data availability.

### Week 2: dbt integration

- Connect dbt to the Bronze datasets.
- Create basic staging and exploratory transformation models.
- Add essential data-quality tests.
- Integrate dbt execution into the Dagster workflow.

### Week 3: EDA and validation

- Perform basic EDA on the prepared datasets.
- Evaluate missing data, duplicates, date coverage, and join compatibility.
- Validate the end-to-end pipeline on the self-hosted VPS.
- Document findings and limitations for the next project module.

## Deliverables

- Dagster pipeline integrating two independent data sources.
- Bronze datasets containing stock and ETF data.
- dbt project connected to the Bronze datasets.
- Basic dbt transformation models and data-quality tests.
- An exploratory dataset combining the two sources.
- A short EDA report or notebook.
- End-to-end pipeline demonstration on the VPS.

## Technology Stack and Deployment Model

### Shared services hosted on the VPS

- **Dagster Webserver and Daemon:** orchestrate scheduled ingestion and dbt workloads.
- **PostgreSQL:** shared database for Dagster metadata and Bronze datasets.
- **Project runtime:** deployed Python ingestion code and dbt Core environment used by Dagster.
- **Docker Compose:** manages and isolates the shared services.
- **Reverse proxy and HTTPS:** provides secure access to the Dagster interface when required.
- **Persistent storage:** stores source files, pipeline logs, and database volumes.

The VPS acts as the shared integration environment and runs the deployed version of the complete pipeline.

### Tools on each developer's machine

- **Git:** source-code management and collaboration.
- **Python environment:** local development and testing of ingestion logic.
- **Dagster development server:** optional local execution and debugging of assets.
- **dbt Core and database adapter:** development, compilation, and testing of dbt models.
- **SQL client or notebook:** basic EDA and data inspection.
- **Docker:** optional local reproduction of the shared environment.

Each developer can build and test dbt models locally using a dedicated development schema. The dbt project remains version-controlled and shared through Git. After changes are merged and deployed, Dagster runs the same dbt project on the VPS against the shared integration datasets. Therefore, dbt development is performed locally, while automated dbt execution is also available on the VPS.

| Component | Developer machine | Shared VPS |
|---|---:|---:|
| Git repository | Yes | Deployed copy |
| Python ingestion development | Yes | Automated execution |
| Dagster development server | Optional | Yes |
| Dagster daemon and schedules | No | Yes |
| dbt Core | Yes | Yes, invoked by Dagster |
| PostgreSQL | Optional local instance | Yes |
| Bronze datasets | Sample or developer schema | Shared instance |
| EDA notebook/SQL client | Yes | Optional |
| Reverse proxy and HTTPS | No | Yes |

## Bronze Design for Module 4

The Bronze layer is a PostgreSQL schema containing separate, dataset-aligned raw tables. Bronze is the common landing layer, not a joined multi-source dataset. Stock and fund records retain their own payload shapes and grains so that source lineage remains explicit.

The initial database migrations will create one shared batch metadata table and separate raw tables for the two datasets:

- `bronze.ingestion_batch`: stores source, dataset, file or request, partition, checksum, Dagster run, status, and ingestion metadata.
- `bronze.stock`: stores stock records from `vnstock` as JSONB payloads and links each record to an ingestion batch.
- `bronze.fund`: stores fund/ETF records from the SSI source as JSONB payloads and links each record to an ingestion batch.

The two payload columns do not imply a common business schema. Each table has its own source-specific record contract. Shared operational columns such as `batch_id`, `source_record_key`, `event_time`, `trade_date`, `payload_checksum`, and `ingested_at` provide consistent lineage and rerun behavior without combining the datasets.

Original files or API responses will also be retained in persistent storage. Python will perform only the minimum parsing required to identify source records and load them into the corresponding Bronze table. It will not join stock and fund data or define Silver or Gold business schemas.

This approach provides:

- Auditability from database records back to the original source response or file.
- Duplicate detection and source-correction tracking through batch and record payload hashes.
- Clear physical and semantic isolation between stock and fund datasets.
- Independent schema evolution, retention, indexing, and access patterns for each dataset.
- Flexibility to profile each source before selecting a final analytical schema.
- Stable, one-to-one source inputs for dbt staging models.

Any cross-source join is performed downstream in a dbt exploratory model after both Bronze tables have been independently staged, typed, deduplicated, and validated.

Database setup will use version-controlled SQL migration files. An ORM is not required. Migrations may be executed during deployment or through a dedicated Dagster migration job, but they remain separate from scheduled ingestion assets.

## Repository Initialization

The repository will separate source extraction, orchestration, database migrations, dbt models, and exploratory analysis.

```text
project/
├── src/
│   ├── ingestion/
│   │   ├── stock/
│   │   └── fund/
│   └── orchestration/
│       ├── assets/
│       ├── jobs/
│       ├── resources.py
│       └── definitions.py
├── migrations/
│   ├── 001_create_bronze_ingestion_batch.sql
│   ├── 002_create_bronze_stock.sql
│   └── 003_create_bronze_fund.sql
├── dbt_project/
│   ├── models/
│   │   ├── staging/
│   │   └── exploration/
│   ├── tests/
│   └── dbt_project.yml
├── notebooks/
├── tests/
├── storage/
│   └── raw/
├── docker/
├── compose.yml
├── pyproject.toml
├── .env.example
├── .gitignore
└── README.md
```

Repository initialization will follow this order:

1. Initialize Git and the Python project.
2. Add Dagster, PostgreSQL, and dbt dependencies.
3. Create the Docker Compose development and VPS runtime.
4. Add environment configuration templates without committing secrets.
5. Add and execute the first Bronze migration.
6. Implement independent stock and fund ingestion modules.
7. Wrap the ingestion functions with Dagster assets.
8. Initialize the dbt project and connect it to Bronze.
9. Add tests, EDA notebooks, and deployment documentation.

Python ingestion functions will remain independent of Dagster where possible. Dagster assets will act as thin orchestration wrappers, making the extraction and loading logic easier to test locally and reuse.

## Completion Criteria

Module 4 is considered complete when:

- Both data sources are successfully ingested by Dagster.
- The collected data is available in the Bronze layer.
- Pipeline reruns do not create unintended duplicate records.
- dbt can read, transform, and test the Bronze datasets.
- At least one exploratory model combines data from both sources.
- Basic EDA identifies data coverage, quality issues, and join feasibility.
- The complete workflow runs successfully on the self-hosted VPS.

## Out of Scope

- Final data architecture decisions.
- Silver and Gold layer implementation.
- Star schema or dimensional modeling.
- Production dashboard development.
- Real-time or streaming processing.
- Advanced ETF performance analytics.
