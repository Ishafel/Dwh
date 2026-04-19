# Project Notes for AI Assistants

## Overview

This repository contains a local DWH stack built with Docker Compose:

- Greenplum 6.27.1 as the main PostgreSQL-compatible DWH engine.
- Apache NiFi 2.8.0 for data flows.
- ClickHouse for analytical/event-style storage.
- Liquibase containers for Greenplum and ClickHouse schema migrations.
- `gpfdist` for serving local landing-zone files to Greenplum external tables.

Prefer reading `README.md` first when a task touches operations, ports, credentials,
or manual commands. Keep this file as the compact working guide.

## Repository Map

- `.github/workflows/deploy.yml` - GitHub Actions deployment workflow for `main`.
- `.github/workflows/tests.yml` - fast Makefile checks for PRs and `main`.
- `.env.example` - documented local defaults for ports, credentials, versions, and JVM sizing.
- `docker-compose.yml` - service graph for Greenplum, gpfdist, NiFi, ClickHouse, and Liquibase.
- `greenplum/init-4-segments.sh` - single-node Greenplum initialization with 4 primary segments.
- `liquibase-greenplum/` - Greenplum Liquibase image and changelog.
- `liquibase-greenplum/changelog/root.yaml` - root changelog using `includeAll` over `migrations/`.
- `liquibase-greenplum/changelog/migrations/` - Greenplum migrations.
- `liquibase-clickhouse/` - ClickHouse Liquibase image and changelog.
- `liquibase-clickhouse/changelog/root.yaml` - root changelog using `includeAll` over `migrations/`.
- `liquibase-clickhouse/changelog/migrations/` - ClickHouse migrations.
- `nifi/Dockerfile` - NiFi image with PostgreSQL and ClickHouse JDBC drivers.
- `data/landing/` - local landing-zone files served by `gpfdist`.
- `Makefile` - local fast checks exposed through `make test`.
- `scripts/test.sh` - compatibility wrapper around `make test`.
- `scripts/deploy.sh` - production-like deploy script used by GitHub Actions.

## Current Data Model

Greenplum migrations currently create:

- `dwh` schema.
- `ext` schema for external tables.
- `stg` schema for staging tables.
- `ext.example_customers_raw` reading CSV files from
  `gpfdist://gpfdist:8081/example_customers/*.csv`.
- `stg.example_customers` distributed by `customer_id`.

ClickHouse migrations currently create:

- `example_events` with `MergeTree` and `ORDER BY (event_time, event_id)`.

## Common Commands

Start or rebuild the full local stack:

```bash
docker compose up -d --build
```

Stop the full local stack:

```bash
docker compose down
```

Run Greenplum migrations manually:

```bash
docker compose build liquibase-greenplum
docker compose run --rm liquibase-greenplum
```

Run ClickHouse migrations manually:

```bash
docker compose build liquibase-clickhouse
docker compose run --rm liquibase-clickhouse
```

Open Greenplum `psql` inside the container:

```bash
docker compose exec -u gpadmin gpdb /usr/local/greenplum-db/bin/psql -d gpdb
```

Open ClickHouse client inside the container:

```bash
docker compose exec clickhouse clickhouse-client --user dwh --password dwhpw --database dwh
```

Check service status and logs:

```bash
docker compose ps
docker compose logs -f gpdb
docker compose logs -f clickhouse
docker compose logs -f nifi
```

## Validation

Run the fast repository checks first:

```bash
make test
```

`./scripts/test.sh` is available as a compatibility wrapper around `make test`.

These checks validate Docker Compose syntax, documented environment variables, Liquibase
migration conventions, Greenplum distribution clauses, ClickHouse engine/order clauses,
gpfdist sample data, and executable shell entrypoints.

When the Docker Compose stack is already running, use `make test-stack` for integration
checks against live Greenplum, ClickHouse, and NiFi services. This target is expected to
fail when containers are not running. Stack checks read `.env` and fall back to the same
defaults as `docker-compose.yml`.

For schema or stack changes, also validate with the smallest relevant Docker Compose commands:

- For Greenplum migration changes, build and run `liquibase-greenplum`.
- For ClickHouse migration changes, build and run `liquibase-clickhouse`.
- For Dockerfile or Compose changes, run `docker compose config` and, when practical,
  `docker compose up -d --build`.
- For landing-zone or external-table changes, query the external table through `psql`.

Some Docker builds download JDBC drivers from Maven Central, so they require network
access.

## Migration Rules

- Add new Greenplum migrations as separate YAML files in
  `liquibase-greenplum/changelog/migrations/`.
- Add new ClickHouse migrations as separate YAML files in
  `liquibase-clickhouse/changelog/migrations/`.
- Keep migration filenames ordered with a numeric prefix, for example
  `0004-create-some-table.yaml`.
- Do not rewrite migrations that may already have been applied unless the user explicitly
  asks for a local reset or confirms it is safe.
- Keep `changeSet.id` values stable after a migration is introduced.
- Use `splitStatements: false` for multi-statement SQL blocks or SQL that Liquibase might
  split incorrectly.
- For Greenplum tables, specify `DISTRIBUTED BY (...)` deliberately.
- For ClickHouse tables, specify the engine explicitly, usually with `ORDER BY (...)`.

## Greenplum Notes

- The local Greenplum container exposes `${GREENPLUM_PORT:-5433}` on the host and uses
  port `5432` inside Docker.
- Defaults are database `gpdb`, user `gpadmin`, password `gpadminpw`.
- The cluster is initialized as one Docker node with 4 primary segments.
- If the segment count or initialization shape changes, the `gpdata` Docker volume may
  need to be recreated. Do not remove volumes unless the user explicitly approves.
- `gpfdist` serves `data/landing` on port `${GPFDIST_PORT:-8081}`.
- Inside Docker, Greenplum reads landing files with URLs like
  `gpfdist://gpfdist:8081/<folder>/*.csv`.

## ClickHouse Notes

- HTTP port defaults to `8123`; native port defaults to `9000`.
- Defaults are database `dwh`, user `dwh`, password `dwhpw`.
- ClickHouse Liquibase uses the ClickHouse JDBC driver plus the GoodforGod Liquibase
  ClickHouse extension.

## NiFi Notes

- NiFi UI is available at `https://localhost:8443/nifi`.
- Defaults are user `admin` and password `GreenplumNiFi123`.
- PostgreSQL JDBC driver path inside NiFi:
  `/opt/nifi/jdbc/postgresql.jar`.
- ClickHouse JDBC driver path inside NiFi:
  `/opt/nifi/jdbc/clickhouse.jar`.
- The landing directory is mounted in NiFi as `/data/landing`.

## Deploy Notes

Deployment is handled by `.github/workflows/deploy.yml` on pushes to `main` and manual
workflow dispatch, but only for repository `Ishafel/Dwh` on `refs/heads/main`.

The workflow expects a self-hosted Linux runner with label `dwh-greenplum` and runs:

```bash
./scripts/deploy.sh
```

`scripts/deploy.sh` expects:

- `APP_DIR=/mnt/bulk/dwh_greenplum`
- `EXPECTED_DOCKER_ROOT=/mnt/bulk/docker`

The deploy script refuses to continue if tracked local changes exist in `APP_DIR`.
It uses `git pull --ff-only origin main`, then `docker compose up -d --build`, then
checks service status, row counts, and NiFi health.

## Dangerous Areas

- Do not delete Docker volumes such as `gpdata`, `clickhouse_data`, or NiFi volumes without
  explicit user approval.
- Do not run broad cleanup commands or destructive Git commands without explicit user
  approval.
- Do not commit real secrets. Use `.env.example` only for safe documented defaults.
- Be careful when changing exposed ports or credentials: README, `.env.example`,
  `docker-compose.yml`, NiFi connection notes, and deploy behavior may all need updates.
- Be careful with `scripts/deploy.sh`; it is used by the production-like deployment path.

## Style

- Keep documentation in Russian when extending existing Russian docs unless the user asks
  otherwise.
- Keep this file in English so tool-facing project guidance stays compact and easy to scan.
- Prefer explicit SQL column lists over `SELECT *` in persistent logic.
- Keep generated or sample data small and safe for Git.
- Follow existing YAML changelog style before introducing new abstractions.
