SHELL := /bin/bash
.DEFAULT_GOAL := test

-include .env

POSTGRES_DB ?= app
POSTGRES_USER ?= app
POSTGRES_PASSWORD ?= apppw
GREENPLUM_DATABASE_NAME ?= gpdb
CLICKHOUSE_DB ?= dwh
CLICKHOUSE_USER ?= dwh
CLICKHOUSE_PASSWORD ?= dwhpw
NIFI_PORT ?= 8443
NIFI_HEALTH_URL ?= https://localhost:$(NIFI_PORT)/nifi/

.PHONY: test test-compose test-env test-migrations test-landing test-pxf test-shell test-stack test-stack-postgres test-stack-hive test-stack-greenplum test-stack-clickhouse test-stack-nifi

test: test-compose test-env test-migrations test-landing test-pxf test-shell

test-compose:
	@echo "==> Validate Docker Compose config"
	docker compose config --quiet

test-env:
	@echo "==> Check docker-compose variables are documented in .env.example"
	@set -euo pipefail; \
	missing=0; \
	for var in $$(grep -oE '\$$\{[A-Z0-9_]+(:-[^}]*)?\}' docker-compose.yml | sed -E 's/^\$$\{//; s/:-.*//' | sort -u); do \
		if ! grep -q "^$${var}=" .env.example; then \
			echo "Missing $${var} in .env.example"; \
			missing=1; \
		fi; \
	done; \
	exit "$${missing}"

test-migrations:
	@echo "==> Check Liquibase migration conventions"
	@set -euo pipefail; \
	for root in liquibase-postgres/changelog/root.yaml liquibase-greenplum/changelog/root.yaml liquibase-clickhouse/changelog/root.yaml; do \
		grep -q 'includeAll:' "$${root}"; \
		grep -q 'path: migrations' "$${root}"; \
		grep -q 'relativeToChangelogFile: true' "$${root}"; \
	done; \
	for dir in liquibase-postgres/changelog/migrations liquibase-greenplum/changelog/migrations liquibase-clickhouse/changelog/migrations; do \
		for file in "$${dir}"/*.yaml; do \
			base="$$(basename "$${file}" .yaml)"; \
			[[ "$${base}" =~ ^[0-9]{4}-[a-z0-9-]+$$ ]] || { echo "Bad migration filename: $${file}"; exit 1; }; \
			id="$$(grep -m1 -E '^[[:space:]]+id:' "$${file}" | sed -E 's/^[[:space:]]+id:[[:space:]]*//')"; \
			[ "$${id}" = "$${base}" ] || { echo "changeSet.id must match filename in $${file}"; exit 1; }; \
		done; \
	done; \
	for file in liquibase-greenplum/changelog/migrations/*.yaml; do \
		if grep -qiE '\bCREATE[[:space:]]+TABLE\b' "$${file}"; then \
			grep -qi 'DISTRIBUTED BY' "$${file}" || { echo "Missing DISTRIBUTED BY in $${file}"; exit 1; }; \
		fi; \
	done; \
	for file in liquibase-clickhouse/changelog/migrations/*.yaml; do \
		if grep -qi 'CREATE TABLE' "$${file}"; then \
			grep -qi 'ENGINE =' "$${file}" || { echo "Missing ENGINE in $${file}"; exit 1; }; \
			grep -qi 'ORDER BY' "$${file}" || { echo "Missing ORDER BY in $${file}"; exit 1; }; \
		fi; \
	done

test-landing:
	@echo "==> Check gpfdist landing samples"
	@set -euo pipefail; \
	locations="$$(grep -RhoE 'gpfdist://gpfdist:8081/[a-zA-Z0-9_-]+/\*\.csv' liquibase-greenplum/changelog/migrations | sed -E 's#gpfdist://gpfdist:8081/([^/]+)/.*#\1#' | sort -u)"; \
	[ -n "$${locations}" ] || { echo "No gpfdist external table locations found"; exit 1; }; \
	for folder in $${locations}; do \
		[ -d "data/landing/$${folder}" ] || { echo "Missing landing directory: data/landing/$${folder}"; exit 1; }; \
		find "data/landing/$${folder}" -maxdepth 1 -name '*.csv' | grep -q . || { echo "Missing CSV sample in data/landing/$${folder}"; exit 1; }; \
	done; \
	header="$$(head -n 1 data/landing/example_customers/sample_customers.csv)"; \
	[ "$${header}" = "customer_id,full_name,email,created_at" ] || { echo "Unexpected example_customers CSV header"; exit 1; }

test-pxf:
	@echo "==> Check PXF sample configuration"
	@test -f greenplum/pxf/servers/hive/hive-site.xml
	@test -f greenplum/pxf/servers/hive/core-site.xml
	@test -f hive/conf/hive-site.xml
	@test -f hive/client-conf/hive-site.xml
	@test -x greenplum/start-gpfdist.sh
	@test -x hive/start-metastore.sh
	@test -x hive/init-example.sh
	@test -x greenplum/create-pxf-example-tables.sh
	@grep -q 'thrift://hive-metastore:9083' greenplum/pxf/servers/hive/hive-site.xml
	@grep -q 'thrift://hive-metastore:9083' hive/client-conf/hive-site.xml
	@grep -q 'GREENPLUM_PXF_ENABLE' docker-compose.yml
	@grep -q 'pxf-examples:' docker-compose.yml
	@grep -q 'db_host="$${GREENPLUM_HOST:-gpdb}"' greenplum/create-pxf-example-tables.sh
	@grep -q 'pxf://demo.example_hive_customers?PROFILE=Hive&SERVER=hive' greenplum/create-pxf-example-tables.sh

test-shell:
	@echo "==> Check shell entrypoints are executable"
	@test -x greenplum/init-4-segments.sh
	@test -x greenplum/start-gpfdist.sh
	@test -x greenplum/create-pxf-example-tables.sh
	@test -x hive/start-metastore.sh
	@test -x hive/init-example.sh
	@test -x scripts/deploy.sh
	@test -x scripts/test.sh

test-stack: test-stack-postgres test-stack-hive test-stack-greenplum test-stack-clickhouse test-stack-nifi

test-stack-postgres:
	@echo "==> Check live PostgreSQL"
	@docker compose exec -T postgres psql -U "$(POSTGRES_USER)" -d "$(POSTGRES_DB)" -Atc "SELECT 1;"
	@docker compose exec -T postgres psql -U "$(POSTGRES_USER)" -d "$(POSTGRES_DB)" -Atc "SELECT CASE WHEN EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'dm') THEN 'dm' ELSE 'missing' END;" | grep -qx "dm"

test-stack-hive:
	@echo "==> Check live Hive Metastore"
	@docker compose exec -T hive-metastore bash -lc '</dev/tcp/localhost/9083'
	@docker compose run --rm --no-deps --entrypoint /opt/hive/bin/hive hive-init --skiphadoopversion --skiphbasecp -e "SELECT count(*) FROM demo.example_hive_customers;" | grep -q "3"

test-stack-greenplum:
	@echo "==> Check live Greenplum"
	@docker compose exec -T -u gpadmin gpdb /usr/local/greenplum-db/bin/psql -d "$(GREENPLUM_DATABASE_NAME)" -Atc "SELECT 1;"
	@docker compose exec -T -u gpadmin gpdb /usr/local/greenplum-db/bin/psql -d "$(GREENPLUM_DATABASE_NAME)" -Atc "SELECT count(*) FROM ext.example_customers_raw;"
	@docker compose exec -T -u gpadmin gpdb /usr/local/greenplum-db/bin/psql -d "$(GREENPLUM_DATABASE_NAME)" -Atc "SELECT count(*) FROM ext.example_hive_customers_pxf;"

test-stack-clickhouse:
	@echo "==> Check live ClickHouse"
	@docker compose exec -T clickhouse clickhouse-client --user "$(CLICKHOUSE_USER)" --password "$(CLICKHOUSE_PASSWORD)" --database "$(CLICKHOUSE_DB)" --query "SELECT 1"
	@docker compose exec -T clickhouse clickhouse-client --user "$(CLICKHOUSE_USER)" --password "$(CLICKHOUSE_PASSWORD)" --database "$(CLICKHOUSE_DB)" --query "DESCRIBE TABLE example_events"

test-stack-nifi:
	@echo "==> Check live NiFi"
	@curl -kfsS "$(NIFI_HEALTH_URL)" >/dev/null
