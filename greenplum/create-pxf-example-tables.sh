#!/usr/bin/env bash
set -euo pipefail

source /usr/local/greenplum-db/greenplum_path.sh

db_name="${GREENPLUM_DATABASE_NAME:-gpdb}"
db_host="${GREENPLUM_HOST:-gpdb}"

psql -v ON_ERROR_STOP=1 -h "${db_host}" -U gpadmin -d "${db_name}" <<'SQL'
CREATE EXTENSION IF NOT EXISTS pxf;

DROP EXTERNAL TABLE IF EXISTS ext.example_customers_pxf;

CREATE EXTERNAL TABLE ext.example_customers_pxf (
    customer_id bigint,
    full_name text,
    email text,
    created_at timestamp
)
LOCATION ('pxf://dm.example_customers?PROFILE=Jdbc&SERVER=postgres')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

DROP EXTERNAL TABLE IF EXISTS ext.example_hive_customers_pxf;

CREATE EXTERNAL TABLE ext.example_hive_customers_pxf (
    customer_id bigint,
    full_name text,
    email text,
    created_at text
)
LOCATION ('pxf://demo.example_hive_customers?PROFILE=Hive&SERVER=hive')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');
SQL
