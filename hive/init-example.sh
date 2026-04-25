#!/usr/bin/env bash
set -euo pipefail

export HIVE_CONF_DIR=/opt/hive/client-conf
export HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS:-} -Xmx512m"

warehouse_dir=/opt/hive/data/warehouse/example_hive_customers
mkdir -p "${warehouse_dir}"

cat > "${warehouse_dir}/data.csv" <<'CSV'
1,Alice Ivanova,alice@example.com,2026-01-10 09:30:00
2,Boris Petrov,boris@example.com,2026-01-11 14:15:00
3,Clara Smirnova,clara@example.com,2026-01-12 18:45:00
CSV

chown -R hive:hive /opt/hive/data/warehouse

/opt/hive/bin/hive --skiphadoopversion --skiphbasecp -e "
  CREATE DATABASE IF NOT EXISTS demo;
  DROP TABLE IF EXISTS demo.example_hive_customers;
  CREATE EXTERNAL TABLE demo.example_hive_customers (
    customer_id BIGINT,
    full_name STRING,
    email STRING,
    created_at STRING
  )
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  STORED AS TEXTFILE
  LOCATION 'file:///opt/hive/data/warehouse/example_hive_customers';
"
