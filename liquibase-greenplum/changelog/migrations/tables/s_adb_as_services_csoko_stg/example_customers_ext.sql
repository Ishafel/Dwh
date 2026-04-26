--liquibase formatted sql

--changeset codex:example_customers_ext runOnChange:true splitStatements:false
DROP EXTERNAL TABLE IF EXISTS ext.example_customers_raw CASCADE;
DROP EXTERNAL TABLE IF EXISTS ext.example_customers_ext CASCADE;
DROP EXTERNAL TABLE IF EXISTS s_adb_as_services_csoko_stg.example_customers_ext CASCADE;

CREATE EXTERNAL TABLE s_adb_as_services_csoko_stg.example_customers_ext (
    customer_id text,
    full_name text,
    email text,
    created_at text
)
LOCATION ('gpfdist://gpfdist:8081/example_customers/*.csv')
FORMAT 'CSV' (
    HEADER
    DELIMITER ','
    NULL ''
    QUOTE '"'
)
ENCODING 'UTF8'
LOG ERRORS
SEGMENT REJECT LIMIT 100 ROWS;
