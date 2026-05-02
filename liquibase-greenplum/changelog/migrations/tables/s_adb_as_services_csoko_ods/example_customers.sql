--liquibase formatted sql

--changeset codex:example_customers runOnChange:true splitStatements:false
DROP TABLE IF EXISTS s_adb_as_services_csoko_ods.example_customers CASCADE;

CREATE TABLE s_adb_as_services_csoko_ods.example_customers (
    customer_id text,
    full_name text,
    email text,
    created_at text
)
DISTRIBUTED BY (customer_id);

COMMENT ON TABLE s_adb_as_services_csoko_ods.example_customers
IS 'Материализованная ODS-копия источника example_customers.';
