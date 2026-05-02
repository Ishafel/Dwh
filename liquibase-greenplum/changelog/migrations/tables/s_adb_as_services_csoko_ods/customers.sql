--liquibase formatted sql

--changeset codex:customers runOnChange:true splitStatements:false
DROP TABLE IF EXISTS s_adb_as_services_csoko_ods.customers CASCADE;

CREATE TABLE s_adb_as_services_csoko_ods.customers (
    customer_id int8,
    full_name text,
    email text,
    created_at text,
    ctl_loading int8
)
DISTRIBUTED BY (customer_id);

COMMENT ON TABLE s_adb_as_services_csoko_ods.customers
IS 'Материализованная ODS-копия источника customers.';

COMMENT ON COLUMN s_adb_as_services_csoko_ods.customers.ctl_loading
IS 'Технический идентификатор партии загрузки.';
