--liquibase formatted sql

--changeset codex:v_src_example_customers runOnChange:true runAlways:true splitStatements:false
CREATE OR REPLACE VIEW s_adb_as_services_csoko_ods.v_src_example_customers AS
SELECT
    customer_id,
    full_name,
    email,
    created_at
FROM s_adb_as_services_csoko_stg.example_customers_ext;

COMMENT ON VIEW s_adb_as_services_csoko_ods.v_src_example_customers
IS 'Источник загрузки из STG external example_customers_ext в ODS example_customers.';
