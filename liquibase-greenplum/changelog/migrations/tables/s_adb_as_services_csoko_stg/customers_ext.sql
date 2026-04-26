--liquibase formatted sql

--changeset codex:customers_ext runOnChange:true splitStatements:false
DROP EXTERNAL TABLE IF EXISTS ext.customers_ext CASCADE;
DROP EXTERNAL TABLE IF EXISTS ext.example_hive_customers_pxf CASCADE;
DROP EXTERNAL TABLE IF EXISTS s_adb_as_services_csoko_stg.customers_ext CASCADE;

SELECT s_adb_as_services_csoko_stg.f_create_external_table(
    $json$
    {
      "schema_name": "s_adb_as_services_csoko_stg",
      "table_name": "customers_ext",
      "source_table": "example_hive_customers",
      "columns": [
        {
          "name": "customer_id",
          "type": "int8",
          "description": "Идентификатор клиента"
        },
        {
          "name": "full_name",
          "type": "text",
          "description": "Полное имя клиента"
        },
        {
          "name": "email",
          "type": "text",
          "description": "Адрес электронной почты клиента"
        },
        {
          "name": "created_at",
          "type": "text",
          "description": "Дата и время создания записи"
        }
      ]
    }
    $json$::jsonb
);
