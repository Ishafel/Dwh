databaseChangeLog:
  - changeSet:
      id: 0007-create-example-hive-customers-pxf
      author: codex
      changes:
        - sql:
            splitStatements: false
            sql: |
              CREATE EXTENSION IF NOT EXISTS pxf;

              DROP EXTERNAL TABLE IF EXISTS ext.example_hive_customers_pxf;

              CREATE EXTERNAL TABLE ext.example_hive_customers_pxf (
                  customer_id bigint,
                  full_name text,
                  email text,
                  created_at text
              )
              LOCATION ('pxf://demo.example_hive_customers?PROFILE=Hive&SERVER=hive')
              FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');
