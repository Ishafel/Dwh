--liquibase formatted sql

--changeset codex:example_customers_raw splitStatements:false
DROP EXTERNAL TABLE IF EXISTS ext.example_customers_raw;

CREATE EXTERNAL TABLE ext.example_customers_raw (
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
