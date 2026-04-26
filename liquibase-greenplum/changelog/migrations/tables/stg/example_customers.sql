--liquibase formatted sql

--changeset codex:example_customers runOnChange:true splitStatements:false
CREATE TABLE IF NOT EXISTS stg.example_customers (
    customer_id bigint,
    full_name text,
    email text,
    created_at timestamp
)
DISTRIBUTED BY (customer_id);
