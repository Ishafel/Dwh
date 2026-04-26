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
