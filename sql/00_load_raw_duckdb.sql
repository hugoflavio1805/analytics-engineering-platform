-- Bootstrap a local DuckDB warehouse from the enriched CSVs.
-- Run with: duckdb warehouse.duckdb < sql/00_load_raw_duckdb.sql

CREATE SCHEMA IF NOT EXISTS raw;

CREATE OR REPLACE TABLE raw.customers AS
SELECT *, current_timestamp AS _loaded_at
FROM read_csv_auto('data/raw/customers.csv', header=true);

CREATE OR REPLACE TABLE raw.subscriptions AS
SELECT *, current_timestamp AS _loaded_at
FROM read_csv_auto('data/raw/subscriptions.csv', header=true);

CREATE OR REPLACE TABLE raw.feedbacks AS
SELECT *, current_timestamp AS _loaded_at
FROM read_csv_auto('data/raw/feedbacks.csv', header=true);

SELECT 'customers' AS tbl, count(*) FROM raw.customers UNION ALL
SELECT 'subscriptions', count(*) FROM raw.subscriptions UNION ALL
SELECT 'feedbacks', count(*) FROM raw.feedbacks;
