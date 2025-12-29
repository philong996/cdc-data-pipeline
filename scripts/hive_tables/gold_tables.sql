
-- Create database for gold layer
CREATE DATABASE IF NOT EXISTS gold;

-- Register gold tables
USE gold;

CREATE TABLE IF NOT EXISTS gold.product_performance_daily
USING DELTA
LOCATION 'gs://cdc-pipeline-data/prod/gold/product_performance_daily';


CREATE TABLE IF NOT EXISTS gold.customer_order_summary
USING DELTA
LOCATION 'gs://cdc-pipeline-data/prod/gold/customer_order_summary';

CREATE TABLE IF NOT EXISTS gold.order_hourly_patterns
USING DELTA
LOCATION 'gs://cdc-pipeline-data/prod/gold/order_hourly_patterns';