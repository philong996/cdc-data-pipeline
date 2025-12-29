-- Create database for Silver layer
CREATE DATABASE IF NOT EXISTS silver;

-- Register Silver tables
USE silver;

CREATE TABLE IF NOT EXISTS silver.products
USING DELTA
LOCATION 'gs://cdc-pipeline-data/prod/silver/products';

CREATE TABLE IF NOT EXISTS silver.departments
USING DELTA
LOCATION 'gs://cdc-pipeline-data/prod/silver/departments';

CREATE TABLE IF NOT EXISTS silver.aisles
USING DELTA
LOCATION 'gs://cdc-pipeline-data/prod/silver/aisles';

CREATE TABLE IF NOT EXISTS silver.orders
USING DELTA
LOCATION 'gs://cdc-pipeline-data/prod/silver/orders';

CREATE TABLE IF NOT EXISTS silver.order_products
USING DELTA
LOCATION 'gs://cdc-pipeline-data/prod/silver/order_products';
