-- Create database for Bronze layer
CREATE DATABASE IF NOT EXISTS bronze;

-- Create database for Silver layer
CREATE DATABASE IF NOT EXISTS silver;

-- Create database for Gold layer
CREATE DATABASE IF NOT EXISTS gold;

-- Register Bronze tables
USE bronze;

CREATE TABLE IF NOT EXISTS products
USING DELTA
LOCATION 'gs://cdc-pipeline-data/prod/bronze/products';

CREATE TABLE IF NOT EXISTS departments
USING DELTA
LOCATION 'gs://cdc-pipeline-data/prod/bronze/departments';

CREATE TABLE IF NOT EXISTS aisles
USING DELTA
LOCATION 'gs://cdc-pipeline-data/prod/bronze/aisles';

CREATE TABLE IF NOT EXISTS orders
USING DELTA
LOCATION 'gs://cdc-pipeline-data/prod/bronze/orders';

CREATE TABLE IF NOT EXISTS order_products
USING DELTA
LOCATION 'gs://cdc-pipeline-data/prod/bronze/order_products';
