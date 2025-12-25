-- PostgreSQL Table Definitions for Instacart Dataset
-- Generated based on CSV data files
create schema instacart;

-- Aisles table
CREATE TABLE IF NOT EXISTS instacart.aisles (
    aisle_id INTEGER PRIMARY KEY,
    aisle VARCHAR(255) NOT NULL
);

-- Products table
CREATE TABLE IF NOT EXISTS instacart.products (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    aisle_id INTEGER NOT NULL,
    department_id INTEGER NOT NULL
);

-- Departments table
CREATE TABLE IF NOT EXISTS instacart.departments (
    department_id INTEGER PRIMARY KEY,
    department VARCHAR(255) NOT NULL
);

-- Orders table
CREATE TABLE IF NOT EXISTS instacart.orders (
    order_id INTEGER PRIMARY KEY
    , user_id INTEGER NOT NULL
    , order_date TIMESTAMP NOT NULL
    -- , order_number INTEGER
    -- , order_dow INTEGER
    -- , order_hour_of_day INTEGER
    -- , days_since_prior_order NUMERIC(10,2)
);

-- Order Products table (all order-product relationships)
CREATE TABLE IF NOT EXISTS instacart.order_products (
    order_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    add_to_cart_order INTEGER,
    -- reordered INTEGER
);



