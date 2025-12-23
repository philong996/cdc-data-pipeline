-- PostgreSQL Table Definitions for Instacart Dataset
-- Generated based on CSV data files
create schema instacart;

-- Departments table
CREATE TABLE IF NOT EXISTS instacart.departments (
    department_id INTEGER PRIMARY KEY,
    department VARCHAR(255) NOT NULL
);

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

-- -- Create indexes for better query performance
-- CREATE INDEX IF NOT EXISTS idx_products_aisle ON products(aisle_id);
-- CREATE INDEX IF NOT EXISTS idx_products_department ON products(department_id);
-- CREATE INDEX IF NOT EXISTS idx_orders_user ON orders(user_id);
-- CREATE INDEX IF NOT EXISTS idx_orders_eval_set ON orders(eval_set);
-- CREATE INDEX IF NOT EXISTS idx_order_products_product ON order_products(product_id);

-- Comments for documentation
COMMENT ON TABLE departments IS 'Product departments/categories';
COMMENT ON TABLE aisles IS 'Store aisles where products are located';
COMMENT ON TABLE products IS 'Product catalog with department and aisle assignments';
COMMENT ON TABLE orders IS 'Customer orders with timing and sequence information';
COMMENT ON TABLE order_products IS 'Mapping of products to orders with cart position and reorder info';

COMMENT ON COLUMN orders.order_dow IS 'Day of week (0-6)';
COMMENT ON COLUMN orders.order_hour_of_day IS 'Hour of day (0-23)';
COMMENT ON COLUMN orders.days_since_prior_order IS 'Days since previous order';
