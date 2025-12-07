-- STAGING TABLES (TRUNCATE DAILY)

CREATE TABLE IF NOT EXISTS stg_customers (
    customer_id TEXT,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    signup_date TIMESTAMP,
    country TEXT
);

CREATE TABLE IF NOT EXISTS stg_products (
    product_id TEXT,
    product_name TEXT,
    category TEXT,
    unit_price NUMERIC
);

CREATE TABLE IF NOT EXISTS stg_orders (
    order_id TEXT,
    order_timestamp TEXT,
    customer_id TEXT,
    product_id TEXT,
    quantity INTEGER,
    total_amount NUMERIC,
    currency TEXT,
    status TEXT
);
