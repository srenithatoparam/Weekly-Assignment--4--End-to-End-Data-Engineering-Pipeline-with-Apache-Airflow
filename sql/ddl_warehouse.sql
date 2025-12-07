-- WAREHOUSE TABLES (DIM + FACT)

CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id TEXT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    signup_date TIMESTAMP,
    country TEXT
);

CREATE TABLE IF NOT EXISTS dim_products (
    product_id TEXT PRIMARY KEY,
    product_name TEXT,
    category TEXT,
    unit_price NUMERIC
);

CREATE TABLE IF NOT EXISTS fact_orders (
    order_id TEXT PRIMARY KEY,
    order_timestamp TIMESTAMP,
    customer_id TEXT,
    product_id TEXT,
    quantity INTEGER,
    total_amount NUMERIC,
    currency TEXT,
    currency_mismatch_flag INTEGER,
    status TEXT
);
