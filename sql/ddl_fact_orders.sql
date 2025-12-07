
-- FACT ORDERS TABLE

CREATE TABLE IF NOT EXISTS fact_orders (
    order_id TEXT PRIMARY KEY,
    order_timestamp_utc TIMESTAMP,
    customer_id TEXT,
    product_id TEXT,
    quantity INTEGER,
    total_amount_usd NUMERIC,
    currency_mismatch_flag BOOLEAN DEFAULT FALSE,
    status TEXT
);
