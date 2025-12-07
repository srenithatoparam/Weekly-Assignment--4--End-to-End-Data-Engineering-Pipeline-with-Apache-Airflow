-- DIM CUSTOMERS
CREATE TABLE IF NOT EXISTS dim_customers AS
SELECT DISTINCT
    customer_id,
    first_name,
    last_name,
    email,
    signup_date,
    country
FROM stg_customers
WHERE customer_id IS NOT NULL;

-- DIM PRODUCTS
CREATE TABLE IF NOT EXISTS dim_products AS
SELECT DISTINCT
    product_id,
    product_name,
    category,
    unit_price
FROM stg_products
WHERE product_id IS NOT NULL;

-- FACT ORDERS (CLEANED)
CREATE TABLE IF NOT EXISTS fact_orders AS
WITH cleaned AS (
    SELECT
        order_id,
        customer_id,
        product_id,
        quantity,
        total_amount,
        status,

        -- Convert timestamp to UTC
        (order_timestamp::timestamp AT TIME ZONE 'UTC') AS order_timestamp_utc,

        -- Currency mismatch flag
        CASE WHEN currency <> 'USD' THEN 1 ELSE 0 END AS currency_mismatch_flag,

        -- Create a ranking to deduplicate by latest timestamp
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY order_timestamp DESC
        ) AS rn
    FROM stg_orders
    WHERE order_id IS NOT NULL
    AND customer_id IS NOT NULL
    AND product_id IS NOT NULL
    AND quantity > 0
)
SELECT
    order_id,
    customer_id,
    product_id,
    quantity,
    total_amount,
    status,
    order_timestamp_utc,
    currency_mismatch_flag
FROM cleaned
WHERE rn = 1;
