# ShopVerse Airflow Pipeline – Quick Setup Guide

## 1. Setting Up Variables & Connections

### Airflow Variables
Create the following variables in the Airflow UI → **Admin → Variables**:

| Key                        | Value              |
|----------------------------|--------------------|
| shopverse_data_base_path   | /opt/airflow/data  |
| shopverse_min_order_threshold | 10             |

### Airflow Connections

#### 1. Postgres Connection
Go to **Admin → Connections → + Add**:

- **Conn Id:** `postgres_dwh`  
- **Conn Type:** Postgres  
- **Host:** postgres  
- **Login:** airflow  
- **Password:** airflow  
- **Schema:** dwh_shopverse  
- **Port:** 5432  

#### 2. FileSystem Connection
Create a connection for sensors to detect files:

- **Conn Id:** `fs_default`  
- **Conn Type:** File (path)  
- **Extra:** `{"path": "/opt/airflow/data"}`  

---

## 2. Placing Input Files

Place daily landing files inside the mounted `data/landing/` directory inside your project:
##### data/ 
##### └── landing/ 
##### ├── customers/ customers_YYYYMMDD.csv 
##### ├── products/ products_YYYYMMDD.csv 
##### └── orders/ orders_YYYYMMDD.json


**File naming format:**
- `customers_YYYYMMDD.csv`  
- `products_YYYYMMDD.csv`  
- `orders_YYYYMMDD.json`  

These filenames must match the DAG patterns using `{{ ds_nodash }}`.

---

## 3. Triggering the DAG & Backfilling

### Run for today's date
In Airflow UI → **DAGs → shopverse_daily_pipeline → Trigger DAG**

### Backfill older dates
Example: 2025-01-01 to 2025-01-05

```bash
airflow dags backfill shopverse_daily_pipeline \
    -s 2025-01-01 -e 2025-01-05
```
#### Make sure files are present for each backfill date
## 4. Data Quality Checks (DAG Built-In)

The pipeline includes three mandatory data quality validations:

- **`dim_customers` row count > 0**  
  Ensures that customer dimension is not empty after loading.

- **`fact_orders` count matches valid staging rows**  
  Validates that orders loaded into the warehouse match processed staging rows.

- **No NULL `customer_id` or `product_id` in `fact_orders`**  
  Prevents orphaned fact records that break analytical joins.

---

### Failure Handling
- The DAG fails immediately  
- A failure notification (email/Slack if configured) is triggered  
- Branching logic stops normal flow and raises alerts  
