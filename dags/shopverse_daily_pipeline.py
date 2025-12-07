from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.email import EmailOperator
from datetime import timedelta
import csv
import json


# DAG CONFIGURATION
@dag(
    dag_id="shopverse_daily_pipeline",
    schedule="0 1 * * *",
    start_date=days_ago(2),
    catchup=True,
    default_args={
        "owner": "airflow",
        "email_on_failure": True,
        "email": ["alert@example.com"],
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    description="Complete ShopVerse Pipeline: Sensors + Staging + Warehouse + Branching + Alerts"
)
def shopverse_pipeline():

    # STEP 1 — Read Config Variables
    @task
    def read_config():
        base_path = Variable.get("shopverse_data_base_path")
        min_orders = int(Variable.get("shopverse_min_order_threshold"))
        return {"base_path": base_path, "min_orders": min_orders}

    config = read_config()

    # STEP 4 — File Sensors
    customers_file = FileSensor(
        task_id="wait_for_customers_file",
        fs_conn_id="fs_default",
        filepath="landing/customers/customers_{{ ds_nodash }}.csv",
        poke_interval=30,
        timeout=1800,
    )

    products_file = FileSensor(
        task_id="wait_for_products_file",
        fs_conn_id="fs_default",
        filepath="landing/products/products_{{ ds_nodash }}.csv",
        poke_interval=30,
        timeout=1800,
    )

    orders_file = FileSensor(
        task_id="wait_for_orders_file",
        fs_conn_id="fs_default",
        filepath="landing/orders/orders_{{ ds_nodash }}.json",
        poke_interval=30,
        timeout=1800,
    )

    # STEP 5 — STAGING LAYER
    with TaskGroup("staging_layer") as staging:

        # --- Truncate existing daily data ---
        truncate_customers = PostgresOperator(
            task_id="truncate_stg_customers",
            postgres_conn_id="postgres_dwh",
            sql="TRUNCATE TABLE stg_customers;"
        )

        truncate_products = PostgresOperator(
            task_id="truncate_stg_products",
            postgres_conn_id="postgres_dwh",
            sql="TRUNCATE TABLE stg_products;"
        )

        truncate_orders = PostgresOperator(
            task_id="truncate_stg_orders",
            postgres_conn_id="postgres_dwh",
            sql="TRUNCATE TABLE stg_orders;"
        )

        # --- Load Customers CSV ---
        @task
        def load_customers_task(ds_nodash: str):
            hook = PostgresHook(postgres_conn_id="postgres_dwh")
            path = f"/opt/airflow/data/landing/customers/customers_{ds_nodash}.csv"

            with open(path, "r") as f:
                r = csv.reader(f)
                next(r)
                rows = list(r)

            hook.run("""
                INSERT INTO stg_customers 
                (customer_id, first_name, last_name, email, signup_date, country)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, parameters=rows)

        # --- Load Products CSV ---
        @task
        def load_products_task(ds_nodash: str):
            hook = PostgresHook(postgres_conn_id="postgres_dwh")
            path = f"/opt/airflow/data/landing/products/products_{ds_nodash}.csv"

            with open(path, "r") as f:
                r = csv.reader(f)
                next(r)
                rows = list(r)

            hook.run("""
                INSERT INTO stg_products
                (product_id, product_name, category, unit_price)
                VALUES (%s, %s, %s, %s)
            """, parameters=rows)

        # --- Load Orders JSON ---
        @task
        def load_orders_task(ds_nodash: str):
            hook = PostgresHook(postgres_conn_id="postgres_dwh")
            path = f"/opt/airflow/data/landing/orders/orders_{ds_nodash}.json"

            rows = []
            with open(path, "r") as f:
                data = json.load(f)
                for item in data:
                    rows.append([
                        item["order_id"], item["order_timestamp"],
                        item["customer_id"], item["product_id"],
                        item["quantity"], item["total_amount"],
                        item["currency"], item["status"]
                    ])

            hook.run("""
                INSERT INTO stg_orders
                (order_id, order_timestamp, customer_id, product_id,
                 quantity, total_amount, currency, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, parameters=rows)

        load_customers = load_customers_task("{{ ds_nodash }}")
        load_products = load_products_task("{{ ds_nodash }}")
        load_orders = load_orders_task("{{ ds_nodash }}")

        truncate_customers >> load_customers
        truncate_products >> load_products
        truncate_orders >> load_orders

    # STEP 7 — WAREHOUSE TRANSFORMATION LAYER
    with TaskGroup("warehouse_layer") as warehouse:

        dim_customers = PostgresOperator(
            task_id="build_dim_customers",
            postgres_conn_id="postgres_dwh",
            sql="""
                INSERT INTO dim_customers
                SELECT DISTINCT customer_id, first_name, last_name, email, signup_date, country
                FROM stg_customers
                ON CONFLICT (customer_id) DO UPDATE SET
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    email = EXCLUDED.email,
                    signup_date = EXCLUDED.signup_date,
                    country = EXCLUDED.country;
            """
        )

        dim_products = PostgresOperator(
            task_id="build_dim_products",
            postgres_conn_id="postgres_dwh",
            sql="""
                INSERT INTO dim_products
                SELECT DISTINCT product_id, product_name, category, unit_price
                FROM stg_products
                ON CONFLICT (product_id) DO UPDATE SET
                    product_name = EXCLUDED.product_name,
                    category = EXCLUDED.category,
                    unit_price = EXCLUDED.unit_price;
            """
        )

        fact_orders = PostgresOperator(
            task_id="build_fact_orders",
            postgres_conn_id="postgres_dwh",
            sql="""
                INSERT INTO fact_orders
                SELECT
                    order_id, customer_id, product_id,
                    (order_timestamp)::timestamp AT TIME ZONE 'UTC',
                    quantity, total_amount,
                    CASE WHEN currency <> 'USD' THEN 1 ELSE 0 END
                FROM stg_orders
                WHERE quantity > 0
                ON CONFLICT (order_id) DO UPDATE SET
                    customer_id = EXCLUDED.customer_id,
                    product_id = EXCLUDED.product_id,
                    order_timestamp = EXCLUDED.order_timestamp,
                    quantity = EXCLUDED.quantity,
                    total_amount = EXCLUDED.total_amount,
                    currency_mismatch_flag = EXCLUDED.currency_mismatch_flag;
            """
        )

        dim_customers >> dim_products >> fact_orders

    # STEP 8 — BRANCHING LOGIC
    @task
    def count_orders(ds: str):
        hook = PostgresHook(postgres_conn_id="postgres_dwh")
        sql = f"""
            SELECT COUNT(*) 
            FROM fact_orders 
            WHERE DATE(order_timestamp) = '{ds}';
        """
        return hook.get_first(sql)[0]

    order_count = count_orders("{{ ds }}")

    def choose_path(order_count: int, min_orders: int):
        if order_count < min_orders:
            return "warn_low_volume"
        return "normal_flow"

    branch = BranchPythonOperator(
        task_id="branching",
        python_callable=choose_path,
        op_args=[order_count, config["min_orders"]],
    )

    warn_low_volume = EmptyOperator(task_id="warn_low_volume")
    normal_flow = EmptyOperator(task_id="normal_flow")

    # STEP 9 — EMAIL NOTIFICATION SYSTEM
    notify_low_volume = EmailOperator(
        task_id="notify_low_volume",
        to="alert@example.com",
        subject="⚠ ShopVerse Alert — Low Order Volume Detected",
        html_content="<h3>The number of orders today is below threshold.</h3>",
        trigger_rule="all_done"
    )

    notify_pipeline_success = EmailOperator(
        task_id="notify_success",
        to="alert@example.com",
        subject="✅ ShopVerse Pipeline Success",
        html_content="<h3>Daily pipeline completed successfully.</h3>",
        trigger_rule="all_success"
    )

    # FINAL DEBUG TASK
    @task
    def print_info(config: dict):
        print("Pipeline run complete:", config)

    debug = print_info(config)

    # FINAL DAG FLOW
    config >> [customers_file, products_file, orders_file] >> staging >> warehouse
    warehouse >> order_count >> branch

    branch >> warn_low_volume >> notify_low_volume >> debug
    branch >> normal_flow >> notify_pipeline_success >> debug


shopverse_pipeline()
