from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime
from airflow.configuration import conf

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="run_ddl_tables",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    template_searchpath=['/opt/airflow/sql'],    
) as dag:

    create_staging = PostgresOperator(
        task_id="create_staging_tables",
        postgres_conn_id="postgres_dwh",
        sql="ddl_staging.sql",                    
    )

    create_warehouse = PostgresOperator(
        task_id="create_warehouse_tables",
        postgres_conn_id="postgres_dwh",
        sql="ddl_warehouse.sql",                
    )

    create_staging >> create_warehouse
