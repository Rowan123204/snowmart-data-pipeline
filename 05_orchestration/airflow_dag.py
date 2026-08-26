"""
Apache Airflow DAG for SnowMart End-to-End Data Pipeline.
This DAG orchestrates the execution of SQL scripts to move data from Bronze -> Silver -> Gold.
"""

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Default settings applied to all tasks
default_args = {
    'owner': 'snowmart_data_engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
}

# Define the DAG
with DAG(
    dag_id='snowmart_daily_etl_pipeline',
    default_args=default_args,
    description='Executes the Bronze to Gold SnowMart Pipeline',
    schedule_interval='0 6 * * *',  # Runs daily at 6:00 AM UTC
    start_date=datetime(2026, 8, 20),
    catchup=False,
    tags=['snowmart', 'production', 'daily'],
    # Base path where Airflow looks for SQL files
    template_searchpath=['/opt/airflow/dags'] 
) as dag:

    # ---------------------------------------------------------
    # TASK 1: Check for new data in Streams
    # ---------------------------------------------------------
    check_streams = SnowflakeOperator(
        task_id='check_streams_for_new_data',
        snowflake_conn_id='snowflake_prod_conn',
        sql="""
            SELECT
                SYSTEM$STREAM_HAS_DATA('snowmart_db.bronze.customers_stream') AS has_customers,
                SYSTEM$STREAM_HAS_DATA('snowmart_db.bronze.orders_stream')    AS has_orders;
        """
    )

    # ---------------------------------------------------------
    # TASK 2: Silver Transformations (Parallel Execution)
    # Reads the SQL files directly from the repository
    # ---------------------------------------------------------
    silver_customers = SnowflakeOperator(
        task_id='merge_silver_customers_scd2',
        snowflake_conn_id='snowflake_prod_conn',
        sql='03_silver/01_customers_scd2.sql'
    )

    silver_orders = SnowflakeOperator(
        task_id='merge_silver_orders_flatten',
        snowflake_conn_id='snowflake_prod_conn',
        sql='03_silver/02_orders_flatten.sql'
    )

    # ---------------------------------------------------------
    # TASK 3: Gold Aggregations (Parallel Execution)
    # ---------------------------------------------------------
    gold_daily_sales = SnowflakeOperator(
        task_id='merge_gold_daily_sales',
        snowflake_conn_id='snowflake_prod_conn',
        sql='04_gold/01_daily_sales.sql'
    )

    gold_customer_summary = SnowflakeOperator(
        task_id='merge_gold_customer_summary',
        snowflake_conn_id='snowflake_prod_conn',
        sql='04_gold/02_customer_summary.sql'
    )

    # ---------------------------------------------------------
    # TASK 4: Data Quality Checks
    # ---------------------------------------------------------
    data_quality_checks = SnowflakeOperator(
        task_id='run_data_quality_checks',
        snowflake_conn_id='snowflake_prod_conn',
        sql='03_silver/03_data_quality_checks.sql'
    )

    # ---------------------------------------------------------
    # TASK DEPENDENCIES (The Orchestration Graph)
    # ---------------------------------------------------------
    # 1. Check streams first
    # 2. If successful, run both Silver tasks concurrently
    # 3. After BOTH Silver tasks finish, run both Gold tasks concurrently
    # 4. After BOTH Gold tasks finish, run DQ checks

    check_streams >> [silver_customers, silver_orders]
    [silver_customers, silver_orders] >> gold_daily_sales
    [silver_customers, silver_orders] >> gold_customer_summary
    [gold_daily_sales, gold_customer_summary] >> data_quality_checks
