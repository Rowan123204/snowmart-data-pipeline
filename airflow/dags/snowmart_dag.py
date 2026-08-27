"""
SnowMart Data Pipeline — Apache Airflow DAG
============================================
Orchestrates the full Bronze -> Silver -> Gold pipeline on a daily schedule.
Snowflake is the transformation engine. Airflow is the orchestrator.

Pipeline flow:
    1. Copy staged JSON files into Bronze raw tables
    2. Check Snowflake Streams for new data
    3. Run Silver transformations in parallel (SCD2 + Flattening)
    4. Run Gold aggregations in parallel
    5. Run Data Quality checks (pipeline halts on failure)
"""

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.common.sql.operators.sql import SQLCheckOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'snowmart_data_engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Base path where Airflow resolves relative SQL file paths
SQL_PATH = '/opt/airflow/dags/repo/sql'

with DAG(
    dag_id='snowmart_daily_pipeline',
    default_args=default_args,
    description='SnowMart end-to-end pipeline: Bronze ingestion to Gold aggregations',
    schedule_interval='0 6 * * *',
    start_date=datetime(2026, 8, 20),
    catchup=False,
    tags=['snowmart', 'production'],
    template_searchpath=[SQL_PATH],
) as dag:

    # ------------------------------------------------------------------
    # 1. Bronze Ingestion
    # Load staged JSON files from Snowflake internal stage into raw tables.
    # ------------------------------------------------------------------
    ingest_bronze = SnowflakeOperator(
        task_id='ingest_bronze',
        snowflake_conn_id='snowflake_prod_conn',
        sql='bronze/copy_into_bronze.sql',
    )

    # ------------------------------------------------------------------
    # 2. Check Streams
    # Confirm that new data exists in the CDC streams before proceeding.
    # If both streams are empty, the pipeline has nothing to process.
    # ------------------------------------------------------------------
    check_streams = SnowflakeOperator(
        task_id='check_streams',
        snowflake_conn_id='snowflake_prod_conn',
        sql="""
            SELECT
                SYSTEM$STREAM_HAS_DATA('snowmart_db.bronze.customers_stream') AS customers_ready,
                SYSTEM$STREAM_HAS_DATA('snowmart_db.bronze.orders_stream')    AS orders_ready;
        """,
    )

    # ------------------------------------------------------------------
    # 3. Silver Transformations (run in parallel)
    # ------------------------------------------------------------------
    silver_customers = SnowflakeOperator(
        task_id='silver_customers_scd2',
        snowflake_conn_id='snowflake_prod_conn',
        sql='silver/customers_scd2.sql',
    )

    silver_orders = SnowflakeOperator(
        task_id='silver_orders_flatten',
        snowflake_conn_id='snowflake_prod_conn',
        sql='silver/orders_flatten.sql',
    )

    # ------------------------------------------------------------------
    # 4. Gold Aggregations (run in parallel after both Silver tasks finish)
    # ------------------------------------------------------------------
    gold_daily_sales = SnowflakeOperator(
        task_id='gold_daily_sales',
        snowflake_conn_id='snowflake_prod_conn',
        sql='gold/daily_sales.sql',
    )

    gold_customer_summary = SnowflakeOperator(
        task_id='gold_customer_summary',
        snowflake_conn_id='snowflake_prod_conn',
        sql='gold/customer_summary.sql',
    )

    # ------------------------------------------------------------------
    # 5. Data Quality Checks
    # Each SQLCheckOperator expects its query to return TRUE (1).
    # If the check returns FALSE (0 or null), Airflow marks the task FAILED.
    # ------------------------------------------------------------------
    dq_no_null_customers = SQLCheckOperator(
        task_id='dq_no_null_customer_ids',
        conn_id='snowflake_prod_conn',
        sql="""
            SELECT COUNT(*) = 0
            FROM snowmart_db.silver.dim_customers
            WHERE customer_id IS NULL
        """,
    )

    dq_no_duplicate_active = SQLCheckOperator(
        task_id='dq_no_duplicate_active_records',
        conn_id='snowflake_prod_conn',
        sql="""
            SELECT COUNT(*) = 0
            FROM (
                SELECT customer_id
                FROM snowmart_db.silver.dim_customers
                WHERE is_current = TRUE
                GROUP BY customer_id
                HAVING COUNT(*) > 1
            )
        """,
    )

    dq_no_negative_revenue = SQLCheckOperator(
        task_id='dq_no_negative_revenue',
        conn_id='snowflake_prod_conn',
        sql="""
            SELECT COUNT(*) = 0
            FROM snowmart_db.silver.fact_orders
            WHERE total_item_cost < 0
        """,
    )

    # ------------------------------------------------------------------
    # Task Dependency Graph
    # ------------------------------------------------------------------
    ingest_bronze >> check_streams
    check_streams >> [silver_customers, silver_orders]
    [silver_customers, silver_orders] >> [gold_daily_sales, gold_customer_summary]
    [gold_daily_sales, gold_customer_summary] >> [dq_no_null_customers, dq_no_duplicate_active, dq_no_negative_revenue]
