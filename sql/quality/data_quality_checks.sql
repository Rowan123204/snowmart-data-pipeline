-- ====================================================================
-- SCRIPT: data_quality_checks.sql
-- PURPOSE: Automated data quality validation for the Silver layer.
-- This script is executed by Airflow after Silver transformations.
-- The script uses COUNT(*) assertions. If any check returns rows (count > 0),
-- the Airflow SQLCheckOperator will mark the task as FAILED automatically.
-- ====================================================================

USE ROLE snowmart_data_engineer;
USE WAREHOUSE snowmart_etl_wh;

-- Check 1: Completeness — No NULL customer IDs should exist in the dimension table
SELECT COUNT(*) = 0
FROM snowmart_db.silver.dim_customers
WHERE customer_id IS NULL;

-- Check 2: SCD2 Integrity — Each customer must have at most one active record
SELECT COUNT(*) = 0
FROM (
    SELECT customer_id
    FROM snowmart_db.silver.dim_customers
    WHERE is_current = TRUE
    GROUP BY customer_id
    HAVING COUNT(*) > 1
);

-- Check 3: Validity — No negative revenue in the fact table
SELECT COUNT(*) = 0
FROM snowmart_db.silver.fact_orders
WHERE total_item_cost < 0;
