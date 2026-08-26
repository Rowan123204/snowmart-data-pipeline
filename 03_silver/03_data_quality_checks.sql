-- ====================================================================
-- SCRIPT: 05_data_quality_checks.sql
-- PURPOSE: Verify data integrity across Silver layer (Completeness, Uniqueness, Validity)
-- ====================================================================

USE ROLE snowmart_data_engineer;
USE WAREHOUSE snowmart_etl_wh;

-- Check 1: Completeness - No NULL customer IDs in Dim
SELECT 
    'FAIL: NULL customer_id found' AS dq_error
FROM snowmart_db.silver.dim_customers 
WHERE customer_id IS NULL
HAVING COUNT(*) > 0;

-- Check 2: SCD2 Integrity - No customer has multiple active records
SELECT 
    'FAIL: Multiple active records for customer ' || customer_id AS dq_error
FROM snowmart_db.silver.dim_customers
WHERE is_current = TRUE 
GROUP BY customer_id 
HAVING COUNT(*) > 1;

-- Check 3: Validity - No negative revenue
SELECT 
    'FAIL: Negative revenue found in order ' || order_id AS dq_error
FROM snowmart_db.silver.fact_orders 
WHERE total_item_cost < 0
HAVING COUNT(*) > 0;
