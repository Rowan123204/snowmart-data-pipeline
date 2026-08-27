-- ====================================================================
-- SCRIPT: 01_silver_customers_scd2.sql
-- PURPOSE: Transform Bronze customer data into Silver dim_customers using Dynamic SCD Type 2
-- ====================================================================

USE ROLE snowmart_data_engineer;
USE WAREHOUSE snowmart_etl_wh;

-- --------------------------------------------------------------------
-- STEP 1: UPDATE (Close) existing active records if their data has changed
-- This dynamically targets ALL customers in the stream whose city or email changed.
-- --------------------------------------------------------------------
UPDATE snowmart_db.silver.dim_customers AS target
SET target.end_date = CURRENT_TIMESTAMP(),
    target.is_current = FALSE
FROM (
    SELECT 
        raw_data:customer_id::INT          AS customer_id,
        raw_data:email::VARCHAR            AS email,
        raw_data:location:city::VARCHAR    AS city
    FROM snowmart_db.bronze.customers_stream
    WHERE METADATA$ACTION = 'INSERT'
) AS stream_data
WHERE target.customer_id = stream_data.customer_id
  AND target.is_current = TRUE
  AND (target.city <> stream_data.city OR target.email <> stream_data.email);

-- --------------------------------------------------------------------
-- STEP 2: INSERT new records (Both brand new customers AND updated versions of old customers)
-- This dynamically inserts everyone coming from the CDC stream.
-- --------------------------------------------------------------------
INSERT INTO snowmart_db.silver.dim_customers 
(customer_key, customer_id, name, email, city, country, start_date, end_date, is_current)
SELECT
    MD5(raw_data:customer_id::VARCHAR || CURRENT_TIMESTAMP()::VARCHAR) AS customer_key,
    raw_data:customer_id::INT          AS customer_id,
    raw_data:name::VARCHAR             AS name,
    raw_data:email::VARCHAR            AS email,
    raw_data:location:city::VARCHAR    AS city,
    raw_data:location:country::VARCHAR AS country,
    CURRENT_TIMESTAMP()                AS start_date,
    NULL                               AS end_date,
    TRUE                               AS is_current
FROM snowmart_db.bronze.customers_stream
WHERE METADATA$ACTION = 'INSERT';
