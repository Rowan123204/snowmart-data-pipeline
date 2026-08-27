USE ROLE snowmart_data_engineer;
USE WAREHOUSE snowmart_etl_wh;

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

INSERT INTO snowmart_db.silver.dim_customers 
(customer_id, name, email, city, country, start_date, end_date, is_current)
SELECT
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
