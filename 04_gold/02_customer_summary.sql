-- ====================================================================
-- SCRIPT: 04_gold_customer_summary.sql
-- PURPOSE: Aggregate customer lifetime value into Gold layer
-- ====================================================================

USE ROLE snowmart_data_engineer;
USE WAREHOUSE snowmart_etl_wh;

MERGE INTO snowmart_db.gold.customer_sales_summary AS target
USING (
    -- Join Fact and Dim tables, ensuring we only use the active customer record
    SELECT
        c.customer_id,
        c.name                        AS customer_name,
        c.city                        AS city,
        COUNT(DISTINCT o.order_id)    AS total_orders_placed,
        SUM(o.total_item_cost)        AS total_spent,
        MAX(o.order_date)             AS last_order_date
    FROM snowmart_db.silver.fact_orders o
    JOIN snowmart_db.silver.dim_customers c
      ON o.customer_id = c.customer_id 
     AND c.is_current = TRUE
    GROUP BY c.customer_id, c.name, c.city
) AS source
ON target.customer_id = source.customer_id

WHEN MATCHED THEN UPDATE SET
    target.customer_name       = source.customer_name,
    target.city                = source.city,
    target.total_orders_placed = source.total_orders_placed,
    target.total_spent         = source.total_spent,
    target.last_order_date     = source.last_order_date,
    target.updated_at          = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (customer_id, customer_name, city, total_orders_placed, total_spent, last_order_date)
VALUES (source.customer_id, source.customer_name, source.city, source.total_orders_placed, source.total_spent, source.last_order_date);
