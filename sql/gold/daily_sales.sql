-- ====================================================================
-- SCRIPT: 03_gold_daily_sales.sql
-- PURPOSE: Aggregate Silver fact data into Gold daily sales summary
-- ====================================================================

USE ROLE snowmart_data_engineer;
USE WAREHOUSE snowmart_etl_wh;

MERGE INTO snowmart_db.gold.daily_sales AS target
USING (
    -- Aggregate metrics at the Grain: Date + Item
    SELECT 
        DATE(order_date)             AS sales_date, 
        item_name,
        COUNT(DISTINCT order_id)     AS total_orders,
        SUM(qty)                     AS total_items_sold, 
        SUM(total_item_cost)         AS total_revenue
    FROM snowmart_db.silver.fact_orders
    GROUP BY DATE(order_date), item_name
) AS source
ON target.sales_date = source.sales_date AND target.item_name = source.item_name

WHEN MATCHED THEN UPDATE SET
    target.total_orders     = source.total_orders,
    target.total_items_sold = source.total_items_sold,
    target.total_revenue    = source.total_revenue,
    target.updated_at       = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (sales_date, item_name, total_orders, total_items_sold, total_revenue)
VALUES (source.sales_date, source.item_name, source.total_orders,
        source.total_items_sold, source.total_revenue);
