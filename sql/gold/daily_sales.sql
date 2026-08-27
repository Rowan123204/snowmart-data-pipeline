USE ROLE snowmart_data_engineer;
USE WAREHOUSE snowmart_etl_wh;

MERGE INTO snowmart_db.gold.daily_sales AS target
USING (
    SELECT 
        DATE(order_date)             AS sales_date, 
        COUNT(DISTINCT order_id)     AS total_orders,
        SUM(total_item_cost)         AS total_revenue
    FROM snowmart_db.silver.fact_orders
    GROUP BY DATE(order_date)
) AS source
ON target.sales_date = source.sales_date

WHEN MATCHED THEN UPDATE SET
    target.total_orders     = source.total_orders,
    target.total_revenue    = source.total_revenue

WHEN NOT MATCHED THEN INSERT (sales_date, total_orders, total_revenue)
VALUES (source.sales_date, source.total_orders, source.total_revenue);
