USE ROLE snowmart_data_engineer;
USE WAREHOUSE snowmart_etl_wh;

MERGE INTO snowmart_db.gold.customer_summary AS target
USING (
    SELECT
        c.customer_id,
        c.name                        AS name,
        SUM(o.total_item_cost)        AS total_spent,
        SUM(o.qty)                    AS total_items_bought,
        MIN(o.order_date)             AS first_order_date,
        MAX(o.order_date)             AS last_order_date
    FROM snowmart_db.silver.fact_orders o
    JOIN snowmart_db.silver.dim_customers c
      ON o.customer_id = c.customer_id 
     AND c.is_current = TRUE
    GROUP BY c.customer_id, c.name
) AS source
ON target.customer_id = source.customer_id

WHEN MATCHED THEN UPDATE SET
    target.name                 = source.name,
    target.total_spent          = source.total_spent,
    target.total_items_bought   = source.total_items_bought,
    target.first_order_date     = source.first_order_date,
    target.last_order_date      = source.last_order_date

WHEN NOT MATCHED THEN INSERT (customer_id, name, total_spent, total_items_bought, first_order_date, last_order_date)
VALUES (source.customer_id, source.name, source.total_spent, source.total_items_bought, source.first_order_date, source.last_order_date);
