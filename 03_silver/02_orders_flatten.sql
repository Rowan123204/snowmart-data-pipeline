-- ====================================================================
-- SCRIPT: 02_silver_orders_flatten.sql
-- PURPOSE: Flatten JSON arrays and load into Silver fact_orders
-- ====================================================================

USE ROLE snowmart_data_engineer;
USE WAREHOUSE snowmart_etl_wh;

MERGE INTO snowmart_db.silver.fact_orders AS target
USING (
    -- Extract root fields and flatten the nested 'items' array
    SELECT
        raw_data:order_id::INT           AS order_id,
        raw_data:customer_id::INT        AS customer_id,
        item.value:item_name::VARCHAR    AS item_name,
        item.value:price::DECIMAL(10,2)  AS price,
        item.value:qty::INT              AS qty,
        (item.value:price::DECIMAL(10,2) * item.value:qty::INT) AS total_item_cost,
        raw_data:order_date::TIMESTAMP   AS order_date
    FROM snowmart_db.bronze.orders_stream,
    LATERAL FLATTEN(input => raw_data:items) item
    WHERE METADATA$ACTION = 'INSERT'
) AS source
ON target.order_id = source.order_id AND target.item_name = source.item_name

WHEN NOT MATCHED THEN
    INSERT (order_id, customer_id, item_name, price, qty, total_item_cost, order_date)
    VALUES (source.order_id, source.customer_id, source.item_name,
            source.price, source.qty, source.total_item_cost, source.order_date);
