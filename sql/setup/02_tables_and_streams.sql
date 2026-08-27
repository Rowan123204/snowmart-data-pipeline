-- ====================================================================
-- SCRIPT: 02_tables_and_streams.sql
-- PURPOSE: Create Database, Schemas, Tables, and Streams
-- ====================================================================

-- 1. Create Database (Must be done by SYSADMIN to grant ownership properly)
USE ROLE SYSADMIN;
CREATE DATABASE IF NOT EXISTS snowmart_db;

-- Hand over total ownership of the database to our custom role
GRANT OWNERSHIP ON DATABASE snowmart_db TO ROLE snowmart_data_engineer COPY CURRENT GRANTS;

-- Now switch to our custom role for everything else
USE ROLE snowmart_data_engineer;
USE WAREHOUSE snowmart_etl_wh;

-- 2. Create Schemas (Bronze, Silver, Gold)
CREATE SCHEMA IF NOT EXISTS snowmart_db.bronze;
CREATE SCHEMA IF NOT EXISTS snowmart_db.silver;
CREATE SCHEMA IF NOT EXISTS snowmart_db.gold;

-- 3. Create Bronze Tables (Raw JSON)
CREATE OR REPLACE TABLE snowmart_db.bronze.customers_raw (
    raw_data VARIANT,
    file_name VARCHAR,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE snowmart_db.bronze.orders_raw (
    raw_data VARIANT,
    file_name VARCHAR,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- 4. Create Streams on Bronze Tables (For CDC tracking)
CREATE STREAM IF NOT EXISTS snowmart_db.bronze.customers_stream 
    ON TABLE snowmart_db.bronze.customers_raw 
    APPEND_ONLY = TRUE;

CREATE STREAM IF NOT EXISTS snowmart_db.bronze.orders_stream 
    ON TABLE snowmart_db.bronze.orders_raw 
    APPEND_ONLY = TRUE;

-- 5. Create Silver Tables (Cleaned & Parsed)
CREATE OR REPLACE TABLE snowmart_db.silver.dim_customers (
    customer_id INT,
    name VARCHAR,
    email VARCHAR,
    city VARCHAR,
    country VARCHAR,
    is_current BOOLEAN DEFAULT TRUE,
    start_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    end_date TIMESTAMP DEFAULT NULL
);

CREATE OR REPLACE TABLE snowmart_db.silver.fact_orders (
    order_id INT,
    customer_id INT,
    item_name VARCHAR,
    price FLOAT,
    qty INT,
    total_item_cost FLOAT,
    order_date TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- 6. Create Gold Tables (Aggregations)
CREATE OR REPLACE TABLE snowmart_db.gold.daily_sales (
    sales_date DATE,
    total_orders INT,
    total_revenue FLOAT
);

CREATE OR REPLACE TABLE snowmart_db.gold.customer_summary (
    customer_id INT,
    name VARCHAR,
    total_spent FLOAT,
    total_items_bought INT,
    first_order_date TIMESTAMP,
    last_order_date TIMESTAMP
);
