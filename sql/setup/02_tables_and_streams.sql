-- ====================================================================
-- SCRIPT: 02_tables_and_streams.sql
-- PURPOSE: Create Medallion Architecture schemas, raw tables, and CDC streams
-- ====================================================================

USE ROLE snowmart_data_engineer;
USE WAREHOUSE snowmart_etl_wh;

-- 1. Create Database and Schemas
CREATE DATABASE IF NOT EXISTS snowmart_db;
CREATE SCHEMA IF NOT EXISTS snowmart_db.bronze;
CREATE SCHEMA IF NOT EXISTS snowmart_db.silver;
CREATE SCHEMA IF NOT EXISTS snowmart_db.gold;

-- 2. Create Bronze Layer (Raw Variant Tables)
CREATE OR REPLACE TABLE snowmart_db.bronze.customers_raw (
    raw_data VARIANT,
    file_name VARCHAR,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE snowmart_db.bronze.orders_raw (
    raw_data VARIANT,
    file_name VARCHAR,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- 3. Create Change Data Capture (CDC) Streams on Bronze Tables
CREATE STREAM IF NOT EXISTS snowmart_db.bronze.customers_stream 
ON TABLE snowmart_db.bronze.customers_raw 
APPEND_ONLY = TRUE;

CREATE STREAM IF NOT EXISTS snowmart_db.bronze.orders_stream 
ON TABLE snowmart_db.bronze.orders_raw 
APPEND_ONLY = TRUE;

-- 4. Create Silver Layer (Structured, Cleaned, SCD2)
CREATE OR REPLACE TABLE snowmart_db.silver.dim_customers (
    customer_key VARCHAR, -- MD5 Surrogate Key
    customer_id INT,
    name VARCHAR,
    email VARCHAR,
    city VARCHAR,
    country VARCHAR,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    is_current BOOLEAN
);

CREATE OR REPLACE TABLE snowmart_db.silver.fact_orders (
    order_id INT,
    customer_id INT,
    item_name VARCHAR,
    price DECIMAL(10,2),
    qty INT,
    total_item_cost DECIMAL(10,2),
    order_date TIMESTAMP
);

-- 5. Create Gold Layer (Business Aggregations)
CREATE OR REPLACE TABLE snowmart_db.gold.daily_sales (
    sales_date DATE,
    item_name VARCHAR,
    total_orders INT,
    total_items_sold INT,
    total_revenue DECIMAL(12,2),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE snowmart_db.gold.customer_sales_summary (
    customer_id INT,
    customer_name VARCHAR,
    city VARCHAR,
    total_orders_placed INT,
    total_spent DECIMAL(12,2),
    last_order_date TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
