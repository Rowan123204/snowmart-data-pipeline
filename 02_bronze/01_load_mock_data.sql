-- ====================================================================
-- SCRIPT: 01_load_mock_data.sql
-- PURPOSE: Commands to upload local JSON files to Snowflake internal stage and load into Bronze tables
-- ====================================================================

USE ROLE snowmart_data_engineer;
USE WAREHOUSE snowmart_etl_wh;
USE DATABASE snowmart_db;
USE SCHEMA bronze;

-- 1. Create an internal stage for JSON files
CREATE STAGE IF NOT EXISTS snowmart_stage
    FILE_FORMAT = (TYPE = JSON);

-- 2. (Execute via SnowSQL CLI to upload files from local machine)
-- PUT file://./mock_data/customers_day1.json @snowmart_stage/day1/ AUTO_COMPRESS=TRUE;
-- PUT file://./mock_data/orders_day1.json @snowmart_stage/day1/ AUTO_COMPRESS=TRUE;

-- 3. Load Day 1 Data into Bronze Tables
COPY INTO customers_raw (raw_data, file_name)
FROM (
    SELECT $1, metadata$filename 
    FROM @snowmart_stage/day1/customers_day1.json.gz
);

COPY INTO orders_raw (raw_data, file_name)
FROM (
    SELECT $1, metadata$filename 
    FROM @snowmart_stage/day1/orders_day1.json.gz
);

-- Note: For Day 2 (CDC Simulation), upload the day2 files and run the corresponding COPY INTO commands.
