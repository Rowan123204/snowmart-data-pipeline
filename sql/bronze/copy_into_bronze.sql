-- ====================================================================
-- SCRIPT: copy_into_bronze.sql
-- PURPOSE: Load staged JSON files into Bronze raw tables
-- This script is executed by Airflow on each pipeline run.
-- ====================================================================

USE ROLE snowmart_data_engineer;
USE WAREHOUSE snowmart_etl_wh;
USE DATABASE snowmart_db;
USE SCHEMA bronze;

-- Create an internal stage if not exists
CREATE STAGE IF NOT EXISTS snowmart_stage
    FILE_FORMAT = (TYPE = JSON);

-- Load customer data from the staged file into Bronze
COPY INTO customers_raw (raw_data, file_name)
FROM (
    SELECT $1, METADATA$FILENAME
    FROM @snowmart_stage/customers/
)
FILE_FORMAT = (TYPE = JSON)
ON_ERROR = CONTINUE;

-- Load order data from the staged file into Bronze
COPY INTO orders_raw (raw_data, file_name)
FROM (
    SELECT $1, METADATA$FILENAME
    FROM @snowmart_stage/orders/
)
FILE_FORMAT = (TYPE = JSON)
ON_ERROR = CONTINUE;
