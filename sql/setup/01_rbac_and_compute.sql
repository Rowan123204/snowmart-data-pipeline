-- ====================================================================
-- SCRIPT: 01_rbac_and_compute.sql
-- PURPOSE: Create the main Role and Warehouse for the pipeline
-- ====================================================================

-- 1. Create Role (Requires SECURITYADMIN)
USE ROLE SECURITYADMIN;
CREATE ROLE IF NOT EXISTS snowmart_data_engineer;

-- Grant role to SYSADMIN to maintain the hierarchy
GRANT ROLE snowmart_data_engineer TO ROLE SYSADMIN;

-- Grant role to the current user so you can actually use it
GRANT ROLE snowmart_data_engineer TO USER CURRENT_USER();

-- 2. Create Warehouse (Requires SYSADMIN)
USE ROLE SYSADMIN;
CREATE WAREHOUSE IF NOT EXISTS snowmart_etl_wh
    WITH 
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- Grant permissions to use the warehouse to our custom role
GRANT USAGE ON WAREHOUSE snowmart_etl_wh TO ROLE snowmart_data_engineer;
GRANT OPERATE ON WAREHOUSE snowmart_etl_wh TO ROLE snowmart_data_engineer;
