-- ====================================================================
-- SCRIPT: 01_rbac_and_compute.sql
-- PURPOSE: Set up Role-Based Access Control (RBAC) and Virtual Warehouses
-- ====================================================================

USE ROLE SECURITYADMIN;

-- 1. Create Custom Roles
CREATE ROLE IF NOT EXISTS snowmart_data_engineer;
CREATE ROLE IF NOT EXISTS snowmart_analyst;

-- 2. Build Role Hierarchy (SYSADMIN inherits custom roles)
GRANT ROLE snowmart_data_engineer TO ROLE SYSADMIN;
GRANT ROLE snowmart_analyst TO ROLE SYSADMIN;

-- 3. Assign Role to User
GRANT ROLE snowmart_data_engineer TO USER ROWAN2005;

USE ROLE SYSADMIN;

-- 4. Create Workload-Isolated Virtual Warehouses
-- ETL Warehouse (For Data Transformation)
CREATE WAREHOUSE IF NOT EXISTS snowmart_etl_wh
    WITH WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- BI Warehouse (For Data Analytics & Dashboarding)
CREATE WAREHOUSE IF NOT EXISTS snowmart_bi_wh
    WITH WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- 5. Grant Warehouse Privileges
GRANT USAGE ON WAREHOUSE snowmart_etl_wh TO ROLE snowmart_data_engineer;
GRANT USAGE ON WAREHOUSE snowmart_bi_wh TO ROLE snowmart_analyst;
