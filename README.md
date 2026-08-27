# SnowMart Data Pipeline

An end-to-end, production-style Data Engineering pipeline built on **Snowflake** and orchestrated by **Apache Airflow**.

The pipeline ingests raw e-commerce JSON data, processes it through a Medallion Architecture (Bronze â†’ Silver â†’ Gold), and runs automated data quality checks at the end of every run.

---

## Architecture

```
[ Raw JSON Data ]
        â”‚
        â–¼  (Airflow: COPY INTO Bronze)
[ Bronze Layer ]  â€” customers_raw, orders_raw (VARIANT)
        â”‚
        â–¼  (Snowflake Streams: CDC)
[ Streams ]  â€” Capture only new/changed rows
        â”‚
        â”œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
        â–¼  (Airflow: Silver SQL)            â–¼  (Airflow: Silver SQL)
[ dim_customers ]                    [ fact_orders ]
  SCD Type 2                          LATERAL FLATTEN
  Historical tracking                 Nested JSON arrays
        â”‚                                   â”‚
        â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
                         â–¼  (Airflow: Gold SQL)
              [ Gold Layer ]
              daily_sales, customer_summary
                         â”‚
                         â–¼  (Airflow: SQLCheckOperator)
              [ Data Quality Checks ]
              Automated failure on anomalies
```

---

## Technologies

| Technology | Role |
|---|---|
| **Snowflake** | Data warehouse, compute, storage, CDC via Streams |
| **Apache Airflow** | Pipeline orchestration, scheduling, retries |
| **SQL (Snowflake dialect)** | All transformation logic |
| **Python** | Data generation script |

---

## Project Structure

```
snowmart-data-pipeline/
â”‚
â”œâ”€â”€ README.md
â”œâ”€â”€ requirements.txt                  # Airflow + Snowflake provider dependencies
â”‚
â”œâ”€â”€ airflow/
â”‚   â””â”€â”€ dags/
â”‚       â””â”€â”€ snowmart_dag.py           # Single pipeline orchestrator
â”‚
â”œâ”€â”€ sql/
â”‚   â”œâ”€â”€ setup/
â”‚   â”‚   â”œâ”€â”€ 01_rbac_and_compute.sql   # Roles, Warehouses (run once)
â”‚   â”‚   â””â”€â”€ 02_tables_and_streams.sql # Database, schemas, tables, streams (run once)
â”‚   â”œâ”€â”€ bronze/
â”‚   â”‚   â””â”€â”€ copy_into_bronze.sql      # Ingestion from stage to raw tables
â”‚   â”œâ”€â”€ silver/
â”‚   â”‚   â”œâ”€â”€ customers_scd2.sql        # SCD Type 2 customer dimension
â”‚   â”‚   â””â”€â”€ orders_flatten.sql        # JSON array flattening into fact table
â”‚   â”œâ”€â”€ gold/
â”‚   â”‚   â”œâ”€â”€ daily_sales.sql           # Daily revenue aggregation
â”‚   â”‚   â””â”€â”€ customer_summary.sql      # Customer lifetime value summary
â”‚   â””â”€â”€ quality/
â”‚       â””â”€â”€ data_quality_checks.sql   # Completeness, uniqueness, validity rules
â”‚
â”œâ”€â”€ data/
â”‚   â”œâ”€â”€ initial_load/                 # Day 1 JSON files (customers + orders)
â”‚   â””â”€â”€ incremental_load/             # Day 2 JSON files (CDC simulation)
â”‚
â””â”€â”€ scripts/
    â””â”€â”€ data_generator.py             # Generates realistic mock JSON data
```

---

## Key Concepts Demonstrated

- **Medallion Architecture** â€” Bronze (raw), Silver (cleansed), Gold (analytics-ready)
- **Semi-structured data** â€” Ingesting and querying Snowflake `VARIANT` columns
- **Streams** â€” Append-only CDC streams for incremental processing
- **LATERAL FLATTEN** â€” Exploding nested JSON arrays into relational rows
- **SCD Type 2** â€” Tracking historical changes to customer attributes (city, email) with `start_date`, `end_date`, and `is_current`
- **MERGE** â€” Upsert logic for idempotent pipeline runs
- **RBAC** â€” Role-based access control with dedicated ETL and BI warehouses
- **Airflow orchestration** â€” DAG with task dependencies, retries, and automated DQ failure
- **Data Quality** â€” Automated checks that halt the pipeline on anomalies

---

## How to Run

### Prerequisites
1. A Snowflake account
2. Apache Airflow with the Snowflake provider installed (`pip install -r requirements.txt`)
3. A Snowflake connection configured in Airflow named `snowflake_prod_conn`

### Step 1 â€” Setup Snowflake (run once)
Execute these scripts in Snowflake in order:
```
sql/setup/01_rbac_and_compute.sql
sql/setup/02_tables_and_streams.sql
```

### Step 2 â€” Generate Mock Data
```bash
python scripts/data_generator.py
```
This creates JSON files in `data/initial_load/` and `data/incremental_load/`.

### Step 3 â€” Upload Data to Snowflake Stage
Using SnowSQL CLI:
```bash
# Upload initial load files
snowsql -q "PUT file://data/initial_load/customers.json @snowmart_db.bronze.snowmart_stage/customers/ AUTO_COMPRESS=TRUE;"
snowsql -q "PUT file://data/initial_load/orders.json @snowmart_db.bronze.snowmart_stage/orders/ AUTO_COMPRESS=TRUE;"
```

### Step 4 â€” Trigger the Airflow DAG
Deploy `airflow/dags/snowmart_dag.py` to your Airflow DAGs folder and trigger the `snowmart_daily_pipeline` DAG. The full pipeline runs automatically.

### Step 5 â€” Simulate an Incremental Load (CDC)
Upload the `data/incremental_load/` files to the same stage. Trigger the DAG again. Airflow will pick up only the new rows via Snowflake Streams, and the SCD Type 2 logic will:
- Close old records for customers who changed city or email
- Insert new records with updated attributes
- Preserve the full history of changes

---

## Pipeline Validation

After a successful run:
```sql
-- Confirm SCD2 history is preserved for updated customers
SELECT customer_id, city, is_current, start_date, end_date
FROM snowmart_db.silver.dim_customers
ORDER BY customer_id, start_date;

-- Confirm Gold aggregations populated
SELECT * FROM snowmart_db.gold.daily_sales ORDER BY sales_date;
```

---

## Pipeline in Action

### Airflow DAG — Full Pipeline Success
![Airflow DAG All Success](screenshots/dag_all_success.png)

### Airflow DAG — Overview & Schedule
![Airflow DAG Overview](screenshots/airflow_dag_overview.png)

### Snowflake — Silver Layer: dim_customers (SCD Type 2)
![Silver dim_customers](screenshots/snowflake_silver_dim_customers.png)

### Snowflake — Gold Layer: Daily Sales Aggregation
![Gold daily_sales](screenshots/snowflake_gold_daily_sales.png)

### Snowflake — Gold Layer: Customer Lifetime Summary
![Gold customer_summary](screenshots/snowflake_gold_customer_summary.png)
