# SnowMart Data Pipeline

An end-to-end, production-style Data Engineering pipeline built on **Snowflake** and orchestrated by **Apache Airflow**.

The pipeline ingests raw e-commerce JSON data, processes it through a Medallion Architecture (Bronze → Silver → Gold), and runs automated data quality checks at the end of every run.

---

## Architecture

```
[ Raw JSON Data ]
        │
        ▼  (Airflow: COPY INTO Bronze)
[ Bronze Layer ]  — customers_raw, orders_raw (VARIANT)
        │
        ▼  (Snowflake Streams: CDC)
[ Streams ]  — Capture only new/changed rows
        │
        ├──────────────────────────────────┐
        ▼  (Airflow: Silver SQL)            ▼  (Airflow: Silver SQL)
[ dim_customers ]                    [ fact_orders ]
  SCD Type 2                          LATERAL FLATTEN
  Historical tracking                 Nested JSON arrays
        │                                   │
        └────────────────┬─────────────────┘
                         ▼  (Airflow: Gold SQL)
              [ Gold Layer ]
              daily_sales, customer_summary
                         │
                         ▼  (Airflow: SQLCheckOperator)
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
│
├── README.md
├── requirements.txt                  # Airflow + Snowflake provider dependencies
│
├── airflow/
│   └── dags/
│       └── snowmart_dag.py           # Single pipeline orchestrator
│
├── sql/
│   ├── setup/
│   │   ├── 01_rbac_and_compute.sql   # Roles, Warehouses (run once)
│   │   └── 02_tables_and_streams.sql # Database, schemas, tables, streams (run once)
│   ├── bronze/
│   │   └── copy_into_bronze.sql      # Ingestion from stage to raw tables
│   ├── silver/
│   │   ├── customers_scd2.sql        # SCD Type 2 customer dimension
│   │   └── orders_flatten.sql        # JSON array flattening into fact table
│   ├── gold/
│   │   ├── daily_sales.sql           # Daily revenue aggregation
│   │   └── customer_summary.sql      # Customer lifetime value summary
│   └── quality/
│       └── data_quality_checks.sql   # Completeness, uniqueness, validity rules
│
├── data/
│   ├── initial_load/                 # Day 1 JSON files (customers + orders)
│   └── incremental_load/             # Day 2 JSON files (CDC simulation)
│
└── scripts/
    └── data_generator.py             # Generates realistic mock JSON data
```

---

## Key Concepts Demonstrated

- **Medallion Architecture** — Bronze (raw), Silver (cleansed), Gold (analytics-ready)
- **Semi-structured data** — Ingesting and querying Snowflake `VARIANT` columns
- **Streams** — Append-only CDC streams for incremental processing
- **LATERAL FLATTEN** — Exploding nested JSON arrays into relational rows
- **SCD Type 2** — Tracking historical changes to customer attributes (city, email) with `start_date`, `end_date`, and `is_current`
- **MERGE** — Upsert logic for idempotent pipeline runs
- **RBAC** — Role-based access control with dedicated ETL and BI warehouses
- **Airflow orchestration** — DAG with task dependencies, retries, and automated DQ failure
- **Data Quality** — Automated checks that halt the pipeline on anomalies

---

## How to Run

### Prerequisites
1. A Snowflake account
2. Apache Airflow with the Snowflake provider installed (`pip install -r requirements.txt`)
3. A Snowflake connection configured in Airflow named `snowflake_prod_conn`

### Step 1 — Setup Snowflake (run once)
Execute these scripts in Snowflake in order:
```
sql/setup/01_rbac_and_compute.sql
sql/setup/02_tables_and_streams.sql
```

### Step 2 — Generate Mock Data
```bash
python scripts/data_generator.py
```
This creates JSON files in `data/initial_load/` and `data/incremental_load/`.

### Step 3 — Upload Data to Snowflake Stage
Using SnowSQL CLI:
```bash
# Upload initial load files
snowsql -q "PUT file://data/initial_load/customers.json @snowmart_db.bronze.snowmart_stage/customers/ AUTO_COMPRESS=TRUE;"
snowsql -q "PUT file://data/initial_load/orders.json @snowmart_db.bronze.snowmart_stage/orders/ AUTO_COMPRESS=TRUE;"
```

### Step 4 — Trigger the Airflow DAG
Deploy `airflow/dags/snowmart_dag.py` to your Airflow DAGs folder and trigger the `snowmart_daily_pipeline` DAG. The full pipeline runs automatically.

### Step 5 — Simulate an Incremental Load (CDC)
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
