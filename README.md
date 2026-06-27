# GCP Retail Analytics Pipeline

An advanced end-to-end retail data engineering pipeline on Google Cloud Platform that ingests
multi-source CSV data (sales, customers, inventory), validates, transforms via PySpark joins
and window functions, loads into BigQuery, and publishes completion events via Pub/Sub.

## Architecture

GCS Landing Bucket → Cloud Composer (Airflow) → Dataproc (PySpark)
→ GCS Curated Bucket → BigQuery → Pub/Sub → GCS Archive Bucket

## Pipeline Flow

1. **GCS Sensors (x3)** — Wait in parallel for sales, customers, and inventory CSVs
2. **Validation Task** — Confirms all files are ready; pushes status via XCom
3. **Branch Operator** — Routes to ETL or failure path based on validation status
4. **Dataproc PySpark Job** — Multi-source joins, deduplication, window functions, aggregations
5. **BigQuery Load** — Appends fact_sales table using GCSToBigQueryOperator
6. **Aggregation Query** — Creates daily_sales_summary table (GROUP BY store, category)
7. **Pub/Sub Notification** — Publishes pipeline completion event to retail-pipeline-topic
8. **File Archive** — Moves processed CSVs from landing to archive bucket

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow (Cloud Composer) |
| Processing | Apache Spark (Dataproc / PySpark) |
| Raw Storage | Google Cloud Storage — Landing Bucket |
| Curated Storage | Google Cloud Storage — Curated Bucket |
| Archive Storage | Google Cloud Storage — Archive Bucket |
| Warehouse | BigQuery |
| Messaging | Google Cloud Pub/Sub |
| Language | Python |
| Cloud | GCP |

## Data Sources

| File | Columns |
|---|---|
| sales.csv | sale_id, store_id, product_id, customer_id, quantity, sale_amount, sale_date |
| customers.csv | customer_id, name, city, state, membership |
| inventory.csv | product_id, product_name, category, stock_quantity |

## PySpark Transformations

- Null removal and deduplication on `sale_id`
- Left joins: sales ← customers (on customer_id), sales ← inventory (on product_id)
- Window function: `rank()` over `store_id` partitioned by `sale_amount DESC`
- Aggregations: total_sales, avg_sales, transaction_count grouped by store and category
- Output: fact_sales Parquet written to curated bucket

## BigQuery Tables

- `fact_sales` — row-level transaction fact table (WRITE_APPEND)
- `daily_sales_summary` — aggregated summary table (CREATE OR REPLACE)

## Setup

1. Upload CSVs to:
   - `gs://retail-landing-bucket/sales/sales.csv`
   - `gs://retail-landing-bucket/customers/customers.csv`
   - `gs://retail-landing-bucket/inventory/inventory.csv`
2. Upload `retail_etl.py` to `gs://retail-landing-bucket/scripts/`
3. Deploy DAG to Cloud Composer `dags/` folder
4. Create Pub/Sub topic: `retail-pipeline-topic`
5. Trigger DAG manually from Airflow UI

## Key Concepts Demonstrated

- Parallel GCS sensing for multi-file ingestion
- BranchPythonOperator for conditional DAG routing
- XCom for inter-task validation status passing
- Multi-source PySpark joins across 3 datasets
- Window functions for in-partition ranking
- Three-tier GCS architecture (landing → curated → archive)
- Pub/Sub event publishing from Airflow
- BigQuery fact table + summary table pattern
