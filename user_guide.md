# User Guide: S3 to Redshift ETL with Airflow

Welcome! This guide will walk you through using the ETL pipeline even if you're not a developer.

---

## 🔧 What This Does

This pipeline:
- Collects data stored in a file (like CSV or JSON) in S3
- Moves it into a Redshift database table
- Runs this daily or on-demand

---

## 🧭 How to Use

### Step 1: Upload Your File

- Go to the S3 bucket your team uses (e.g., `my-data-bucket`)
- Upload your data file into a folder (e.g., `data/2025-06-17.csv`)

### Step 2: Airflow Setup

- Open the **Airflow UI**
- Locate the DAG named: `s3_to_redshift_dag`
- Click **Trigger DAG** to start the load

### Step 3: Confirm the Load

- Go to Redshift Query Editor
- Run:
  ```sql
  SELECT COUNT(*) FROM rental_db.raw_t;
