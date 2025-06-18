# ETL Streaming Pipeline: S3 to Redshift with Airflow

## 📌 Project Overview

This project implements a robust data pipeline using **Apache Airflow** to extract, transform, and load streaming music data into **Amazon Redshift**. Data is ingested from **S3**, processed through various KPI metrics (like genre KPIs and hourly KPIs), and stored in Redshift for downstream analytics.

---

## 🛠️ Tech Stack

- **Apache Airflow** (Managed by MWAA)
- **Amazon S3** (Data Lake)
- **Amazon Redshift** (Data Warehouse)
- **Python** (ETL scripts)
- **Pandas** (Data manipulation)
- **SQL** (Upsert & DDL operations)

---

## 🧩 Pipeline Components

### DAG: `etl_streaming_pipeline_s3_redshift`
Triggered manually or on schedule to run the complete ETL process.

### Tasks:
1. **Extract Streaming Data**
   - Extracts raw data from S3
   - Cleans, formats, and writes intermediate files to `staging/`

2. **Extract Metadata**
   - Pulls additional metadata about users and songs
   - Used to enrich the streaming logs

3. **Schema Validation**
   - Ensures required columns like `unique_listeners`, `total_streams`, and `top_artists` exist

4. **Load to Redshift**
   - Automatically creates missing tables
   - Performs **upsert** operations for:
     - `genre_kpis.csv`
     - `hourly_kpis.csv`
   - Adds calculated columns like `total_streams` (default avg: 2)

---

## 📊 Sample Output Columns

### Genre KPIs
- `genre`
- `total_streams`
- `avg_stream_duration`
- `unique_listeners`

### Hourly KPIs
- `hour`
- `unique_listeners`
- `top_artists`
- `track_diversity_index`
- `total_streams` *(calculated)*

---

## ⚠️ Error Handling

- Catches and logs missing columns
- Warns if non-critical fields are missing
- Falls back to calculated defaults if needed
- Marks task as `UP_FOR_RETRY` or `FAILED` based on exception

---

## 🧪 Testing & Validation

- Logs available via **CloudWatch**
- Schema check step ensures required structure
- Preview sample printed before Redshift load
- Successful upsert logs include row counts

---

## 🚀 How to Run

> Ensure Airflow and AWS credentials are configured correctly.

1. Upload input files to:
   - `s3://<bucket-name>/data/streaming/`
   - `s3://<bucket-name>/data/metadata/`
2. Trigger the DAG `etl_streaming_pipeline_s3_redshift`
3. Monitor logs in **CloudWatch** or Airflow UI

---

## Project Structure

├── dags/
│ ├── etl_streaming_pipeline.py
│ └── etl/
│ ├── extract_stream_data.py
│ ├── extract_metadata.py
│ ├── schema_check.py
│ ├── s3_logger.py
│ ├── kpi_processor.py
│ └── load_to_redshift.py
├── requirements.txt
└── README.md


---

## How to Deploy

1. **Install Requirements**
   ```bash
   pip install apache-airflow amazon-redshift python-dotenv boto3


## Security
Use IAM roles with minimal S3 and Redshift permissions.

Store secrets in Airflow Connections securely.


---