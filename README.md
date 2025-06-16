# Cloud Deployment Guide for ETL Pipeline

## Prerequisites

### 1. AWS Resources Setup

#### S3 Bucket Structure
```
your-etl-bucket/
├── metadata/
│   ├── users.csv
│   └── songs.csv
├── streams/
│   ├── stream_2024_01_01_00.csv
│   ├── stream_2024_01_01_01.csv
│   └── ...
├── staging/
│   └── (processed files will be stored here)
└── processed/
    └── (archived files will be stored here)
```

#### Redshift Cluster
- Create a Redshift cluster with appropriate node type and count
- Ensure VPC security groups allow Airflow to connect
- Note down cluster endpoint, database name, and credentials

#### IAM Roles and Policies
Create IAM role for Airflow with these policies:
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::your-etl-bucket",
                "arn:aws:s3:::your-etl-bucket/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "redshift:GetClusterCredentials"
            ],
            "Resource": "*"
        }
    ]
}
```

### 2. Airflow Environment Setup

#### Option A: Amazon MWAA (Managed Workflow for Apache Airflow)
1. Create MWAA environment
2. Upload requirements.txt to S3
3. Configure environment variables
4. Upload DAGs to S3 DAGs folder

#### Option B: Self-managed Airflow
1. Install Apache Airflow 2.7.0+
2. Install required packages from requirements.txt
3. Configure Airflow connections and variables

## Deployment Steps

### Step 1: Configure Airflow Variables
In Airflow Web UI, go to Admin > Variables and add:

```bash
# Core S3 Configuration
S3_BUCKET_NAME = "your-etl-bucket-name"
S3_METADATA_PREFIX = "metadata/"
S3_STREAMS_PREFIX = "streams/"
S3_STAGING_PREFIX = "staging/"
S3_PROCESSED_PREFIX = "processed/"

# Redshift Configuration
REDSHIFT_HOST = "your-cluster.region.redshift.amazonaws.com"
REDSHIFT_PORT = "5439"
REDSHIFT_DATABASE = "your-database"
REDSHIFT_USER = "your-username"
REDSHIFT_PASSWORD = "your-password"

# AWS Credentials (or use IAM roles)
AWS_ACCESS_KEY_ID = "your-access-key"
AWS_SECRET_ACCESS_KEY = "your-secret-key"
```

### Step 2: Upload Files to Airflow
```
/opt/airflow/
├── dags/
│   └── etl_pipeline.py
└── scripts/
    ├── extract_metadata.py
    ├── extract_stream_data.py
    ├── schema_check.py
    ├── kpi_processor.py
    ├── load_to_redshift.py
    └── archive_files.py
```

### Step 3: Prepare Your Data
Upload your data files to S3:
```bash
# Upload metadata
aws s3 cp users.csv s3://your-etl-bucket/metadata/
aws s3 cp songs.csv s3://your-etl-bucket/metadata/

# Upload streaming data
aws s3 cp stream_data.csv s3://your-etl-bucket/streams/
```

### Step 4: Create Redshift Tables (Optional)
The pipeline will create tables automatically, but you can pre-create them:

```sql
-- Genre KPIs table
CREATE TABLE IF NOT EXISTS genre_kpis (
    genre VARCHAR(255),
    total_streams BIGINT,
    unique_users BIGINT,
    avg_stream_duration DECIMAL(10,2),
    date_processed TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (genre, date_processed)
) DISTSTYLE KEY DISTKEY (genre);

-- Hourly KPIs table
CREATE TABLE IF NOT EXISTS hourly_kpis (
    hour TIMESTAMP,
    total_streams BIGINT,
    unique_users BIGINT,
    unique_songs BIGINT,
    avg_stream_duration DECIMAL(10,2),
    date_processed TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (hour, date_processed)
) DISTSTYLE KEY DIST