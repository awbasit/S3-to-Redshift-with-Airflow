import sys
import os
sys.path.append('/opt/airflow')

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from scripts.extract_metadata import extract_user_and_song_data
from scripts.extract_stream_data import extract_streaming_batch
from scripts.schema_check import validate_datasets
from scripts.kpi_processor import compute_kpis
from scripts.load_to_redshift import upsert_to_redshift
from scripts.archive_files import archive_stream_files

"""ETL Streaming Pipeline DAG
This DAG orchestrates the ETL process for streaming data, including metadata extraction,
streaming data extraction, data validation, KPI computation, loading to Redshift, and archiving processed files.
"""

# Airflow DAG configuration
default_args = {
    'owner': 'data_engineer',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG

with DAG(
    dag_id='etl_streaming_pipeline', # Unique identifier for the DAG
    default_args=default_args, # Default arguments for the DAG
    start_date=days_ago(1), # Start date for the DAG
    schedule_interval='@hourly', # Schedule interval for the DAG
    catchup=False, # Do not backfill missed runs
    tags=['etl', 'streaming', 'music'], # Tags for categorization
) as dag:

    """
    Define the tasks in the DAG and their dependencies.
    Each task corresponds to a step in the ETL process.
    """
    def extract_metadata():
        extract_user_and_song_data()

    def extract_streaming_data():
        extract_streaming_batch()

    def validate_data():
        validate_datasets()

    def transform_and_compute_kpis():
        compute_kpis()

    def load_to_redshift():
        upsert_to_redshift()

    def archive_processed_files():
        archive_stream_files()

    extract_metadata_task = PythonOperator(
        task_id='extract_metadata',
        python_callable=extract_metadata,
    )

    extract_streaming_task = PythonOperator(
        task_id='extract_streaming_data',
        python_callable=extract_streaming_data,
    )

    validate_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
    )

    transform_task = PythonOperator(
        task_id='transform_and_compute_kpis',
        python_callable=transform_and_compute_kpis,
    )

    load_task = PythonOperator(
        task_id='load_to_redshift',
        python_callable=load_to_redshift,
    )

    archive_task = PythonOperator(
        task_id='archive_processed_files',
        python_callable=archive_processed_files,
    )
  
    extract_metadata_task >> extract_streaming_task >> validate_task >> transform_task >> load_task >> archive_task
