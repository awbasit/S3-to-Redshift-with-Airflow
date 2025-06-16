# import sys
import os
# sys.path.append('/opt/airflow')

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import timedelta
from etl.extract_metadata import extract_user_and_song_data
from etl.extract_stream_data import extract_streaming_batch
from etl.schema_check import validate_datasets
# from etl.kpi_processor import compute_kpis
# from etl.load_to_redshift import upsert_to_redshift
# from s3_logger import get_s3_logger

"""ETL Streaming Pipeline DAG
This DAG orchestrates the ETL process for streaming data from S3 to Redshift,
including metadata extraction, streaming data extraction, data validation, 
KPI computation, loading to Redshift, and archiving processed files.
"""

# Configure logging
# logger, s3_handler = get_s3_logger(__name__, log_name="dag_etl_streaming_pipeline")

# Airflow DAG configuration
default_args = {
    'owner': 'dataengineer',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
    'depends_on_past': False,
}

# Define the DAG
with DAG(
    dag_id='etl_streaming_pipeline_s3_redshift',
    default_args=default_args,
    description='ETL pipeline for streaming data from S3 to Redshift',
    start_date=days_ago(1),
    schedule_interval='@hourly',
    catchup=False,
    tags=['etl', 'streaming', 'music', 's3', 'redshift'],
    max_active_runs=1,
) as dag:

    def extract_metadata_task():
        """Extract user and song metadata from S3"""
        try:
            # logger.info("Starting metadata extraction from S3")
            extract_user_and_song_data()
            # logger.info("Metadata extraction completed successfully")
        except Exception as e:
            # logger.error(f"Metadata extraction failed: {str(e)}")
            raise
        # finally:
        #     s3_handler.flush_to_s3()

    def extract_streaming_data_task():
        """Extract streaming data from S3"""
        try:
            # logger.info("Starting streaming data extraction from S3")
            extract_streaming_batch()
            # logger.info("Streaming data extraction completed successfully")
        except Exception as e:
            # logger.error(f"Streaming data extraction failed: {str(e)}")
            raise
        # finally:
        #     s3_handler.flush_to_s3()

    def validate_data_task():
        """Validate extracted datasets"""
        try:
            # logger.info("Starting data validation")
            validate_datasets()
            # logger.info("Data validation completed successfully")
        except Exception as e:
            # logger.error(f"Data validation failed: {str(e)}")
            raise
        # finally:
        #     s3_handler.flush_to_s3()

    # def transform_and_compute_kpis_task():
    #     """Transform data and compute KPIs"""
    #     try:
            # logger.info("Starting data transformation and KPI computation")
            # compute_kpis()
            # logger.info("Data transformation and KPI computation completed successfully")
        # except Exception as e:
            # logger.error(f"Data transformation and KPI computation failed: {str(e)}")
            # raise
        # finally:
        #     s3_handler.flush_to_s3()

    # def load_to_redshift_task():
        # """Load processed data to Redshift"""
        # try:
            # logger.info("Starting data load to Redshift")
            # upsert_to_redshift()
            # logger.info("Data load to Redshift completed successfully")
        # except Exception as e:
            # logger.error(f"Data load to Redshift failed: {str(e)}")
            # raise

    # def archive_processed_files_task():
    #     """Archive processed files in S3"""
    #     try:
    #         logger.info("Starting file archiving in S3")
    #         archive_stream_files()
    #         logger.info("File archiving completed successfully")
    #     except Exception as e:
    #         logger.error(f"File archiving failed: {str(e)}")
    #         raise

    # Define tasks
    extract_metadata = PythonOperator(
        task_id='extract_metadata',
        python_callable=extract_metadata_task,
        pool='default_pool',
    )

    extract_streaming = PythonOperator(
        task_id='extract_streaming_data',
        python_callable=extract_streaming_data_task,
        pool='default_pool',
    )

    validate_data = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data_task,
        pool='default_pool',
    )

    # transform_kpis = PythonOperator(
    #     task_id='transform_and_compute_kpis',
    #     python_callable=transform_and_compute_kpis_task,
    #     pool='default_pool',
    # )

    # load_redshift = PythonOperator(
    #     task_id='load_to_redshift',
    #     python_callable=load_to_redshift_task,
    #     pool='default_pool',
    # )

    # archive_files = PythonOperator(
    #     task_id='archive_processed_files',
    #     python_callable=archive_processed_files_task,
    #     pool='default_pool',
    # )

    # Define task dependencies
    # extract_metadata >> extract_streaming >> validate_data >> transform_kpis >> load_redshift >> archive_files
    extract_metadata >> extract_streaming >> validate_data