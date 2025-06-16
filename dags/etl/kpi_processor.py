import pandas as pd
import logging
import boto3
from io import StringIO
from etl.s3_logger import S3Logger
from airflow.models import Variable
from datetime import datetime, timedelta


# Configure logging
bucket = Variable.get("S3_BUCKET_NAME")
log_key = f"logs/metadata/etl_kpi_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
s3_logger = S3Logger(bucket, log_key)
logger = s3_logger.get_logger()


def read_csv_from_s3(bucket, key):
    """Download a CSV file from S3 and return it as a Pandas DataFrame."""
    s3 = boto3.client("s3")
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return pd.read_csv(StringIO(obj["Body"].read().decode("utf-8")))
    except Exception as e:
        logger.error(f"Failed to load {key} from S3: {e}")
        raise

def write_csv_to_s3(df, bucket, key):
    """Upload a Pandas DataFrame as CSV to S3."""
    s3 = boto3.client("s3")
    try:
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
        logger.info(f"Successfully wrote {key} to S3")
    except Exception as e:
        logger.error(f"Failed to upload {key} to S3: {e}")
        raise


def compute_kpis():
    """"Compute KPIs from the staged streaming data and metadata."""
    try:
        # Read the staged user and song metadata, and streaming data
        staging_prefix = Variable.get("S3_STAGING_PREFIX", default_var="data/staging").rstrip("/")
        user_key = f"{staging_prefix}/user_metadata_staged.csv"
        song_key = f"{staging_prefix}/song_metadata_staged.csv"
        stream_key = f"{staging_prefix}/streaming_data_staged.csv"

        # Define paths for KPI outputs
        genre_kpi_key = f"{staging_prefix}/genre_kpis.csv"
        hourly_kpi_key = f"{staging_prefix}/hourly_kpis.csv"

        logger.info("Reading staged CSVs from S3...")
        user_df = read_csv_from_s3(bucket, user_key)
        song_df = read_csv_from_s3(bucket, song_key)
        stream_df = read_csv_from_s3(bucket, stream_key)

        # Merging the dataframes on user_id and track_id
        merged = stream_df.merge(song_df, on='track_id').merge(user_df, on='user_id')
        merged['listen_time'] = pd.to_datetime(merged['listen_time'])
        merged['hour'] = merged['listen_time'].dt.hour


        # Compute KPIs
        """
        1. Total listens per user
        2. Average listen duration per user
        """
        genre_kpis = merged.groupby('track_genre').agg(
            listen_count=('track_id', 'count'),
            avg_duration=('duration_ms', 'mean')
        ).reset_index()

        # Save Genre KPIs to S3
        write_csv_to_s3(genre_kpis, bucket, genre_kpi_key)
        logger.info("Genre KPIs computed and saved to S3.")

        # Save Hourly KPIs to S3
        

        """
        3. Hourly KPIs
        4. Unique listeners per hour
        5. Top artist per hour
        6. Track diversity index per hour
        """
        # Compute Hourly KPIs
        hourly_kpis = merged.groupby('hour').agg(
            unique_listeners=('user_id', pd.Series.nunique),
            top_artists=('track_id', lambda x: x.mode().iloc[0] if not x.mode().empty else None),
            track_diversity_index=('track_id', lambda x: len(set(x)) / len(x))
        ).reset_index()

        # Save Hourly KPIs to S3
        logger.info("Computing hourly KPIs...")
        write_csv_to_s3(hourly_kpis, bucket, hourly_kpi_key)
        logger.info("KPI computation and export to S3 completed successfully.")

    except Exception as e:
        logger.error(f"KPI transformation failed: {e}")
        raise
