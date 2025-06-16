import pandas as pd
import logging
from etl.s3_logger import S3Logger
from airflow.models import Variable
from datetime import datetime, timedelta


# Configure logging
bucket = Variable.get("S3_BUCKET_NAME")
log_key = f"logs/metadata/etl_kpi_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
s3_logger = S3Logger(bucket, log_key)
logger = s3_logger.get_logger()

def compute_kpis():
    """"Compute KPIs from the staged streaming data and metadata."""
    try:
        # Read the staged user and song metadata, and streaming data
        staging_prefix = Variable.get("STAGING_PREFIX", default_var="data/staging/")
        user_path = f's3://{bucket}/{staging_prefix}user_metadata_staged.csv'
        song_path = f's3://{bucket}/{staging_prefix}song_metadata_staged.csv'
        stream_path = f's3://{bucket}/{staging_prefix}streaming_data_staged.csv'

        # Define paths for KPI outputs
        genre_kpi_path = f's3://{bucket}/{staging_prefix}genre_kpis.csv'
        hourly_kpi_path = f's3://{bucket}/{staging_prefix}hourly_kpis.csv'

        logger.info("Reading staged CSVs from S3...")
        user_df = pd.read_csv(user_path)
        song_df = pd.read_csv(song_path)
        stream_df = pd.read_csv(stream_path)

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
        with open(genre_kpi_path, 'w') as f:
            genre_kpis.to_csv(f, index=False)

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
        with open(hourly_kpi_path, 'w') as f:
            hourly_kpis.to_csv(f, index=False)

        logger.info("KPI computation and export to S3 completed successfully.")

    except Exception as e:
        logger.error(f"KPI transformation failed: {e}")
        raise
