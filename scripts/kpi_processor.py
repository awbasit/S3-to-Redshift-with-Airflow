import pandas as pd
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('/opt/airflow/logs/transform.log')
logger.addHandler(handler)

def compute_kpis():
    """"Compute KPIs from the staged streaming data and metadata."""
    try:
        # Read the staged user and song metadata, and streaming data
        staging_dir = '/opt/airflow/data/staging/'
        output_dir = '/opt/airflow/data/output/'
        user_df = pd.read_csv(f'{staging_dir}user_metadata_staged.csv')
        song_df = pd.read_csv(f'{staging_dir}song_metadata_staged.csv')
        stream_df = pd.read_csv(f'{staging_dir}streaming_data_staged.csv')

        # Ensure the dataframes have the expected columns
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
        genre_kpis.to_csv(f'{output_dir}genre_kpis.csv', index=False)

        """
        3. Hourly KPIs
        4. Unique listeners per hour
        5. Top artist per hour
        6. Track diversity index per hour
        """
        hourly_kpis = merged.groupby('hour').agg(
            unique_listeners=('user_id', pd.Series.nunique),
            top_artists=('track_id', lambda x: x.mode()[0]),
            track_diversity_index=('track_id', lambda x: len(set(x))/len(x))
        ).reset_index()
        hourly_kpis.to_csv(f'{output_dir}hourly_kpis.csv', index=False)

        logger.info("KPI computation successful.")
    except Exception as e:
        logger.error(f"KPI transformation failed: {e}")
        raise
