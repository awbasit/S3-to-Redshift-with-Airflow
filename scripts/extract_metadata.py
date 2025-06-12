import pandas as pd
import os
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('/opt/airflow/logs/extract_metadata.log')
logger.addHandler(handler)

def extract_user_and_song_data():
    """
    Extract user and song metadata from CSV files and stage them for further processing.
    """
    try:
        # Read user and song metadata from CSV files located in the metadata directory
        # in the Airflow data directory
        user_df = pd.read_csv('/opt/airflow/data/metadata/users.csv')
        song_df = pd.read_csv('/opt/airflow/data/metadata/songs.csv')

        # Staging the data by saving to a new directory in the Airflow data directory
        user_df.to_csv('/opt/airflow/data/staging/user_metadata_staged.csv', index=False)
        song_df.to_csv('/opt/airflow/data/staging/song_metadata_staged.csv', index=False)

        logger.info("Metadata extracted successfully.")
    except Exception as e:
        logger.error(f"Error extracting metadata: {e}")
        raise
    