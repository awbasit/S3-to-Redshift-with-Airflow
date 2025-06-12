import pandas as pd
import glob
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('/opt/airflow/logs/extract_stream.log')
logger.addHandler(handler)

def extract_streaming_batch():
    """
    Extract the latest streaming data from CSV files in the specified directory.
    """
    try:
        # Streaming data files are stored in /opt/airflow/data/streams/
        # and are named with a consistent pattern (e.g., streams_*.csv)
        # The files will contain all the csv files in that directory
        files = sorted(glob.glob('/opt/airflow/data/streams/*.csv'))
        if not files:
            logger.warning("No streaming files found.")
            return
        # The latest file is assumed to be the most recent one based on the filename
        latest_file = files[-1]
        df = pd.read_csv(latest_file)
        df.to_csv('/opt/airflow/data/staging/streaming_data_staged.csv', index=False)
        logger.info(f"Streaming data extracted from {latest_file}")
    except Exception as e:
        logger.error(f"Error extracting streaming data: {e}")
        raise