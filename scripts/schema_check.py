import pandas as pd
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('/opt/airflow/logs/validate.log')
logger.addHandler(handler)

def validate_datasets():
    required_cols = {
        'user_metadata_staged.csv': ["user_id", "user_country", "user_age"],
        'song_metadata_staged.csv': ["track_id", "track_genre", "duration_ms"],
        'streaming_data_staged.csv': ["user_id", "track_id", "listen_time"]
    }

    for file, columns in required_cols.items():
        try:
            df = pd.read_csv(f"/opt/airflow/data/staging/{file}")
            for col in columns:
                if col not in df.columns:
                    raise ValueError(f"Missing column '{col}' in {file}")
            logger.info(f"{file} passed schema validation.")
        except Exception as e:
            logger.error(f"Validation failed for {file}: {e}")
            raise