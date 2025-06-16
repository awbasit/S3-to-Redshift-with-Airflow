import shutil
import glob
import os
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('/opt/airflow/logs/archive.log')
logger.addHandler(handler)

def archive_stream_files():
    """Archive processed stream files by moving them to an archive directory."""
    try:
        # Ensure these directories exists
        archive_dir = '/opt/airflow/data/streams/archive'
        file_path = '/opt/airflow/data/streams'

        # Check if the archive directory exists, if not, create it
        if not os.path.exists(archive_dir):
            os.makedirs(archive_dir)
            logger.info(f"Created archive directory: {archive_dir}")

        # Check if the stream files directory exists
        if not os.path.exists(file_path):
            logger.error(f"Stream files directory {file_path} does not exist.")
            return

        # Move all CSV files from the stream directory to the archive directory
        files = glob.glob(f'{file_path}/*.csv')
        for file in files:
            filename = os.path.basename(file)
            shutil.move(file, f"{archive_dir}/{filename}")
        logger.info("Archived processed stream files.")
    except Exception as e:
        logger.error(f"Error archiving stream files: {e}")
        raise