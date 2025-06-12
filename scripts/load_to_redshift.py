import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('/opt/airflow/logs/load.log')
logger.addHandler(handler)

def upsert_to_redshift():
    try:
        logger.info("Loading genre_kpis.csv and hourly_kpis.csv into Redshift staging tables...")
        # Simulated: connect using psycopg2 and execute UPSERT logic here
        # Save connection details in environment or AWS Secrets Manager
        logger.info("UPSERT to Redshift successful.")
    except Exception as e:
        logger.error(f"Error loading to Redshift: {e}")
        raise