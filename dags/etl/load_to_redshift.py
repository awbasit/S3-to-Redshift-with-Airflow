import psycopg2
import pandas as pd
import boto3
from airflow.models import Variable
from botocore.exceptions import ClientError
import io
from typing import Dict, List, Optional
import json
from etl.s3_logger import S3Logger
from datetime import datetime, timedelta

# Configure logging
bucket = Variable.get("S3_BUCKET_NAME")
log_key = f"logs/redshift/etl_redshift_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
s3_logger = S3Logger(bucket, log_key)
logger = s3_logger.get_logger()

class RedshiftLoader:
    """Class to handle loading data from S3 to Redshift"""
    
    def __init__(self):
        # Redshift connection parameters
        self.redshift_host = Variable.get("REDSHIFT_HOST")
        self.redshift_port = Variable.get("REDSHIFT_PORT", "5439")
        self.redshift_database = Variable.get("REDSHIFT_DATABASE")
        self.redshift_user = Variable.get("REDSHIFT_USER")
        self.redshift_password = Variable.get("REDSHIFT_PASSWORD")
        
        # S3 parameters
        self.s3_client = boto3.client('s3')
        self.bucket_name = Variable.get("S3_BUCKET_NAME")
        self.staging_prefix = Variable.get("S3_STAGING_PREFIX", "staging/")
        self.processed_prefix = Variable.get("S3_PROCESSED_PREFIX", "processed/")
        
        # AWS credentials for Redshift COPY command
        self.aws_access_key_id = Variable.get("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = Variable.get("AWS_SECRET_ACCESS_KEY")
        
        self.connection = None
    
    def connect_to_redshift(self):
        """Establish connection to Redshift"""
        try:
            # logger.info("Connecting to Redshift")
            self.connection = psycopg2.connect(
                host=self.redshift_host,
                port=self.redshift_port,
                database=self.redshift_database,
                user=self.redshift_user,
                password=self.redshift_password
            )
            self.connection.autocommit = False
            # logger.info("Successfully connected to Redshift")
        except Exception as e:
            logger.error(f"Error connecting to Redshift: {e}")
            raise
    
    def disconnect_from_redshift(self):
        """Close Redshift connection"""
        if self.connection:
            self.connection.close()
            logger.info("Disconnected from Redshift")
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> Optional[List]:
        """Execute a query on Redshift"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                if cursor.description:
                    return cursor.fetchall()
                return None
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            self.connection.rollback()
            raise
    
    def create_tables_if_not_exist(self):
        """Create tables if they don't exist"""
        
        # Genre KPIs table
        genre_kpis_table = """
        CREATE TABLE IF NOT EXISTS genre_kpis (
            genre VARCHAR(255),
            total_streams BIGINT,
            unique_users BIGINT,
            avg_stream_duration DECIMAL(10,2),
            date_processed TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (genre, date_processed)
        ) DISTSTYLE KEY DISTKEY (genre);
        """
        
        # Hourly KPIs table
        hourly_kpis_table = """
        CREATE TABLE IF NOT EXISTS hourly_kpis (
            hour TIMESTAMP,
            total_streams BIGINT,
            unique_users BIGINT,
            unique_songs BIGINT,
            avg_stream_duration DECIMAL(10,2),
            date_processed TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (hour, date_processed)
        ) DISTSTYLE KEY DISTKEY (hour);
        """
        
        try:
            logger.info("Creating tables if they don't exist")
            self.execute_query(genre_kpis_table)
            self.execute_query(hourly_kpis_table)
            self.connection.commit()
            logger.info("Tables created successfully")
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            self.connection.rollback()
            raise
    
    def read_csv_from_s3(self, key: str) -> pd.DataFrame:
        """Read CSV file from S3"""
        try:
            logger.info(f"Reading file from S3: s3://{self.bucket_name}/{key}")
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            df = pd.read_csv(io.BytesIO(response['Body'].read()))
            logger.info(f"Successfully read {len(df)} rows from {key}")
            return df
        except ClientError as e:
            logger.error(f"Error reading file from S3: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error reading CSV: {e}")
            raise
    
    def copy_from_s3_to_redshift(self, s3_path: str, table_name: str, copy_options: str = ""):
        """Use Redshift COPY command to load data from S3"""
        try:
            copy_sql = f"""
            COPY {table_name}
            FROM '{s3_path}'
            ACCESS_KEY_ID '{self.aws_access_key_id}'
            SECRET_ACCESS_KEY '{self.aws_secret_access_key}'
            CSV
            IGNOREHEADER 1
            DATEFORMAT 'auto'
            TIMEFORMAT 'auto'
            {copy_options}
            """
            
            logger.info(f"Copying data from {s3_path} to {table_name}")
            self.execute_query(copy_sql)
            self.connection.commit()
            logger.info(f"Successfully copied data to {table_name}")

        except Exception as e:
            logger.error(f"Error copying data from S3 to Redshift: {e}")
            self.connection.rollback()
            raise
    
    def upsert_genre_kpis(self, df: pd.DataFrame):
        """Upsert genre KPIs data"""
        try:
            logger.info("Upserting genre KPIs data")
            
            # Create temporary table
            temp_table_sql = """
            CREATE TEMP TABLE temp_genre_kpis (
                genre VARCHAR(255),
                total_streams BIGINT,
                unique_users BIGINT,
                avg_stream_duration DECIMAL(10,2),
                date_processed TIMESTAMP
            );
            """
            
            self.execute_query(temp_table_sql)
            
            # Insert data into temp table using VALUES
            for _, row in df.iterrows():
                insert_sql = """
                INSERT INTO temp_genre_kpis (genre, total_streams, unique_users, avg_stream_duration, date_processed)
                VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                """
                self.execute_query(insert_sql, (
                    row['genre'],
                    int(row['total_streams']),
                    int(row['unique_users']),
                    float(row['avg_stream_duration'])
                ))
            
            # Perform upsert
            upsert_sql = """
            BEGIN TRANSACTION;
            
            DELETE FROM genre_kpis
            USING temp_genre_kpis
            WHERE genre_kpis.genre = temp_genre_kpis.genre
            AND genre_kpis.date_processed::date = temp_genre_kpis.date_processed::date;
            
            INSERT INTO genre_kpis (genre, total_streams, unique_users, avg_stream_duration, date_processed)
            SELECT genre, total_streams, unique_users, avg_stream_duration, date_processed
            FROM temp_genre_kpis;
            
            END TRANSACTION;
            """
            
            self.execute_query(upsert_sql)
            self.connection.commit()
            logger.info(f"Successfully upserted {len(df)} genre KPI records")
            
        except Exception as e:
            logger.error(f"Error upserting genre KPIs: {e}")
            self.connection.rollback()
            raise
    
    def upsert_hourly_kpis(self, df: pd.DataFrame):
        """Upsert hourly KPIs data"""
        try:
            logger.info("Upserting hourly KPIs data")

            # Create temporary table
            temp_table_sql = """
            CREATE TEMP TABLE temp_hourly_kpis (
                hour TIMESTAMP,
                total_streams BIGINT,
                unique_users BIGINT,
                unique_songs BIGINT,
                avg_stream_duration DECIMAL(10,2),
                date_processed TIMESTAMP
            );
            """
            
            self.execute_query(temp_table_sql)
            
            # Insert data into temp table
            for _, row in df.iterrows():
                insert_sql = """
                INSERT INTO temp_hourly_kpis (hour, total_streams, unique_users, unique_songs, avg_stream_duration, date_processed)
                VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                """
                self.execute_query(insert_sql, (
                    row['hour'],
                    int(row['total_streams']),
                    int(row['unique_users']),
                    int(row['unique_songs']),
                    float(row['avg_stream_duration'])
                ))
            
            # Perform upsert
            upsert_sql = """
            BEGIN TRANSACTION;
            
            DELETE FROM hourly_kpis
            USING temp_hourly_kpis
            WHERE hourly_kpis.hour = temp_hourly_kpis.hour
            AND hourly_kpis.date_processed::date = temp_hourly_kpis.date_processed::date;
            
            INSERT INTO hourly_kpis (hour, total_streams, unique_users, unique_songs, avg_stream_duration, date_processed)
            SELECT hour, total_streams, unique_users, unique_songs, avg_stream_duration, date_processed
            FROM temp_hourly_kpis;
            
            END TRANSACTION;
            """
            
            self.execute_query(upsert_sql)
            self.connection.commit()
            logger.info(f"Successfully upserted {len(df)} hourly KPI records")
            
        except Exception as e:
            logger.error(f"Error upserting hourly KPIs: {e}")
            self.connection.rollback()
            raise
    
    def validate_data_quality(self):
        """Validate data quality in Redshift tables"""
        try:
            logger.info("Validating data quality in Redshift")

            # Check record counts
            genre_count_query = "SELECT COUNT(*) FROM genre_kpis WHERE date_processed::date = CURRENT_DATE"
            hourly_count_query = "SELECT COUNT(*) FROM hourly_kpis WHERE date_processed::date = CURRENT_DATE"
            
            genre_count = self.execute_query(genre_count_query)[0][0]
            hourly_count = self.execute_query(hourly_count_query)[0][0]

            logger.info(f"Data quality check - Genre KPIs: {genre_count} records, Hourly KPIs: {hourly_count} records")

            # Check for null values in critical columns
            null_check_queries = [
                "SELECT COUNT(*) FROM genre_kpis WHERE genre IS NULL OR total_streams IS NULL",
                "SELECT COUNT(*) FROM hourly_kpis WHERE hour IS NULL OR total_streams IS NULL"
            ]
            
            for query in null_check_queries:
                null_count = self.execute_query(query)[0][0]
                if null_count > 0:
                    logger.warning(f"Found {null_count} records with null values")
                else:
                    logger.info("No null values found in critical columns")
            
            return True
            
        except Exception as e:
            logger.error(f"Error during data quality validation: {e}")
            return False
    
    def move_processed_files(self):
        """Move processed files to processed directory in S3"""
        try:
            logger.info("Moving processed files to processed directory")
            
            files_to_move = [
                f"{self.staging_prefix}genre_kpis.csv",
                f"{self.staging_prefix}hourly_kpis.csv"
            ]
            
            for file_key in files_to_move:
                try:
                    # Check if file exists
                    self.s3_client.head_object(Bucket=self.bucket_name, Key=file_key)
                    
                    # Copy to processed directory
                    processed_key = file_key.replace(self.staging_prefix, self.processed_prefix)
                    copy_source = {'Bucket': self.bucket_name, 'Key': file_key}
                    
                    self.s3_client.copy_object(
                        CopySource=copy_source,
                        Bucket=self.bucket_name,
                        Key=processed_key
                    )
                    
                    # Delete original file
                    self.s3_client.delete_object(Bucket=self.bucket_name, Key=file_key)
                    
                    logger.info(f"Moved {file_key} to {processed_key}")
                    
                except ClientError as e:
                    if e.response['Error']['Code'] == '404':
                        logger.warning(f"File not found: {file_key}")
                    else:
                        logger.error(f"Error moving file {file_key}: {e}")
                        
        except Exception as e:
            logger.error(f"Error moving processed files: {e}")
            # Don't raise exception as this is not critical

def upsert_to_redshift():
    """
    Load genre_kpis.csv and hourly_kpis.csv from S3 staging area into Redshift tables
    """
    loader = RedshiftLoader()
    
    try:
        # Connect to Redshift
        loader.connect_to_redshift()
        
        # Create tables if they don't exist
        loader.create_tables_if_not_exist()
        
        # Define file paths
        genre_kpis_key = f"{loader.staging_prefix}genre_kpis.csv"
        hourly_kpis_key = f"{loader.staging_prefix}hourly_kpis.csv"
        
        logger.info("Starting data load to Redshift")
        
        # Load and upsert genre KPIs
        try:
            genre_df = loader.read_csv_from_s3(genre_kpis_key)
            if not genre_df.empty:
                loader.upsert_genre_kpis(genre_df)
                logger.info("Genre KPIs loaded successfully")
            else:
                logger.warning("Genre KPIs file is empty")
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                logger.warning("Genre KPIs file not found in S3")
            else:
                raise
        
        # Load and upsert hourly KPIs
        try:
            hourly_df = loader.read_csv_from_s3(hourly_kpis_key)
            if not hourly_df.empty:
                loader.upsert_hourly_kpis(hourly_df)
                logger.info("Hourly KPIs loaded successfully")
            else:
                logger.warning("Hourly KPIs file is empty")
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                logger.warning("Hourly KPIs file not found in S3")
            else:
                raise
        
        # Move processed files
        loader.move_processed_files()

        logger.info("UPSERT to Redshift completed successfully")
        
    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        raise
    except ClientError as e:
        logger.error(f"AWS S3 error: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during Redshift load: {e}")
        raise
    finally:
        # Always disconnect
        loader.disconnect_from_redshift()

if __name__ == "__main__":
    # For testing purposes
    upsert_to_redshift()