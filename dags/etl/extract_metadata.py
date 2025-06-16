import pandas as pd
import boto3
from airflow.models import Variable
from botocore.exceptions import ClientError
from etl.s3_logger import S3Logger
import io
import os
from typing import Optional
from datetime import datetime, timedelta

# Configure logging
bucket = Variable.get("S3_BUCKET_NAME")
log_key = f"logs/metadata/metadata_etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
s3_logger = S3Logger(bucket, log_key)
logger = s3_logger.get_logger()

class S3MetadataExtractor:
    """Class to handle metadata extraction from S3"""
    
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.bucket_name = Variable.get("S3_BUCKET_NAME")
        self.metadata_prefix = Variable.get("S3_METADATA_PREFIX", "data/metadata/")
        self.staging_prefix = Variable.get("S3_STAGING_PREFIX", "data/staging/")
        
    def read_csv_from_s3(self, key: str) -> pd.DataFrame:
        """Read CSV file from S3 and return as DataFrame"""
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
            logger.error(f"Unexpected error reading CSV from S3: {e}")
            raise
        finally:
            s3_logger.push_to_s3()

    def write_csv_to_s3(self, df: pd.DataFrame, key: str) -> None:
        """Write DataFrame to S3 as CSV"""
        try:
            logger.info(f"Writing file to S3: s3://{self.bucket_name}/{key}")
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=csv_buffer.getvalue(),
                ContentType='text/csv'
            )
            logger.info(f"Successfully wrote {len(df)} rows to {key}")
        except ClientError as e:
            logger.error(f"Error writing file to S3: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error writing CSV to S3: {e}")
            raise
        finally:
            s3_logger.push_to_s3()
    
    def validate_dataframe(self, df: pd.DataFrame, file_type: str) -> bool:
        """Validate DataFrame structure and content"""
        if df.empty:
            logger.error(f"{file_type} DataFrame is empty")
            return False
        
        if file_type == "users":
            required_columns = ['user_id', 'user_name']  # Adjust based on your schema
        elif file_type == "songs":
            required_columns = ['track_id', 'track_name', 'artists']  # Adjust based on your schema
        else:
            return True
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"Missing required columns in {file_type}: {missing_columns}")
            return False
        
        logger.info(f"{file_type} DataFrame validation passed")
        return True

def extract_user_and_song_data():
    """
    Extract user and song metadata from S3 CSV files and stage them for further processing.
    """
    extractor = S3MetadataExtractor()
    
    try:
        # Define file paths
        user_metadata_key = f"{extractor.metadata_prefix}users.csv"
        song_metadata_key = f"{extractor.metadata_prefix}songs.csv"
        
        # Staging file paths
        user_staging_key = f"{extractor.staging_prefix}user_metadata_staged.csv"
        song_staging_key = f"{extractor.staging_prefix}song_metadata_staged.csv"
        
        logger.info("Starting metadata extraction from S3")
        
        # Read user metadata
        logger.info("Extracting user metadata")
        user_df = extractor.read_csv_from_s3(user_metadata_key)
        
        # Validate user data
        if not extractor.validate_dataframe(user_df, "users"):
            raise ValueError("User metadata validation failed")
        
        # Read song metadata
        logger.info("Extracting song metadata")
        song_df = extractor.read_csv_from_s3(song_metadata_key)
        
        # Validate song data
        if not extractor.validate_dataframe(song_df, "songs"):
            raise ValueError("Song metadata validation failed")
        
        # Clean data (remove duplicates, handle nulls)
        user_df = user_df.drop_duplicates().dropna(subset=['user_id'])
        song_df = song_df.drop_duplicates().dropna(subset=['track_id'])
        
        logger.info(f"Cleaned user data: {len(user_df)} rows")
        logger.info(f"Cleaned song data: {len(song_df)} rows")
        
        # Stage the data in S3
        logger.info("Staging user metadata to S3")
        extractor.write_csv_to_s3(user_df, user_staging_key)
        
        logger.info("Staging song metadata to S3")
        extractor.write_csv_to_s3(song_df, song_staging_key)
        
        logger.info("Metadata extraction and staging completed successfully")
        
        # Log summary statistics
        logger.info(f"Summary - Users: {len(user_df)} rows, Songs: {len(song_df)} rows")
        
    except FileNotFoundError as e:
        logger.error(f"Required metadata files not found in S3: {e}")
        raise
    except ValueError as e:
        logger.error(f"Data validation error: {e}")
        raise
    except ClientError as e:
        logger.error(f"AWS S3 error: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during metadata extraction: {e}")
        raise
    finally:
        s3_logger.push_to_s3()

if __name__ == "__main__":
    # For testing purposes
    extract_user_and_song_data()