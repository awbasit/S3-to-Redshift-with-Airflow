import pandas as pd
import boto3
import logging
from airflow.models import Variable
from botocore.exceptions import ClientError
from etl.s3_logger import S3Logger
import io
from datetime import datetime, timedelta
from typing import List, Optional

# Configure logging
bucket = Variable.get("S3_BUCKET_NAME")
log_key = f"logs/stream_data/streaming_etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
s3_logger = S3Logger(bucket, log_key)
logger = s3_logger.get_logger()

class S3StreamDataExtractor:
    """Class to handle streaming data extraction from S3"""
    
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.bucket_name = Variable.get("S3_BUCKET_NAME")
        self.streams_prefix = Variable.get("S3_STREAMS_PREFIX", "data/streams/")
        self.staging_prefix = Variable.get("S3_STAGING_PREFIX", "data/staging/")
        
    def list_stream_files(self, max_files: int = 100) -> List[str]:
        """List streaming files in S3 bucket"""
        try:
            logger.info(f"Listing streaming files in s3://{self.bucket_name}/{self.streams_prefix}")
            
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=self.streams_prefix,
                MaxKeys=max_files
            )
            
            if 'Contents' not in response:
                logger.warning("No streaming files found in S3")
                return []
            
            # Filter for CSV files and sort by last modified
            files = [
                obj['Key'] for obj in response['Contents']
                if obj['Key'].endswith('.csv') and obj['Size'] > 0
            ]
            
            # Sort by key name
            files.sort()

            logger.info(f"Found {len(files)} streaming files")
            return files
            
        except ClientError as e:
            logger.error(f"Error listing files from S3: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error listing files: {e}")
            raise
        finally:
            s3_logger.push_to_s3()
    
    def read_csv_from_s3(self, key: str) -> pd.DataFrame:
        """Read CSV file from S3 and return as DataFrame"""
        try:
            logger.info(f"Reading streaming file: s3://{self.bucket_name}/{key}")
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
        finally:
            s3_logger.push_to_s3()
    
    def write_csv_to_s3(self, df: pd.DataFrame, key: str) -> None:
        """Write DataFrame to S3 as CSV"""
        try:
            logger.info(f"Writing staged file: s3://{self.bucket_name}/{key}")
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
            logger.error(f"Unexpected error writing CSV: {e}")
            raise
        finally:
            s3_logger.push_to_s3()
    
    def validate_streaming_data(self, df: pd.DataFrame) -> bool:
        """Validate streaming data structure and content"""
        if df.empty:
            logger.error("Streaming DataFrame is empty")
            return False
        
        # Define required columns for streaming data
        required_columns = ['user_id', 'track_id', 'listen_time']  # Adjust based on your schema
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logger.error(f"Missing required columns in streaming data: {missing_columns}")
            return False
        
        # Check for null values in critical columns
        critical_nulls = df[required_columns].isnull().sum()
        if critical_nulls.any():
            logger.warning(f"Found null values in critical columns: {critical_nulls[critical_nulls > 0].to_dict()}")

        logger.info("Streaming data validation passed")
        return True

    def get_latest_files(self, files: List[str], hours_back: int = 1) -> List[str]:
        """Get files from the last N hours based on S3 object modification time"""
        try:
            cutoff_time = datetime.now() - timedelta(hours=hours_back)
            recent_files = []
            
            for file_key in files:
                try:
                    response = self.s3_client.head_object(Bucket=self.bucket_name, Key=file_key)
                    last_modified = response['LastModified'].replace(tzinfo=None)
                    
                    if last_modified >= cutoff_time:
                        recent_files.append(file_key)
                        
                except ClientError as e:
                    logger.warning(f"Could not get metadata for {file_key}: {e}")
                    continue

            logger.info(f"Found {len(recent_files)} files from the last {hours_back} hours")
            return recent_files
            
        except Exception as e:
            logger.error(f"Error filtering files by time: {e}")
            # Return all files as fallback
            return files
        finally:
            s3_logger.push_to_s3()

def extract_streaming_batch():
    """
    Extract the latest streaming data from CSV files in S3 and stage for processing.
    """
    extractor = S3StreamDataExtractor()
    try:
        logger.info("Starting streaming data extraction from S3")

        # List all streaming files
        all_files = extractor.list_stream_files()
        
        if not all_files:
            logger.warning("No streaming files found in S3")
            return
        
        # Get recent files (last 2 hours to ensure we don't miss any)
        recent_files = extractor.get_latest_files(all_files, hours_back=2)
        
        if not recent_files:
            logger.info("No recent streaming files found, using latest available file")
            recent_files = [all_files[-1]]  # Use the most recent file
        
        # Process files
        all_streaming_data = []
        processed_files = []
        
        for file_key in recent_files:
            try:
                df = extractor.read_csv_from_s3(file_key)
                
                # Validate data
                if not extractor.validate_streaming_data(df):
                    logger.warning(f"Skipping invalid file: {file_key}")
                    continue
                
                # Add file source for tracking
                df['source_file'] = file_key
                all_streaming_data.append(df)
                processed_files.append(file_key)
                
            except Exception as e:
                logger.error(f"Error processing file {file_key}: {e}")
                continue
        
        if not all_streaming_data:
            raise ValueError("No valid streaming data found in any files")
        
        # Combine all data
        combined_df = pd.concat(all_streaming_data, ignore_index=True)
        
        # Clean and deduplicate data
        logger.info(f"Combined data shape before cleaning: {combined_df.shape}")
        
        # Remove duplicates based on user_id, song_id, and timestamp
        combined_df = combined_df.drop_duplicates(subset=['user_id', 'track_id', 'listen_time'])

        # Sort by user_id
        if 'user_id' in combined_df.columns:
            combined_df = combined_df.sort_values('user_id')

        logger.info(f"Final cleaned data shape: {combined_df.shape}")

        # Stage the combined data
        staging_key = f"{extractor.staging_prefix}streaming_data_staged.csv"
        extractor.write_csv_to_s3(combined_df, staging_key)

        logger.info(f"Streaming data extraction completed successfully")
        logger.info(f"Processed files: {processed_files}")
        logger.info(f"Total records staged: {len(combined_df)}")

    except ValueError as e:
        logger.error(f"Data validation error: {e}")
        raise
    except ClientError as e:
        logger.error(f"AWS S3 error: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during streaming data extraction: {e}")
        raise
    finally:
        s3_logger.push_to_s3()

if __name__ == "__main__":
    # For testing purposes
    extract_streaming_batch()