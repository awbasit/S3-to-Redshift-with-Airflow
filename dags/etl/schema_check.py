import pandas as pd
import boto3
from airflow.models import Variable
from botocore.exceptions import ClientError
import io
from typing import Dict, List, Optional, Any
import json
from datetime import datetime
from etl.s3_logger import S3Logger

# Configure logging
bucket = Variable.get("S3_BUCKET_NAME")
log_key = f"logs/schema/validate_etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
s3_logger = S3Logger(bucket, log_key)
logger = s3_logger.get_logger()
# Ensure the logger is flushed to S3 at the end of the script

class S3DataValidator:
    """Class to handle data validation from S3 staged files"""
    
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.bucket_name = Variable.get("S3_BUCKET_NAME")
        self.staging_prefix = Variable.get("S3_STAGING_PREFIX", "data/staging/")
        
        # Define expected schemas
        self.schemas = {
            'user_metadata': {
                'required_columns': ['user_id', 'user_name'],
                'optional_columns': ['user_age', 'user_country', 'created_at'],
                'data_types': {
                    'user_id': ['int64', 'object'],
                    'user_name': ['object']
                }
            },
            'song_metadata': {
                'required_columns': ['track_id', 'track_name', 'artists'],
                'data_types': {
                    'track_id': ['int64', 'object'],
                    'track_name': ['object'],
                    'artists': ['object']
                }
            },
            'streaming_data': {
                'required_columns': ['user_id', 'track_id', 'listen_time'],
                'data_types': {
                    'user_id': ['int64', 'object'],
                    'track_id': ['int64', 'object'],
                    'listen_time': ['int64', 'float64']
                }
            }
        }
    
    def read_csv_from_s3(self, key: str) -> Optional[pd.DataFrame]:
        """Read CSV file from S3 and return as DataFrame"""
        try:
            logger.info(f"Reading file from S3: s3://{self.bucket_name}/{key}")
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            df = pd.read_csv(io.BytesIO(response['Body'].read()))
            logger.info(f"Successfully read {len(df)} rows from {key}")
            return df
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                logger.warning(f"File not found: s3://{self.bucket_name}/{key}")
                return None
            else:
                logger.error(f"Error reading file from S3: {e}")
                raise
        except Exception as e:
            logger.error(f"Unexpected error reading CSV: {e}")
            raise
        finally:
            s3_logger.push_to_s3()
    
    def validate_schema(self, df: pd.DataFrame, dataset_type: str) -> Dict[str, Any]:
        """Validate DataFrame against expected schema"""
        validation_results = {
            'dataset_type': dataset_type,
            'row_count': len(df),
            'column_count': len(df.columns),
            'columns_present': list(df.columns),
            'validation_passed': True,
            'errors': [],
            'warnings': []
        }
        
        if dataset_type not in self.schemas:
            validation_results['errors'].append(f"Unknown dataset type: {dataset_type}")
            validation_results['validation_passed'] = False
            return validation_results
        
        schema = self.schemas[dataset_type]
        
        # Check required columns
        missing_columns = [col for col in schema['required_columns'] if col not in df.columns]
        if missing_columns:
            validation_results['errors'].append(f"Missing required columns: {missing_columns}")
            validation_results['validation_passed'] = False
        
        # Check data types
        for column, expected_types in schema['data_types'].items():
            if column in df.columns:
                actual_type = str(df[column].dtype)
                if actual_type not in expected_types:
                    validation_results['warnings'].append(
                        f"Column '{column}' has type '{actual_type}', expected one of: {expected_types}"
                    )
        
        # Check for empty DataFrame
        if df.empty:
            validation_results['errors'].append("DataFrame is empty")
            validation_results['validation_passed'] = False
        
        # Check for duplicate rows
        duplicate_count = df.duplicated().sum()
        if duplicate_count > 0:
            validation_results['warnings'].append(f"Found {duplicate_count} duplicate rows")
        
        # Dataset-specific validations
        if dataset_type == 'user_metadata':
            validation_results.update(self._validate_user_metadata(df))
        elif dataset_type == 'song_metadata':
            validation_results.update(self._validate_song_metadata(df))
        elif dataset_type == 'streaming_data':
            validation_results.update(self._validate_streaming_data(df))
        
        return validation_results
    
    def _validate_user_metadata(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Specific validation for user metadata"""
        results = {'specific_errors': [], 'specific_warnings': []}
        
        # Check for null user_ids
        if 'user_id' in df.columns:
            null_user_ids = df['user_id'].isnull().sum()
            if null_user_ids > 0:
                results['specific_errors'].append(f"Found {null_user_ids} null user_ids")
        
        # Check for duplicate user_ids
        if 'user_id' in df.columns:
            duplicate_user_ids = df['user_id'].duplicated().sum()
            if duplicate_user_ids > 0:
                results['specific_warnings'].append(f"Found {duplicate_user_ids} duplicate user_ids")
        # Check for missing user names
        if 'user_name' in df.columns:
            null_user_names = df['user_name'].isnull().sum()
            if null_user_names > 0:
                results['specific_warnings'].append(f"Found {null_user_names} null user_name values")
        
        return results
    
    def _validate_song_metadata(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Specific validation for song metadata"""
        results = {'specific_errors': [], 'specific_warnings': []}
        
        # Check for null track_ids
        if 'track_id' in df.columns:
            null_track_ids = df['track_id'].isnull().sum()
            if null_track_ids > 0:
                results['specific_errors'].append(f"Found {null_track_ids} null track_ids")

        # Check for duplicate track_ids
        if 'track_id' in df.columns:
            duplicate_track_ids = df['track_id'].duplicated().sum()
            if duplicate_track_ids > 0:
                results['specific_warnings'].append(f"Found {duplicate_track_ids} duplicate track_ids")

        # Check for missing track names or artists
        for col in ['track_name', 'artists']:
            if col in df.columns:
                null_count = df[col].isnull().sum()
                if null_count > 0:
                    results['specific_warnings'].append(f"Found {null_count} null {col} values")
        
        # Validate genre values
        if 'track_genre' in df.columns:
            valid_genres = ['rock', 'pop', 'jazz', 'classical', 'hip-hop', 'electronic', 'country', 'r&b', 'folk', 'blues', 'accoustic', 'metal', 'reggae', 'latin', 'world']
            df['track_genre'] = df['track_genre'].str.lower()
            invalid_genres = df[~df['track_genre'].str.lower().isin(valid_genres)]['track_genre'].unique()
            if len(invalid_genres) > 0:
                results['specific_warnings'].append(f"Found potentially invalid genres: {list(invalid_genres)[:10]}")
        return results

    def _validate_streaming_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Specific validation for streaming data"""
        results = {'specific_errors': [], 'specific_warnings': []}
        
        # Check for null values in critical columns
        critical_columns = ['user_id', 'track_id', 'listen_time']
        for col in critical_columns:
            if col in df.columns:
                null_count = df[col].isnull().sum()
                if null_count > 0:
                    results['specific_errors'].append(f"Found {null_count} null {col} values")
        
        # Check for duplicate streaming records
        if 'user_id' in df.columns and 'track_id' in df.columns and 'listen_time' in df.columns:
            duplicate_streams = df.duplicated(subset=['user_id', 'track_id', 'listen_time']).sum()
            if duplicate_streams > 0:
                results['specific_warnings'].append(f"Found {duplicate_streams} duplicate streaming records")

        # Check for negative listen times
        if 'listen_time' in df.columns:
            negative_listen_times = (df['listen_time'] < 0).sum()
            if negative_listen_times > 0:
                results['specific_warnings'].append(f"Found {negative_listen_times} negative listen_time values")

        # Check for unrealistic durations (e.g., > 30 minutes)
        if 'duration_ms' in df.columns:
            long_streams = (df['duration_ms'] > 1800000).sum()  # 30 minutes in milliseconds
            if long_streams > 0:
                results['specific_warnings'].append(f"Found {long_streams} streams longer than 30 minutes")
        
        return results
    
    def save_validation_report(self, validation_results: List[Dict[str, Any]]):
        """Save validation report to S3"""
        try:
            report = {
                'validation_timestamp': datetime.now().isoformat(),
                'results': validation_results,
                'summary': {
                    'total_datasets_validated': len(validation_results),
                    'datasets_passed': sum(1 for r in validation_results if r['validation_passed']),
                    'datasets_failed': sum(1 for r in validation_results if not r['validation_passed'])
                }
            }
            
            report_key = f"{self.staging_prefix}validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=report_key,
                Body=json.dumps(report, indent=2),
                ContentType='application/json'
            )
            
            logger.info(f"Validation report saved to s3://{self.bucket_name}/{report_key}")
            
        except Exception as e:
            logger.error(f"Error saving validation report: {e}")
        finally:
            s3_logger.push_to_s3()

def validate_datasets():
    """
    Validate all staged datasets against their expected schemas
    """
    validator = S3DataValidator()
    
    try:
        logger.info("Starting data validation for staged datasets")

        # Define files to validate
        files_to_validate = [
            ('user_metadata_staged.csv', 'user_metadata'),
            ('song_metadata_staged.csv', 'song_metadata'),
            ('streaming_data_staged.csv', 'streaming_data')
        ]
        
        validation_results = []
        all_passed = True
        
        for filename, dataset_type in files_to_validate:
            file_key = f"{validator.staging_prefix}{filename}"

            logger.info(f"Validating {dataset_type} from {filename}")

            # Read the file
            df = validator.read_csv_from_s3(file_key)
            if df is None:
                logger.warning(f"Skipping validation for missing file: {filename}")
                continue

            # Validate schema
            result = validator.validate_schema(df, dataset_type)
            validation_results.append(result)
            
            # Log results
            if result['validation_passed']:
                logger.info(f"{dataset_type} validation PASSED - {result['row_count']} rows, {result['column_count']} columns")
                if result.get('warnings'):
                    for warning in result['warnings']:
                        logger.warning(f"  Warning: {warning}")
            else:
                logger.error(f"{dataset_type} validation FAILED")
                for error in result['errors']:
                    logger.error(f"  Error: {error}")
                    all_passed = False

            # Log specific validation results
            for key in ['specific_errors', 'specific_warnings']:
                if key in result:
                    for item in result[key]:
                        level = 'error' if 'error' in key else 'warning'
                        getattr(logger, level)(f"  {item}")
        
        # Save validation report
        validator.save_validation_report(validation_results)
        
        # Summary
        passed_count = sum(1 for r in validation_results if r['validation_passed'])
        total_count = len(validation_results)
        
        logger.info(f"Validation Summary: {passed_count}/{total_count} datasets passed validation")
        
        if not all_passed:
            raise ValueError("One or more datasets failed validation")

        logger.info("All datasets passed validation successfully")

    except Exception as e:
        logger.error(f"Data validation failed: {e}")
        raise
    finally:
        s3_logger.push_to_s3()

if __name__ == "__main__":
    # For testing purposes
    validate_datasets()