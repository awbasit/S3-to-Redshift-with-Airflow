import logging
import boto3
import io
from datetime import datetime


class S3Logger:
    def __init__(self, bucket_name: str, s3_key: str):
        self.log_buffer = io.StringIO()
        self.bucket_name = bucket_name
        self.s3_key = s3_key

        self.logger = logging.getLogger(f"s3_logger_{datetime.now().strftime('%Y%m%d%H%M%S')}")
        self.logger.setLevel(logging.INFO)

        # Clear existing handlers to avoid duplicates
        if self.logger.hasHandlers():
            self.logger.handlers.clear()

        handler = logging.StreamHandler(self.log_buffer)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def get_logger(self):
        return self.logger

    def push_to_s3(self):
        self.log_buffer.seek(0)
        s3_client = boto3.client("s3")
        s3_client.put_object(
            Bucket=self.bucket_name,
            Key=self.s3_key,
            Body=self.log_buffer.read().encode('utf-8'),
            ContentType='text/plain'
        )
