"""S3 client wrapper for interacting with AWS S3."""

import logging
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import ClientError

from exceptions import S3Error


class S3Client:
    """Wrapper class for AWS S3 operations using boto3."""

    def __init__(
        self,
        region: str,
        access_key_id: Optional[str] = None,
        secret_access_key: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """
        Initialize S3Client with AWS credentials and configuration.

        Args:
            region: AWS region name.
            access_key_id: AWS access key ID (optional, can use IAM roles).
            secret_access_key: AWS secret access key (optional, can use IAM roles).
            logger: Logger instance for logging operations.
        """
        self.region = region
        self.logger = logger or logging.getLogger(__name__)

        # Create boto3 S3 client with optional credentials
        client_config: Dict[str, Any] = {"region_name": region}
        if access_key_id and secret_access_key:
            client_config["aws_access_key_id"] = access_key_id
            client_config["aws_secret_access_key"] = secret_access_key

        try:
            self.client = boto3.client("s3", **client_config)
            self.logger.info(f"S3Client initialized for region: {region}")
        except Exception as e:
            raise S3Error(f"Failed to initialize S3 client: {str(e)}")

    def read_object(self, bucket: str, key: str) -> bytes:
        """
        Read an object from S3.

        Args:
            bucket: S3 bucket name.
            key: S3 object key.

        Returns:
            Object content as bytes.

        Raises:
            S3Error: If the object cannot be read.
        """
        try:
            self.logger.info(f"Reading object from s3://{bucket}/{key}")
            response = self.client.get_object(Bucket=bucket, Key=key)
            content = response["Body"].read()
            self.logger.info(f"Successfully read {len(content)} bytes from S3")
            return content
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            raise S3Error(
                f"Failed to read object from s3://{bucket}/{key}: {error_code} - {str(e)}"
            )
        except Exception as e:
            raise S3Error(f"Unexpected error reading from S3: {str(e)}")

    def write_object(self, bucket: str, key: str, content: bytes) -> None:
        """
        Write an object to S3.

        Args:
            bucket: S3 bucket name.
            key: S3 object key.
            content: Object content as bytes.

        Raises:
            S3Error: If the object cannot be written.
        """
        try:
            self.logger.info(f"Writing object to s3://{bucket}/{key}")
            self.client.put_object(Bucket=bucket, Key=key, Body=content)
            self.logger.info(f"Successfully wrote {len(content)} bytes to S3")
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            raise S3Error(
                f"Failed to write object to s3://{bucket}/{key}: {error_code} - {str(e)}"
            )
        except Exception as e:
            raise S3Error(f"Unexpected error writing to S3: {str(e)}")

    def object_exists(self, bucket: str, key: str) -> bool:
        """
        Check if an object exists in S3.

        Args:
            bucket: S3 bucket name.
            key: S3 object key.

        Returns:
            True if the object exists, False otherwise.
        """
        try:
            self.client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "404":
                return False
            raise S3Error(f"Error checking object existence: {str(e)}")
