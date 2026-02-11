"""ETL processor orchestrator for the pipeline."""

import json
import logging
from typing import Any, Dict, List, Optional

from config import Config
from exceptions import ETLError
from s3_client import S3Client
from transformer import TripTransformer


class ETLProcessor:
    """Main ETL processor that orchestrates the data pipeline."""

    def __init__(
        self,
        config: Config,
        s3_client: S3Client,
        transformer: TripTransformer,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """
        Initialize ETLProcessor with dependencies.

        Args:
            config: Configuration instance.
            s3_client: S3Client instance for S3 operations.
            transformer: TripTransformer instance for data transformation.
            logger: Logger instance for logging operations.
        """
        self.config = config
        self.s3_client = s3_client
        self.transformer = transformer
        self.logger = logger or logging.getLogger(__name__)

    def extract(self) -> List[Dict[str, Any]]:
        """
        Extract data from source S3 bucket.

        Returns:
            List of data records extracted from S3.

        Raises:
            ETLError: If extraction fails.
        """
        try:
            self.logger.info("Starting data extraction")
            content = self.s3_client.read_object(
                self.config.source_bucket, self.config.source_key
            )

            # Parse JSON content (assuming JSONL or JSON array format)
            data = json.loads(content.decode("utf-8"))

            # Handle both single object and array of objects
            if isinstance(data, dict):
                data = [data]
            elif not isinstance(data, list):
                raise ETLError(f"Unexpected data format: {type(data)}")

            self.logger.info(f"Extracted {len(data)} records")
            return data
        except json.JSONDecodeError as e:
            raise ETLError(f"Failed to parse JSON data: {str(e)}")
        except Exception as e:
            raise ETLError(f"Extraction failed: {str(e)}")

    def transform(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform extracted data.

        Args:
            data: List of data records to transform.

        Returns:
            List of transformed data records.

        Raises:
            ETLError: If transformation fails.
        """
        try:
            self.logger.info("Starting data transformation")
            transformed_data = self.transformer.transform_batch(data)
            self.logger.info(f"Transformed {len(transformed_data)} records")
            return transformed_data
        except Exception as e:
            raise ETLError(f"Transformation failed: {str(e)}")

    def load(self, data: List[Dict[str, Any]]) -> None:
        """
        Load transformed data to destination S3 bucket.

        Args:
            data: List of transformed data records to load.

        Raises:
            ETLError: If loading fails.
        """
        try:
            self.logger.info("Starting data loading")

            # Convert data to JSON format
            content = json.dumps(data, indent=2).encode("utf-8")

            # Write to destination S3 bucket
            self.s3_client.write_object(
                self.config.destination_bucket, self.config.destination_key, content
            )

            self.logger.info(f"Successfully loaded {len(data)} records to S3")
        except Exception as e:
            raise ETLError(f"Loading failed: {str(e)}")

    def run(self) -> None:
        """
        Execute the complete ETL pipeline: Extract -> Transform -> Load.

        Raises:
            ETLError: If any stage of the ETL process fails.
        """
        try:
            self.logger.info("Starting ETL pipeline")

            # Extract
            data = self.extract()

            # Transform
            transformed_data = self.transform(data)

            # Load
            self.load(transformed_data)

            self.logger.info("ETL pipeline completed successfully")
        except ETLError:
            raise
        except Exception as e:
            raise ETLError(f"ETL pipeline failed: {str(e)}")
