"""Main entry point for the ETL pipeline CLI."""

import logging
import sys
from typing import NoReturn

from config import Config
from etl_processor import ETLProcessor
from exceptions import ConfigurationError, ETLError
from logger import setup_logger
from s3_client import S3Client
from transformer import TripTransformer


def main() -> None:
    """
    Main function to execute the ETL pipeline.

    This function:
    1. Loads configuration from environment variables
    2. Sets up logging
    3. Initializes dependencies (S3Client, TripTransformer)
    4. Creates and runs the ETL processor

    Raises:
        SystemExit: If the pipeline fails with non-zero exit code.
    """
    logger: logging.Logger | None = None

    try:
        # Load configuration from environment
        config = Config.from_env()
        config.validate()

        # Setup logging
        log_level = getattr(logging, config.log_level.upper())
        logger = setup_logger(level=log_level)
        logger.info("Starting ETL pipeline")
        logger.info(f"Configuration loaded: {config.source_bucket}/{config.source_key} -> {config.destination_bucket}/{config.destination_key}")

        # Initialize S3 client
        s3_client = S3Client(
            region=config.aws_region,
            access_key_id=config.aws_access_key_id,
            secret_access_key=config.aws_secret_access_key,
            logger=logger,
        )

        # Initialize transformer
        transformer = TripTransformer(logger=logger)

        # Initialize and run ETL processor
        processor = ETLProcessor(
            config=config,
            s3_client=s3_client,
            transformer=transformer,
            logger=logger,
        )

        processor.run()

        logger.info("ETL pipeline completed successfully")
        sys.exit(0)

    except ConfigurationError as e:
        if logger:
            logger.error(f"Configuration error: {str(e)}")
        else:
            print(f"Configuration error: {str(e)}", file=sys.stderr)
        sys.exit(1)

    except ETLError as e:
        if logger:
            logger.error(f"ETL error: {str(e)}")
        else:
            print(f"ETL error: {str(e)}", file=sys.stderr)
        sys.exit(1)

    except Exception as e:
        if logger:
            logger.exception(f"Unexpected error: {str(e)}")
        else:
            print(f"Unexpected error: {str(e)}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
