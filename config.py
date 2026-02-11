"""Configuration management for the ETL pipeline."""

import os
from dataclasses import dataclass
from typing import Optional

from exceptions import ConfigurationError


@dataclass
class Config:
    """Configuration class for ETL pipeline settings."""

    # AWS Configuration
    aws_region: str
    source_bucket: str
    source_key: str
    destination_bucket: str
    destination_key: str

    # AWS Credentials (optional, can use IAM roles)
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None

    # Logging Configuration
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "Config":
        """
        Create a Config instance from environment variables.

        Returns:
            Config instance with values from environment.

        Raises:
            ConfigurationError: If required environment variables are missing.
        """
        required_vars = [
            "AWS_REGION",
            "SOURCE_BUCKET",
            "SOURCE_KEY",
            "DESTINATION_BUCKET",
            "DESTINATION_KEY",
        ]

        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise ConfigurationError(
                f"Missing required environment variables: {', '.join(missing_vars)}"
            )

        return cls(
            aws_region=os.getenv("AWS_REGION", ""),
            source_bucket=os.getenv("SOURCE_BUCKET", ""),
            source_key=os.getenv("SOURCE_KEY", ""),
            destination_bucket=os.getenv("DESTINATION_BUCKET", ""),
            destination_key=os.getenv("DESTINATION_KEY", ""),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
        )

    def validate(self) -> None:
        """
        Validate configuration values.

        Raises:
            ConfigurationError: If configuration values are invalid.
        """
        if not self.aws_region:
            raise ConfigurationError("AWS region cannot be empty")

        if not self.source_bucket:
            raise ConfigurationError("Source bucket cannot be empty")

        if not self.source_key:
            raise ConfigurationError("Source key cannot be empty")

        if not self.destination_bucket:
            raise ConfigurationError("Destination bucket cannot be empty")

        if not self.destination_key:
            raise ConfigurationError("Destination key cannot be empty")

        valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if self.log_level.upper() not in valid_log_levels:
            raise ConfigurationError(
                f"Invalid log level: {self.log_level}. Must be one of {valid_log_levels}"
            )
