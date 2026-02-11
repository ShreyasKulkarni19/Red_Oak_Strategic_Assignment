"""Custom exception classes for the ETL pipeline."""


class ETLError(Exception):
    """Base exception class for all ETL-related errors."""

    pass


class S3Error(ETLError):
    """Exception raised for S3-related errors."""

    pass


class ConfigurationError(ETLError):
    """Exception raised for configuration-related errors."""

    pass


class TransformationError(ETLError):
    """Exception raised for data transformation errors."""

    pass
