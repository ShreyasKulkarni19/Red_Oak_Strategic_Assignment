"""Custom exception classes for the ETL pipeline."""


class ETLError(Exception):
    """Base exception class for all ETL-related errors."""


class S3Error(ETLError):
    """Exception raised for S3-related errors."""


class ConfigurationError(ETLError):
    """Exception raised for configuration-related errors."""


class TransformationError(ETLError):
    """Exception raised for data transformation errors."""
