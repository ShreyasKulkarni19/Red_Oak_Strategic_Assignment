# Red Oak Strategic Assignment - AWS S3-to-S3 ETL Pipeline

A Python-based ETL (Extract, Transform, Load) pipeline for processing data between AWS S3 buckets using clean Object-Oriented Programming (OOP) design principles.

## Project Structure

```
.
├── main.py              # CLI entry point, orchestrates the ETL pipeline
├── config.py            # Environment configuration management
├── logger.py            # Logging setup and configuration
├── s3_client.py         # S3Client wrapper around boto3
├── transformer.py       # TripTransformer with data transformation logic
├── etl_processor.py     # ETLProcessor orchestrator class
├── exceptions.py        # Custom exception classes
├── requirements.txt     # Python dependencies
└── .env.example         # Environment variables template
```

## Features

- **Clean OOP Design**: Follows SOLID principles with clear separation of concerns
- **Type Hints**: Full typing support for better code quality and IDE assistance
- **Comprehensive Docstrings**: Well-documented code with detailed docstrings
- **Error Handling**: Custom exception hierarchy for better error management
- **Logging**: Configurable logging system for debugging and monitoring
- **Configuration Management**: Environment-based configuration with validation
- **Modular Architecture**: Easy to extend and maintain

## Components

### main.py
CLI entry point that:
- Loads configuration from environment variables
- Sets up logging
- Initializes dependencies (S3Client, TripTransformer)
- Creates and runs the ETL processor

### config.py
Configuration management with:
- Dataclass-based configuration
- Environment variable loading
- Configuration validation
- Type safety

### logger.py
Logging setup providing:
- Configurable log levels
- Structured logging format
- Console output handlers

### s3_client.py
S3 operations wrapper featuring:
- Read objects from S3
- Write objects to S3
- Check object existence
- Error handling and logging

### transformer.py
Data transformation with:
- Row-level transformation (placeholder)
- Batch transformation support
- Error handling

### etl_processor.py
Pipeline orchestrator handling:
- Extract: Read data from source S3 bucket
- Transform: Apply transformations to data
- Load: Write transformed data to destination S3 bucket

### exceptions.py
Custom exception hierarchy:
- ETLError (base)
- S3Error
- ConfigurationError
- TransformationError

## Installation

1. Install Python dependencies:
```bash
pip install -r requirements.txt
```

2. Copy the example environment file and configure it:
```bash
cp .env.example .env
# Edit .env with your AWS credentials and S3 bucket details
```

## Configuration

Set the following environment variables (or create a `.env` file):

```bash
# AWS Configuration
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your_access_key_id
AWS_SECRET_ACCESS_KEY=your_secret_access_key

# S3 Source Configuration
SOURCE_BUCKET=your-source-bucket
SOURCE_KEY=path/to/source/data.json

# S3 Destination Configuration
DESTINATION_BUCKET=your-destination-bucket
DESTINATION_KEY=path/to/destination/data.json

# Logging Configuration
LOG_LEVEL=INFO  # Options: DEBUG, INFO, WARNING, ERROR, CRITICAL
```

## Usage

### Running the ETL Pipeline

```bash
python main.py
```

### Using with dotenv

If you prefer using a `.env` file for configuration:

```python
from dotenv import load_dotenv
load_dotenv()  # Load .env file before running
```

## Development

### Design Principles

This project follows SOLID principles:

- **Single Responsibility**: Each class has a single, well-defined purpose
- **Open/Closed**: Classes are open for extension but closed for modification
- **Liskov Substitution**: Inheritance hierarchies maintain behavioral compatibility
- **Interface Segregation**: Focused interfaces without unnecessary dependencies
- **Dependency Inversion**: High-level modules depend on abstractions

### Extending the Pipeline

To add custom transformation logic:

1. Edit `transformer.py` and implement the `transform_row()` method
2. Add any additional transformation methods as needed
3. Update error handling in `exceptions.py` if required

### Adding New Features

The modular design makes it easy to:
- Add new data sources/destinations
- Implement different transformation strategies
- Extend error handling
- Add monitoring and metrics
- Implement data validation

## Requirements

- Python 3.8+
- boto3 >= 1.34.0
- python-dotenv >= 1.0.0 (optional, for .env file support)

## License

This project is licensed under the terms specified in the LICENSE file.