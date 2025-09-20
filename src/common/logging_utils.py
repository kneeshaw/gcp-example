"""Logging utility configuration for the Transit Data Pipeline.

This module provides a centralized logger configuration for the transit data pipeline.
The logger is configured to output to stdout with a consistent format across all modules.

Usage:
    from common.logging_utils import logger
    logger.info("Your message here")
"""

import logging
import sys

# Create a logger with a specific name for the transit data pipeline
logger = logging.getLogger("transit_data_pipeline")

# Configure the logger only if it doesn't already have handlers
# This prevents duplicate handlers when the module is imported multiple times
if not logger.hasHandlers():
    # Create a stream handler that outputs to stdout (required for AWS Lambda)
    handler = logging.StreamHandler(sys.stdout)
    
    # Create a formatter with timestamp, level, and message
    # Format: "2025-07-26 10:30:45 - INFO - Your log message here"
    '''formatter = logging.Formatter(
        fmt='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )'''

    formatter = logging.Formatter(
        fmt='%(levelname)s - %(message)s'
    )
    
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# Set the logging level to INFO
# This will capture INFO, WARNING, ERROR, and CRITICAL messages
logger.setLevel(logging.INFO)

# Allow log messages to propagate to parent loggers
# This is the default behavior and is generally recommended
logger.propagate = True
