"""Snowpark DB-API: Professional data transfer tool for Snowflake.

This package provides a scalable, professional solution for transferring data
from various databases to Snowflake using the new Snowpark DB-API.
"""

from .core import DataTransfer, transfer_data, create_snowflake_session
from .config import config_manager, DatabaseType, AppConfig
from .connections import get_connection_factory, test_connection
from .utils import setup_logging, ProgressTracker

__version__ = "1.0.0"
__author__ = "Your Organization"

__all__ = [
    "DataTransfer",
    "transfer_data", 
    "create_snowflake_session",
    "config_manager",
    "DatabaseType",
    "AppConfig",
    "get_connection_factory",
    "test_connection",
    "setup_logging",
    "ProgressTracker"
]
