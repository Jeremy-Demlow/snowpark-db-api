"""Utilities and helper functions for Snowpark DB-API transfers.

This module provides logging setup, progress tracking, and other utility functions.
"""

import logging
import sys
from typing import Optional, Dict, Any
from datetime import datetime
from pathlib import Path
import json

def setup_logging(
    log_level: str = "INFO",
    log_file: Optional[str] = None,
    include_timestamp: bool = True
) -> logging.Logger:
    """Setup logging configuration.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional file to write logs to
        include_timestamp: Whether to include timestamp in log format
        
    Returns:
        Configured logger instance
    """
    # Create logger
    logger = logging.getLogger("snowpark_db_api")
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # Create formatter
    if include_timestamp:
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    else:
        formatter = logging.Formatter(
            '%(name)s - %(levelname)s - %(message)s'
        )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler if specified
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger

class ProgressTracker:
    """Track and report progress of data transfer operations."""
    
    def __init__(self, total_items: int, description: str = "Processing"):
        self.total_items = total_items
        self.current_items = 0
        self.description = description
        self.start_time = datetime.now()
        self.last_report_time = self.start_time
        self.logger = logging.getLogger(__name__)
        
    def update(self, items_processed: int = 1):
        """Update progress counter.
        
        Args:
            items_processed: Number of items processed in this update
        """
        self.current_items += items_processed
        
        # Report progress every 1000 items or at completion
        if (self.current_items % 1000 == 0 
            or self.current_items == self.total_items
            or (datetime.now() - self.last_report_time).seconds >= 30):
            
            self._report_progress()
            self.last_report_time = datetime.now()
    
    def _report_progress(self):
        """Report current progress."""
        if self.total_items > 0:
            percentage = (self.current_items / self.total_items) * 100
            elapsed = datetime.now() - self.start_time
            
            if self.current_items > 0:
                rate = self.current_items / elapsed.total_seconds()
                remaining_items = self.total_items - self.current_items
                eta_seconds = remaining_items / rate if rate > 0 else 0
                eta_str = f" (ETA: {eta_seconds:.0f}s)" if eta_seconds > 0 else ""
            else:
                rate = 0
                eta_str = ""
            
            self.logger.info(
                f"{self.description}: {self.current_items:,}/{self.total_items:,} "
                f"({percentage:.1f}%) - {rate:.1f} items/sec{eta_str}"
            )
        else:
            self.logger.info(f"{self.description}: {self.current_items:,} items processed")
    
    def complete(self):
        """Mark progress as complete and report final statistics."""
        elapsed = datetime.now() - self.start_time
        rate = self.current_items / elapsed.total_seconds() if elapsed.total_seconds() > 0 else 0
        
        self.logger.info(
            f"{self.description} completed: {self.current_items:,} items "
            f"in {elapsed.total_seconds():.1f}s (avg {rate:.1f} items/sec)"
        )

def validate_table_name(table_name: str) -> str:
    """Validate and sanitize table name.
    
    Args:
        table_name: Table name to validate
        
    Returns:
        Sanitized table name
        
    Raises:
        ValueError: If table name is invalid
    """
    if not table_name:
        raise ValueError("Table name cannot be empty")
    
    # Remove dangerous characters
    sanitized = ''.join(c for c in table_name if c.isalnum() or c in ['_', '.'])
    
    if not sanitized:
        raise ValueError(f"Table name '{table_name}' contains no valid characters")
    
    # Ensure it doesn't start with a number
    if sanitized[0].isdigit():
        sanitized = f"T_{sanitized}"
    
    return sanitized

def format_bytes(bytes_count: int) -> str:
    """Format byte count in human readable format.
    
    Args:
        bytes_count: Number of bytes
        
    Returns:
        Formatted string (e.g., "1.5 MB")
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_count < 1024.0:
            return f"{bytes_count:.1f} {unit}"
        bytes_count /= 1024.0
    return f"{bytes_count:.1f} PB"

def format_duration(seconds: float) -> str:
    """Format duration in human readable format.
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        Formatted string (e.g., "2h 30m 15s")
    """
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    
    parts = []
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    if secs > 0 or not parts:
        parts.append(f"{secs}s")
    
    return " ".join(parts)

def save_transfer_metadata(
    source_info: Dict[str, Any],
    destination_info: Dict[str, Any],
    transfer_stats: Dict[str, Any],
    file_path: str
):
    """Save transfer metadata to JSON file.
    
    Args:
        source_info: Information about source database/table
        destination_info: Information about destination table
        transfer_stats: Transfer statistics
        file_path: Path to save metadata file
    """
    metadata = {
        "timestamp": datetime.now().isoformat(),
        "source": source_info,
        "destination": destination_info,
        "transfer_stats": transfer_stats,
        "version": "1.0"
    }
    
    metadata_path = Path(file_path)
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2, default=str)

def print_banner(title: str, width: int = 80):
    """Print a formatted banner.
    
    Args:
        title: Banner title
        width: Total width of banner
    """
    print("\n" + "=" * width)
    print(f"{title.center(width)}")
    print("=" * width)

def print_summary(
    source_db: str,
    source_table: str,
    destination_table: str,
    rows_transferred: int,
    duration: float,
    errors: int = 0
):
    """Print transfer summary.
    
    Args:
        source_db: Source database info
        source_table: Source table name
        destination_table: Destination table name
        rows_transferred: Number of rows transferred
        duration: Duration in seconds
        errors: Number of errors encountered
    """
    print_banner("TRANSFER SUMMARY")
    print(f"Source: {source_db}.{source_table}")
    print(f"Destination: {destination_table}")
    print(f"Rows Transferred: {rows_transferred:,}")
    print(f"Duration: {format_duration(duration)}")
    if duration > 0:
        rate = rows_transferred / duration
        print(f"Transfer Rate: {rate:.1f} rows/sec")
    if errors > 0:
        print(f"Errors: {errors}")
    print("=" * 80) 