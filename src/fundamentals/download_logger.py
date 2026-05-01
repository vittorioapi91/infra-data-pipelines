"""
Generic Download Logger Utilities

This module provides reusable logging functions for download operations.
Can be used by any downloader (EDGAR, FRED, etc.) to log errors consistently.
"""

import logging
from typing import Optional


# Global logger instances cache (keyed by logger name)
_loggers: dict[str, logging.Logger] = {}


def setup_download_logger(
    logger_name: str,
    log_file: str = 'download_errors.log',
    log_level: int = logging.ERROR,
    add_console_handler: bool = False
) -> logging.Logger:
    """
    Set up a logger for download errors with full tracebacks.
    
    Args:
        logger_name: Unique name for the logger (e.g., 'edgar_downloader', 'fred_downloader')
        log_file: Path to the log file (default: 'download_errors.log')
        log_level: Logging level (default: logging.ERROR)
        add_console_handler: If True, also add a console handler for INFO+ messages
    
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)
    
    # Remove existing handlers to avoid duplicates
    logger.handlers = []
    
    # File handler for detailed error logs
    file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
    file_handler.setLevel(log_level)
    
    # Format with timestamp and full details
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(name)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Optionally add console handler
    if add_console_handler:
        import sys
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter('%(levelname)s - %(message)s')
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
    
    return logger


def get_download_logger(
    logger_name: str,
    log_file: Optional[str] = None,
    log_level: int = logging.ERROR,
    add_console_handler: bool = False
) -> logging.Logger:
    """
    Get or create a download logger instance.
    
    This function maintains a cache of logger instances per logger_name,
    so multiple calls with the same logger_name will return the same logger.
    
    Args:
        logger_name: Unique name for the logger (e.g., 'edgar_downloader', 'fred_downloader')
        log_file: Path to the log file. If None, uses '{logger_name}_errors.log'
        log_level: Logging level (default: logging.ERROR)
        add_console_handler: If True, also add a console handler for INFO+ messages
    
    Returns:
        Logger instance (cached per logger_name)
    """
    # Use default log file name if not provided
    if log_file is None:
        log_file = f'{logger_name}_errors.log'
    
    # Return cached logger if it exists
    if logger_name in _loggers:
        return _loggers[logger_name]
    
    # Create and cache new logger
    logger = setup_download_logger(logger_name, log_file, log_level, add_console_handler=add_console_handler)
    _loggers[logger_name] = logger
    
    return logger


def reset_logger(logger_name: str) -> None:
    """
    Reset a cached logger instance (useful for testing or reconfiguration).
    
    Args:
        logger_name: Name of the logger to reset
    """
    if logger_name in _loggers:
        del _loggers[logger_name]
        # Also remove handlers from the actual logger
        logger = logging.getLogger(logger_name)
        logger.handlers = []
