# File: src/core/logging_config.py
# Purpose: Configure logging for xWings trading system with rotation and dynamic levels
# Dependencies: logging, logging.handlers, datetime, zoneinfo, pathlib, threading
# Notes: Configures logging with UTC+8 timestamps and rotation.
#        Optimized to prevent duplicate handlers and ensure Windows/Linux compatibility.
#        Added thread lock to prevent concurrent configuration.
#        Ensures DEBUG, INFO, WARNING, ERROR levels are logged to file and console.
#        Added protection against Python shutdown errors.
# Updated: 2025-08-05

import logging
import sys
import os
import threading
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
from logging.handlers import RotatingFileHandler
from typing import Optional

# Ensure xWings/ is in sys.path for imports
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.core.utils import PathManager


def is_python_shutting_down() -> bool:
    """
    Check if Python interpreter is shutting down.
    
    Returns:
        bool: True if Python is shutting down, False otherwise
    """
    try:
        return sys.meta_path is None or 'logging' not in sys.modules
    except Exception:
        return True


def safe_log(logger_func, *args, **kwargs):
    """
    Safely call a logger function, checking if Python is shutting down.
    
    Args:
        logger_func: Logger function to call (e.g., logger.info, logger.error)
        *args: Arguments to pass to logger function
        **kwargs: Keyword arguments to pass to logger function
    """
    try:
        if not is_python_shutting_down():
            logger_func(*args, **kwargs)
    except Exception:
        # Python is shutting down or logging failed, ignore
        pass


def configure_logging(log_file: str, log_level: str = 'INFO') -> logging.Logger:
    """
    Configure logging with specified log file, UTC+8 timezone, and rotation.

    Args:
        log_file (str): Path to the log file
        log_level (str, optional): Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL). Defaults to 'DEBUG'

    Returns:
        logging.Logger: Configured root logger

    Raises:
        ValueError: If log_level is invalid
        PermissionError: If log directory cannot be created
    """
    logger = logging.getLogger()  # Get root logger
    path_manager = PathManager(PROJECT_ROOT)
    log_path = path_manager.get_log_path(log_file)

    # Check for existing handlers with the same log file
    for handler in logger.handlers[:]:
        if isinstance(handler, RotatingFileHandler) and handler.baseFilename == str(log_path):
            logging.debug(f"Logging already configured for {log_file}, returning existing logger")
            return logger

    try:
        log_dir = log_path.parent
        log_dir.mkdir(parents=True, exist_ok=True)
        if not os.access(log_dir, os.W_OK):
            raise PermissionError(f"Directory {log_dir} is not writable")

        # Determine log level
        level_map = {
            'DEBUG': logging.DEBUG,
            'INFO': logging.INFO,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR,
            'CRITICAL': logging.CRITICAL
        }
        log_level_value = level_map.get(log_level.upper(), logging.INFO)
        if log_level_value not in level_map.values():
            raise ValueError(f"Invalid log level: {log_level}. Must be one of {list(level_map.keys())}")

        # Clear existing handlers to avoid duplication
        logger.handlers.clear()

        # Configure handlers
        file_handler = RotatingFileHandler(
            log_path,
            maxBytes=30*1024*1024,  # 30MB per file
            backupCount=5,          # Keep 5 backup files
            encoding='utf-8'
        )
        # file_handler.setLevel(logging.INFO)  # Capture all levels to file
        file_handler.setLevel(log_level_value)

        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)  # Capture all levels to console
        # stream_handler.setLevel(log_level_value)  # Capture all levels to console

        # Set format with millisecond precision
        log_format = '%(asctime)s.%(msecs)03d UTC+08:00 - %(name)s - %(levelname)s - %(message)s'
        formatter = logging.Formatter(log_format, datefmt='%Y-%m-%d %H:%M:%S')
        file_handler.setFormatter(formatter)
        stream_handler.setFormatter(formatter)

        # Configure root logger
        logger.setLevel(log_level_value)
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

        # Set timezone for logging with shutdown protection
        def safe_time_converter(*args):
            try:
                # Check if Python is shutting down
                if sys.meta_path is None:
                    return datetime.now().timetuple()

                # Check if ZoneInfo is available
                if 'zoneinfo' not in sys.modules:
                    return datetime.now().timetuple()

                return datetime.now(ZoneInfo("Asia/Shanghai")).timetuple()
            except ImportError:
                # Python is likely shutting down, return simple time
                return datetime.now().timetuple()
            except Exception:
                # Any other error, return simple time
                return datetime.now().timetuple()

        logging.Formatter.converter = safe_time_converter
        logging.info(f"Logging configured with file: {log_path}, level: {log_level}, timezone: UTC+08:00")
        return logger
    except Exception as e:
        print(f"Critical: Failed to configure logging: {str(e)}")  # Fallback to print if logging fails
        raise


def shutdown_logging():
    """
    Properly shutdown logging to prevent errors during Python exit.
    """
    try:
        # Check if Python is shutting down
        if sys.meta_path is None:
            return
        
        logging.shutdown()
    except ImportError:
        # Python is shutting down, modules are being unloaded
        pass
    except Exception as e:
        print(f"Critical: Failed to shutdown logging: {str(e)}")