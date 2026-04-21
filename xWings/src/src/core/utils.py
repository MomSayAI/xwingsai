# File: src/core/utils.py
# Purpose: Utility functions and path management for xWings trading system
# Dependencies: os, pathlib, logging, threading
# Notes: All timestamps in UTC+8. PathManager centralizes path definitions with robust validation.
#        Adapted for Windows/Linux path compatibility. Uses singleton pattern for PathManager.
# Updated: 2025-08-05

import sys
import os
import logging
import threading
from pathlib import Path
from typing import Optional
import time

# Initialize logging
logger = logging.getLogger(__name__)


def _create_directory_with_retry(path: Path, retries: int = 3, delay: int = 1) -> None:
    """
    Create directory with retry mechanism for handling transient errors.

    Args:
        path (Path): Directory path to create
        retries (int): Number of retries
        delay (int): Delay between retries in seconds

    Raises:
        PermissionError: If directory is not writable
        OSError: If directory creation fails after retries
    """
    for attempt in range(retries):
        try:
            path.mkdir(parents=True, exist_ok=True)
            if not os.access(path, os.W_OK):
                logging.error(f"Directory {path} is not writable")
                raise PermissionError(f"Directory {path} is not writable")
            logging.debug(f"Created directory: {path}")
            break
        except Exception as e:
            logging.warning(f"Failed to create directory {path} (attempt {attempt+1}/{retries}): {str(e)}")
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                logging.error(f"Failed to create directory {path} after {retries} attempts")
                raise


class PathManager:
    """Centralized path management for xWings project."""
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, project_root: Path):
        """Implement singleton pattern for PathManager."""
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialize(project_root)
            return cls._instance

    def _initialize(self, project_root: Path):
        """
        Initialize PathManager with project root.

        Args:
            project_root (Path): Root directory of the project

        Raises:
            PermissionError: If directories are not writable
        """
        logging.debug("PathManager initialized")
        self.project_root = project_root.resolve()  # Ensure absolute path
        self.src_path = self.project_root / 'src'
        self.core_path = self.src_path / 'core'
        self.config_path = self.src_path / 'config'
        self.data_path = self.project_root / 'data'
        self.data_raw_path = self.data_path / 'raw'
        self.data_processed_path = self.data_path / 'processed'
        self.data_backtest_path = self.data_path / 'backtest'
        self.log_dir = self.project_root / 'logs'
        self.reports_path = self.project_root / 'reports'
        self.reports_client_statements_path = self.reports_path / 'client_statements'
        self.reports_monthly_path = self.reports_path / 'monthly'
        self.reports_performance_path = self.reports_path / 'performance'

        # Ensure directories exist with retry
        for path in [
            self.log_dir,
            self.data_raw_path,
            self.data_processed_path,
            self.data_backtest_path,
            self.config_path,
            self.reports_client_statements_path,
            self.reports_monthly_path,
            self.reports_performance_path
        ]:
            _create_directory_with_retry(path)

        # Dynamically add paths to sys.path if not already present
        self._configure_sys_path()

    def _configure_sys_path(self) -> None:
        """Add project paths to sys.path to ensure module imports work."""
        paths_to_add = [str(self.project_root), str(self.src_path), str(self.core_path)]
        for path in paths_to_add:
            if path not in sys.path:
                sys.path.insert(0, path)
                logging.debug(f"Added {path} to sys.path")

    def get_log_path(self, log_file: str) -> Path:
        """
        Get path for a log file.

        Args:
            log_file (str): Name of the log file

        Returns:
            Path: Full path to the log file
        """
        return self.log_dir / log_file.replace('/', os.sep)

    def get_config_path(self, config_file: str) -> Path:
        """
        Get path for a config file.

        Args:
            config_file (str): Name of the config file

        Returns:
            Path: Full path to the config file
        """
        return self.config_path / config_file.replace('/', os.sep)

    def get_data_path(self, subpath: str) -> Path:
        """
        Get path for a data file in data/raw or data/processed.

        Args:
            subpath (str): Subpath within data directory (e.g., 'processed/api_changes.json')

        Returns:
            Path: Full path to the data file
        """
        base_path = self.data_path / subpath.replace('/', os.sep)
        _create_directory_with_retry(base_path.parent)
        return base_path

    def get_custom_path(self, category: str, subpath: str) -> Path:
        """
        Get path for a custom category and subpath.

        Args:
            category (str): Category (e.g., 'reports', 'data')
            subpath (str): Subpath within the category

        Returns:
            Path: Full path to the custom location
        """
        base_path = self.project_root / category / subpath.replace('/', os.sep)
        _create_directory_with_retry(base_path.parent)
        return base_path
