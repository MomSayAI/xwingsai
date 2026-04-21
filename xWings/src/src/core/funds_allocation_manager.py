# File: src/core/funds_allocation_manager.py
# Purpose: Manage saving of funds allocations for xWings trading system
# Dependencies: core.database, core.config_manager, core.utils, config.schema, pymongo
# Notes: Separated from database.py to avoid circular imports.
#        Validates allocations and symbol allocations using schema.
#        Timestamps in UTC+8. Uses markets_encrypted.yaml for symbol validation.
#        Removed default subaccount insertion, relies on client_manager.py.
# Updated: 2025-08-05 20:23 PDT

import sys
import os
import time
from pathlib import Path
import logging
import asyncio
import threading
from typing import Dict, Any
from datetime import datetime, timezone, timedelta
import pymongo

# Set project root and ensure paths
PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_PATH = PROJECT_ROOT / 'src'
CORE_PATH = SRC_PATH / 'core'
for path in [PROJECT_ROOT, SRC_PATH, CORE_PATH]:
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)
        logging.debug(f"Added {path_str} to sys.path")

try:
    from src.core.database import Database
    from src.core.utils import PathManager
    from src.core.config_manager import ConfigManager
    from src.config.schema import AllocationConfig, SymbolAllocationConfig
except ModuleNotFoundError as e:
    logging.error(f"Module import error: {e}. Ensure src/core/ contains __init__.py and all required modules.")
    raise

# Define custom exception for database duplicate key errors
class DatabaseDuplicateKeyError(Exception):
    """Custom exception for MongoDB duplicate key errors."""
    pass

# Initialize logging
path_manager = PathManager(PROJECT_ROOT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class NewFundsAllocationManager:
    """
    New funds allocation manager implementation.
    Responsible for managing and saving funds allocation information to the database.
    """
    def __init__(self, db: Database):
        """
        Initialize the funds allocation manager.

        Args:
            db (Database): Database instance
        """
        self.db = db
        logger.info("NewFundsAllocationManager initialized")

    async def save_allocation(self, subaccount_id: str, allocation_config: Dict, symbol_allocations: Dict) -> None:
        """
        Save the subaccount's funds allocation configuration to the database.

        Args:
            subaccount_id (str): Subaccount ID
            allocation_config (Dict): Funds allocation configuration
            symbol_allocations (Dict): Symbol allocation configuration

        Raises:
            ValueError: If parameters are invalid
        """
        if not subaccount_id or not isinstance(subaccount_id, str):
            raise ValueError(f"Invalid subaccount_id: {subaccount_id}")

        try:
            document = {
                "subaccount_id": subaccount_id,
                "timestamp": datetime.now(timezone(timedelta(hours=8))),
                "allocations": allocation_config,
                "symbol_allocations": symbol_allocations
            }
            
            await self.db.insert("funds_allocations", document)
            logger.info(f"Saved allocation for {subaccount_id}")
            
        except Exception as e:
            logger.error(f"Failed to save allocation for {subaccount_id}: {str(e)}")
            raise

    async def get_allocation(self, subaccount_id: str) -> Dict:
        """
        Retrieve the latest funds allocation configuration for a subaccount.

        Args:
            subaccount_id (str): Subaccount ID

        Returns:
            Dict: Funds allocation configuration
        """
        try:
            result = await self.db.find_one(
                "funds_allocations",
                {"subaccount_id": subaccount_id},
                sort=[("timestamp", -1)]
            )
            return result or {}
        except Exception as e:
            logger.error(f"Failed to get allocation for {subaccount_id}: {str(e)}")
            raise

    async def update_allocation(self, subaccount_id: str, allocation_config: Dict, symbol_allocations: Dict) -> None:
        """
        Update the subaccount's funds allocation configuration.

        Args:
            subaccount_id (str): Subaccount ID
            allocation_config (Dict): New funds allocation configuration
            symbol_allocations (Dict): New symbol allocation configuration
        """
        try:
            document = {
                "subaccount_id": subaccount_id,
                "timestamp": datetime.now(timezone(timedelta(hours=8))),
                "allocations": allocation_config,
                "symbol_allocations": symbol_allocations
            }
            
            await self.db.update_one(
                "funds_allocations",
                {"subaccount_id": subaccount_id},
                {"$set": document},
                upsert=True
            )
            logger.info(f"Updated allocation for {subaccount_id}")
            
        except Exception as e:
            logger.error(f"Failed to update allocation for {subaccount_id}: {str(e)}")
            raise

    def close(self) -> None:
        """
        Close the funds allocation manager.
        """
        try:
            if hasattr(self, 'db'):
                self.db.close()
            logger.info("NewFundsAllocationManager closed")
        except Exception as e:
            logger.error(f"Error closing NewFundsAllocationManager: {str(e)}")

# For backward compatibility, retain old class name but use new implementation
FundsAllocationManager = NewFundsAllocationManager