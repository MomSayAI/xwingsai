# File: src/tools/create_mongo_collections.py
# Purpose: Create MongoDB collections for xWings trading system with necessary indexes
# Dependencies: core.database, core.config_manager, core.logging_config, core.utils
# Notes: Uses Database class for MongoDB operations. Dynamically creates indexes for ohlcv collection
#        based on timeframes in client_credentials_encrypted.yaml. Timestamps in UTC+8.
#        Uses global logging configuration for consistent logging.
# Updated: 2025-08-05

import sys
import logging
import asyncio
import os
from pathlib import Path
from typing import List, Set
from datetime import datetime, timezone, timedelta

PROJECT_ROOT = Path(__file__).resolve().parents[2]
os.chdir(PROJECT_ROOT)
sys.path.extend([str(path) for path in [PROJECT_ROOT, PROJECT_ROOT / 'src', PROJECT_ROOT / 'src/core'] if str(path) not in sys.path])

try:
    from src.core.database import Database
    from src.core.utils import PathManager
    from src.core.config_manager import ConfigManager
except ImportError as e:
    logging.error(f"Failed to import core modules: {str(e)}. Ensure src/core/ contains __init__.py")
    raise

# Initialize logger
logger = logging.getLogger(__name__)
logger.debug(f"PROJECT_ROOT: {PROJECT_ROOT}")

def get_all_timeframes(credentials: dict) -> List[str]:
    """
    Extract all unique timeframes from trading_config in client_credentials_encrypted.yaml.

    Args:
        credentials (dict): Client credentials configuration

    Returns:
        List[str]: Sorted list of unique timeframes
    """
    timeframes: Set[str] = set()
    for exchange, subaccounts in credentials.get('subaccounts', {}).items():
        for subaccount_id, config in subaccounts.items():
            trading_config = config.get('trading_config', {}).get('symbols', {})
            for symbol_config in trading_config.values():
                timeframes.update(symbol_config.get('timeframes', []))
            for nested_subaccount_id, nested_subaccount in config.get('subaccounts', {}).items():
                nested_trading_config = nested_subaccount.get('trading_config', {}).get('symbols', {})
                for symbol_config in nested_trading_config.values():
                    timeframes.update(symbol_config.get('timeframes', []))
    # Retrieve timeframes from configuration or default to ['5m', '15m', '30m']
    timeframes_list = sorted(list(timeframes)) or ['5m', '15m', '30m']
    logger.debug(f"Extracted timeframes: {timeframes_list}")
    return timeframes_list

async def create_collections():
    """
    Create MongoDB collections for xWings trading system and manage indexes asynchronously.
    Timestamps in UTC+8 for logs.
    """
    try:
        path_manager = PathManager(PROJECT_ROOT)
        config_manager = ConfigManager(key_file=str(path_manager.get_config_path('encryption.key')))
        params_config = config_manager.get_config('parameters_encrypted')
        if not params_config:
            logger.error("Failed to load parameters_encrypted.yaml")
            raise ValueError("Failed to load parameters_encrypted.yaml")

        db_config = params_config.get('database')
        required_keys = ['host', 'port', 'database', 'user', 'password']
        missing_keys = [key for key in required_keys if key not in db_config or not db_config[key]]
        if missing_keys:
            logger.error(f"Missing database configuration keys: {missing_keys}")
            raise ValueError(f"Missing database configuration keys: {missing_keys}")

        # Initialize Database instance
        db = Database(db_config)
        logger.info(f"Connected to MongoDB: {db_config['database']}")

        credentials = config_manager.get_config('client_credentials_encrypted')
        if not credentials:
            logger.error("Failed to load client_credentials_encrypted.yaml")
            raise ValueError("Failed to load client_credentials_encrypted.yaml")

        # Extract supported timeframes
        supported_timeframes = get_all_timeframes(credentials)
        logger.info(f"Supported timeframes from trading_config: {supported_timeframes}")

        # Define collections and their indexes
        collections = [
            'subaccounts', 'trades', 'balances', 'ohlcv', 'api_stats',
            'order_books', 'pending_orders', 'positions', 'funds_allocations',
            'plugin_metadata', 'signals'
        ]

        expected_indexes = {
            'subaccounts': [('xwings_uid', 1)],
            'trades': [('order_id', 1, {'unique': True}), ('subaccount_id', 1), ('timestamp', -1)],
            'balances': [('subaccount_id', 1), ('timestamp', -1)],
            'ohlcv': [('symbol', 1), ('market_type', 1), ('timeframe', 1), ('timestamp', -1)],
            'signals': [('symbol', 1), ('market_type', 1), ('timeframe', 1), ('timestamp', -1)],
            # 'signals1': [('symbol', 1), ('market_type', 1), ('timeframe', 1), ('timestamp', -1)],
            # 'signals2': [('symbol', 1), ('market_type', 1), ('timeframe', 1), ('timestamp', -1)],
            'api_stats': [('exchange', 1), ('timestamp', -1)],
            'order_books': [('symbol', 1), ('market_type', 1), ('timestamp', -1)],
            'pending_orders': [('order_id', 1, {'unique': True}), ('subaccount_id', 1)],
            'positions': [('subaccount_id', 1), ('symbol', 1), ('market_type', 1)],
            'funds_allocations': [('subaccount_id', 1), ('timestamp', -1)],
            'plugin_metadata': [('exchange_name', 1)]
        }

        # Create collections and indexes using Database class
        for collection_name in collections:
            # Check if collection exists (handled by Database class)
            logger.info(f"Ensuring collection: {collection_name}")
            
            # Create collection if it doesn't exist
            try:
                # Insert a dummy document to create the collection
                dummy_doc = {"_id": "dummy", "created_at": datetime.now(timezone(timedelta(hours=8)))}
                await db.db[collection_name].insert_one(dummy_doc)
                # Delete the dummy document
                await db.db[collection_name].delete_one({"_id": "dummy"})
                logger.info(f"Created collection: {collection_name}")
            except Exception as e:
                logger.warning(f"Collection {collection_name} may already exist: {e}")
            
            # Create indexes for the collection
            for index_field in expected_indexes.get(collection_name, []):
                field = index_field[0] if isinstance(index_field, tuple) else index_field
                direction = index_field[1] if isinstance(index_field, tuple) else 1
                options = index_field[2] if isinstance(index_field, tuple) and len(index_field) > 2 else {}
                index_name = f"{collection_name}_{field}_{direction}"
                
                try:
                    # Create index
                    await db.db[collection_name].create_index(
                        [(field, direction)], 
                        name=index_name,
                        unique=options.get('unique', False),
                        background=True
                    )
                    logger.info(f"Created index {index_name} on {collection_name} with unique={options.get('unique', False)}")
                    print(f"Created index {index_name} on {collection_name} with unique={options.get('unique', False)}")
                except Exception as e:
                    logger.warning(f"Index {index_name} may already exist: {e}")
                    print(f"Index {index_name} may already exist: {e}")

        # Validate ohlcv data timeframes
        if supported_timeframes:
            ohlcv_data = await db.find('ohlcv', {})
            print(f"ohlcv_data from DB: {ohlcv_data}")
            for data in ohlcv_data:
                if data.get('timeframe') not in supported_timeframes:
                    logger.warning(f"Invalid timeframe {data.get('timeframe')} found in ohlcv collection for {data.get('symbol')}")

        logger.info("MongoDB collections and indexes created successfully")
        await db.close()
    except Exception as e:
        logger.error(f"Failed to create collections: {str(e)}")
        print(f"Failed to create collections: {str(e)}")
        raise


async def delete_signal_ohlov():
    """
    delete MongoDB collections for xWings trading system and manage indexes asynchronously.
    Timestamps in UTC+8 for logs.
    """
    try:
        path_manager = PathManager(PROJECT_ROOT)
        config_manager = ConfigManager(key_file=str(path_manager.get_config_path('encryption.key')))
        params_config = config_manager.get_config('parameters_encrypted')
        if not params_config:
            logger.error("Failed to load parameters_encrypted.yaml")
            raise ValueError("Failed to load parameters_encrypted.yaml")

        db_config = params_config.get('database')
        required_keys = ['host', 'port', 'database', 'user', 'password']
        missing_keys = [key for key in required_keys if key not in db_config or not db_config[key]]
        if missing_keys:
            logger.error(f"Missing database configuration keys: {missing_keys}")
            raise ValueError(f"Missing database configuration keys: {missing_keys}")

        # Initialize Database instance
        db = Database(db_config)
        logger.info(f"Connected to MongoDB: {db_config['database']}")

        # Define collections and their indexes
        collections = [
             'ohlcv', 'signals'
        ]

        # Create collections and indexes using Database class
        for collection_name in collections:

            # Create collection if it doesn't exist
            try:
                # Insert a dummy document to create the collection
                query = {}
                await db.delete(collection_name, query)
            except Exception as e:
                logger.warning(f"delete {collection_name} may already exist: {e}")
        db.close()
    except Exception as e:
        logger.error(f"Failed to create collections: {str(e)}")
        print(f"Failed to create collections: {str(e)}")
        raise

if __name__ == '__main__':
    asyncio.run(create_collections())
    # asyncio.run(delete_signal_ohlov())
    # print(datetime.now(timezone(timedelta(hours=8))))