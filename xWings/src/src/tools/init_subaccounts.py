# File: src/tools/init_subaccounts.py
# Purpose: Initialize subaccounts in MongoDB for xWings trading system
# Dependencies: pymongo, core.config_manager, core.logging_config, core.utils
# Notes: Creates or updates subaccounts based on client_credentials_encrypted.yaml.
#        Timestamps in UTC+8 for consistency with other system components.
#        Uses global logging configuration for consistent logging.
# Updated: 2025-08-05

import sys
import logging
import time
import os
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError, ConfigurationError, OperationFailure
from pathlib import Path
from datetime import datetime, timezone, timedelta

# Set working directory to project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]
os.chdir(PROJECT_ROOT)

# Define paths
SRC_PATH = PROJECT_ROOT / 'src'
CORE_PATH = SRC_PATH / 'core'

# Adjust sys.path with absolute paths
for path in [PROJECT_ROOT, SRC_PATH, CORE_PATH]:
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

# Verify paths
for path in [PROJECT_ROOT, SRC_PATH, CORE_PATH]:
    if not path.exists():
        raise FileNotFoundError(f"Directory not found: {path}")

try:
    from src.core.utils import PathManager
    from src.core.config_manager import ConfigManager
except ImportError as e:
    logging.error(f"Failed to import core modules: {str(e)}. Ensure src/core/ contains __init__.py")
    raise

# Initialize logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.debug(f"PROJECT_ROOT: {PROJECT_ROOT}")

def init_subaccounts():
    """
    Initialize or update subaccounts in MongoDB based on client_credentials_encrypted.yaml.
    """
    try:
        # Load database configuration from parameters_encrypted.yaml
        path_manager = PathManager(PROJECT_ROOT)
        config_manager = ConfigManager(key_file=str(path_manager.get_config_path('encryption.key')))
        
        # Retrieve database configuration from parameters_encrypted.yaml
        params_config = config_manager.get_config('parameters_encrypted')
        if not params_config:
            logger.error("Failed to load parameters_encrypted.yaml")
            raise ValueError("Failed to load parameters_encrypted.yaml")
        
        db_config = params_config.get('database')
        if not db_config:
            logger.error("Database configuration not found in parameters_encrypted.yaml")
            raise ValueError("Database configuration not found in parameters_encrypted.yaml")
        
        required_keys = ['host', 'port', 'database', 'user', 'password']
        missing_keys = [key for key in required_keys if key not in db_config or not db_config[key]]
        if missing_keys:
            logger.error(f"Missing database configuration keys: {missing_keys}")
            raise ConfigurationError(f"Missing database configuration keys: {missing_keys}")

        # Construct MongoDB URI
        user = db_config['user']
        password = db_config['password']
        host = db_config['host']
        port = db_config['port']
        database = db_config['database']
        from urllib.parse import quote_plus
        escaped_user = quote_plus(user)
        escaped_password = quote_plus(password)
        uri = f"mongodb://{escaped_user}:{escaped_password}@{host}:{port}/{database}?authSource={database}"

        # Connect to MongoDB
        max_retries = 3
        for attempt in range(max_retries):
            try:
                client = MongoClient(uri, serverSelectionTimeoutMS=60000, connectTimeoutMS=60000)
                db = client[database]
                logger.info(f"Connected to MongoDB: {database}")
                break
            except ServerSelectionTimeoutError as e:
                logger.error(f"MongoDB connection attempt {attempt + 1} failed: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(5 * (2 ** attempt))
                else:
                    raise

        # Load subaccounts from client_credentials_encrypted.yaml
        credentials = config_manager.get_config('client_credentials_encrypted')
        if not credentials:
            logger.error("Failed to load client_credentials_encrypted.yaml")
            raise ValueError("Failed to load client_credentials_encrypted.yaml")
            
        subaccounts_collection = db['subaccounts']
        current_time = datetime.now(timezone(timedelta(hours=8)))
        
        for exchange, subaccounts in credentials.get('subaccounts', {}).items():
            for subaccount_id, config in subaccounts.items():
                # Use login_name as the actual subaccount ID
                login_name = config.get('login_name', subaccount_id)
                
                subaccount_data = {
                    '_id': f"{exchange}.{login_name}",  # Use login_name as the primary key
                    'xwings_uid': f"{exchange}_{login_name}",
                    'exchange': exchange,
                    'subaccount_id': f"{exchange}.{login_name}",
                    'api_key_name': config.get('api_key_name', ''),
                    'api_key': config.get('api_key', ''),
                    'api_secret': config.get('api_secret', ''),
                    'passphrase': config.get('passphrase', ''),
                    'permissions': config.get('Permissions', []),
                    'ip': config.get('IP', ''),
                    'account_type': config.get('account_type', 'unknown'),
                    'account_mode': config.get('account_mode', 'unknown'),
                    'fund_allocations': config.get('fund_allocations', {}),
                    'symbol_allocations': config.get('symbol_allocations', {}),
                    'trading_config': config.get('trading_config', {}),
                    'created_at': current_time,
                    'updated_at': current_time
                }
                
                # Perform upsert operation: update if exists, insert if not
                subaccounts_collection.update_one(
                    {'_id': exchange+"."+login_name},
                    {'$set': subaccount_data},
                    upsert=True
                )
                logger.info(f"Initialized subaccount: {login_name}")

                # Process nested subaccounts
                for nested_subaccount_id, nested_config in config.get('subaccounts', {}).items():
                    nested_login_name = nested_config.get('login_name', nested_subaccount_id)
                    
                    nested_subaccount_data = {
                        '_id': nested_login_name,  # Use nested login_name as the primary key
                        'xwings_uid': f"{exchange}_{login_name}_{nested_login_name}",
                        'exchange': exchange,
                        'parent_subaccount_id': login_name,  # Add parent subaccount ID
                        'subaccount_id': nested_login_name,
                        'api_key_name': nested_config.get('api_key_name', ''),
                        'api_key': nested_config.get('api_key', ''),
                        'api_secret': nested_config.get('api_secret', ''),
                        'passphrase': nested_config.get('passphrase', ''),
                        'permissions': nested_config.get('Permissions', []),
                        'ip': nested_config.get('IP', ''),
                        'account_type': nested_config.get('account_type', 'unknown'),
                        'account_mode': nested_config.get('account_mode', 'unknown'),
                        'fund_allocations': nested_config.get('fund_allocations', {}),
                        'symbol_allocations': nested_config.get('symbol_allocations', {}),
                        'trading_config': nested_config.get('trading_config', {}),
                        'created_at': current_time,
                        'updated_at': current_time
                    }
                    
                    subaccounts_collection.update_one(
                        {'_id': nested_login_name},
                        {'$set': nested_subaccount_data},
                        upsert=True
                    )
                    logger.info(f"Initialized nested subaccount: {nested_login_name}")

        logger.info("Subaccounts initialized successfully")
        client.close()
        
    except ServerSelectionTimeoutError as e:
        logger.error(f"Database connection failed: {str(e)}")
        raise
    except ConfigurationError as e:
        logger.error(f"Invalid database configuration: {str(e)}")
        raise
    except OperationFailure as e:
        logger.error(f"Database operation failed: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Failed to initialize subaccounts: {str(e)}")
        raise

if __name__ == '__main__':
    init_subaccounts()