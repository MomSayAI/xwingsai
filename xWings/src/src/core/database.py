# File: src/core/database.py
# Purpose: MongoDB database operations for xWings trading system, including analytics metrics storage and retrieval
# Dependencies: motor, pymongo, asyncio, logging, threading, yaml
# Notes: All timestamps in UTC+8. Supports async operations with retry mechanism and thread safety.
#        Uses PathManager for path configuration. Handles subaccount validation and data persistence.
# Updated: 2025-08-05 19:40 PDT

import sys
import os
import asyncio
from pathlib import Path
from motor.motor_asyncio import AsyncIOMotorClient
import logging
import time
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone, timedelta
from bson.objectid import ObjectId
from urllib.parse import quote_plus
from threading import Lock
import yaml
import pymongo

# Set working directory to project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]
os.chdir(PROJECT_ROOT)

# Ensure project root and src are in sys.path
SRC_PATH = PROJECT_ROOT / 'src'
CORE_PATH = SRC_PATH / 'core'
for path in [PROJECT_ROOT, SRC_PATH, CORE_PATH]:
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)
        logging.debug(f"Added {path_str} to sys.path")

from src.core.utils import PathManager

# Initialize logging (logging configured in main.py)
path_manager = PathManager(PROJECT_ROOT)
logger = logging.getLogger(__name__)

# Define custom exception
class DatabaseDuplicateKeyError(Exception):
    """Exception raised for duplicate key errors in database operations."""
    pass

class Database:
    MAX_RETRIES = 3
    RETRY_DELAY = 1
    _lock = None

    def __init__(self, db_config: Dict[str, Any], server_selection_timeout_ms: int = 10000, connect_timeout_ms: int = 10000):
        """
        Initialize Database with MongoDB connection parameters. Timestamps in UTC+8.

        Args:
            db_config (dict): Database configuration (host, port, database, user, password)
            server_selection_timeout_ms (int): Timeout for server selection in milliseconds
            connect_timeout_ms (int): Timeout for connection establishment in milliseconds

        Raises:
            ConfigurationError: If configuration is invalid
            Exception: If connection fails after retries
        """
        self.db_config = db_config or {}
        self.client = None
        self.db = None
        # logger.info(f"Start Connect MongoDB.")
        if Database._lock is None:
            Database._lock = asyncio.Lock()
        self._connect(server_selection_timeout_ms, connect_timeout_ms)

    def _connect(self, server_selection_timeout_ms: int, connect_timeout_ms: int):
        """Establish MongoDB connection with retries."""
        required_keys = ['host', 'port', 'database', 'user', 'password']
        missing_keys = [key for key in required_keys if key not in self.db_config or not self.db_config[key]]
        if missing_keys:
            logger.error(f"Missing database configuration keys: {missing_keys}")
            raise ValueError(f"Missing database configuration keys: {missing_keys}")

        for attempt in range(self.MAX_RETRIES):
            try:
                user = self.db_config['user']
                password = self.db_config['password']
                host = self.db_config['host']
                port = self.db_config['port']
                database = self.db_config['database']
                escaped_user = quote_plus(user)
                escaped_password = quote_plus(password)
                uri = f"mongodb://{escaped_user}:{escaped_password}@{host}:{port}/{database}?authSource={database}"
                self.client = AsyncIOMotorClient(uri, serverSelectionTimeoutMS=server_selection_timeout_ms,
                                                 connectTimeoutMS=connect_timeout_ms)
                self.db = self.client[database]
                # logger.info(f"Connected to MongoDB: {database} (host: {host}, data path: {path_manager.data_path})")
                break
            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed to connect to MongoDB: {str(e)}")
                if attempt < self.MAX_RETRIES - 1:
                    time.sleep(self.RETRY_DELAY)
                    self.RETRY_DELAY *= 2
                else:
                    raise

    async def _validate_subaccount(self, subaccount_id: str) -> None:
        """Validate subaccount_id exists in subaccounts collection or create if not exists."""
        lock = asyncio.Lock()
        async with lock:
            if not subaccount_id or not isinstance(subaccount_id, str):
                logger.error(f"Invalid subaccount_id: {subaccount_id}")
                raise ValueError(f"Invalid subaccount_id: {subaccount_id}")
            if not await self.db.subaccounts.find_one({"_id": subaccount_id}):
                default_doc = {
                    "_id": subaccount_id,
                    "exchange": "unknown",
                    "xwings_uid": f"unknown_{subaccount_id}",
                    "subaccounts": {}
                }
                await self.db.subaccounts.insert_one(default_doc)
                logger.warning(f"Subaccount {subaccount_id} not found, created with default values")

    async def insert(self, collection: str, document: Dict) -> None:
        """
        Insert a document into a collection asynchronously.

        Args:
            collection (str): Collection name
            document (dict): Document to insert

        Raises:
            DatabaseDuplicateKeyError: If a duplicate key is detected
        """
        for attempt in range(self.MAX_RETRIES):
            try:
                lock = asyncio.Lock()
                async with lock:
                    if "subaccount_id" in document:
                        await self._validate_subaccount(document["subaccount_id"])
                    if "timestamp" in document:
                        if isinstance(document["timestamp"], (int, float)):
                            document["timestamp"] = datetime.fromtimestamp(document["timestamp"] / 1000, tz=timezone(timedelta(hours=8)))
                        elif isinstance(document["timestamp"], str):
                            document["timestamp"] = datetime.fromisoformat(document["timestamp"]).astimezone(timezone(timedelta(hours=8)))
                    await self.db[collection].insert_one(document)
                    logger.debug(f"Inserted document into {collection}: {document}")
                    return
            except pymongo.errors.DuplicateKeyError as e:
                raise DatabaseDuplicateKeyError(f"Duplicate key error: {str(e)}")
            except Exception as e:
                logger.error(f"Insert attempt {attempt + 1} failed for {collection}: {str(e)}")
                if attempt < self.MAX_RETRIES - 1:
                    await asyncio.sleep(self.RETRY_DELAY)
                    self.RETRY_DELAY *= 2
                else:
                    raise

    async def find(self, collection: str, query: Dict = {}, sort: List[tuple] = None, limit: int = None) -> List[Dict]:
        """
        Fetch documents from a collection asynchronously.

        Args:
            collection (str): Collection name
            query (dict): Query filter
            sort (list of tuples): Sort criteria (e.g., [('timestamp', -1)])
            limit (int): Maximum number of documents to return

        Returns:
            list: List of documents
        """
        try:
            cursor = self.db[collection].find(query)
            if sort:
                cursor = cursor.sort(sort)
            if limit:
                cursor = cursor.limit(limit)
            results = await cursor.to_list(length=limit)
            for doc in results:
                if "_id" in doc:
                    doc["_id"] = str(doc["_id"])
            logger.debug(f"Fetched {len(results)} documents from {collection}")
            return results
        except Exception as e:
            logger.error(f"Failed to find documents in {collection}: {str(e)}")
            raise

    async def find_one(self, collection: str, query: Dict) -> Optional[Dict]:
        """
        Fetch a single document from a collection asynchronously.

        Args:
            collection (str): Collection name
            query (dict): Query filter

        Returns:
            Optional[Dict]: The document if found, else None
        """
        try:
            result = await self.db[collection].find_one(query)
            if result and "_id" in result:
                result["_id"] = str(result["_id"])
            logger.debug(f"Fetched document from {collection}: {result}")
            return result
        except Exception as e:
            logger.error(f"Database query failed for {collection}: {str(e)}")
            raise

    async def save_ohlcv(self, ohlcv_data: Dict[str, Any]) -> None:
        """
        Save or update OHLCV data to the ohlcv collection asynchronously.

        Args:
            ohlcv_data (dict): OHLCV data with symbol, market_type, timeframe, timestamp, etc.

        Raises:
            DatabaseDuplicateKeyError: If duplicate key is detected
        """
        for attempt in range(self.MAX_RETRIES):
            try:
                lock = asyncio.Lock()
                async with lock:
                    if "timestamp" in ohlcv_data:
                        if isinstance(ohlcv_data["timestamp"], (int, float)):
                            ohlcv_data["timestamp"] = datetime.fromtimestamp(ohlcv_data["timestamp"] / 1000, tz=timezone(timedelta(hours=8)))
                        elif isinstance(ohlcv_data["timestamp"], str):
                            ohlcv_data["timestamp"] = datetime.fromisoformat(ohlcv_data["timestamp"]).astimezone(timezone(timedelta(hours=8)))
                    await self.db['ohlcv'].update_one(
                        {
                            'symbol': ohlcv_data['symbol'],
                            'market_type': ohlcv_data['market_type'],
                            'timeframe': ohlcv_data['timeframe'],
                            'timestamp': ohlcv_data['timestamp']
                        },
                        {'$set': ohlcv_data},
                        upsert=True
                    )
                    logger.info(f"Saved OHLCV data{ohlcv_data} for {ohlcv_data['symbol']}/{ohlcv_data['market_type']}/{ohlcv_data['timeframe']} at {ohlcv_data['timestamp']}")
                    return
            except pymongo.errors.DuplicateKeyError as e:
                raise DatabaseDuplicateKeyError(f"Duplicate key error for OHLCV: {str(e)}")
            except Exception as e:
                logger.error(f"Save OHLCV attempt {attempt + 1} failed: {str(e)}")
                if attempt < self.MAX_RETRIES - 1:
                    await asyncio.sleep(self.RETRY_DELAY)
                    self.RETRY_DELAY *= 2
                else:
                    raise

    async def save_signal_one(self, signal_data: Dict[str, Any]) -> None:
        """
        Save or update SIGNAL data to the signal collection asynchronously.

        Args:
            signal_data (dict): SIGNAL data with symbol, market_type, timeframe, timestamp, etc.

        Raises:
            DatabaseDuplicateKeyError: If duplicate key is detected
        """
        for attempt in range(self.MAX_RETRIES):
            try:
                lock = asyncio.Lock()
                async with lock:
                    if "timestamp" in signal_data:
                        if isinstance(signal_data["timestamp"], (int, float)):
                            signal_data["timestamp"] = datetime.fromtimestamp(signal_data["timestamp"] / 1000, tz=timezone(timedelta(hours=8)))
                        elif isinstance(signal_data["timestamp"], str):
                            signal_data["timestamp"] = datetime.fromisoformat(signal_data["timestamp"]).astimezone(timezone(timedelta(hours=8)))
                    await self.db['signals'].update_one(
                        {
                            'symbol': signal_data['symbol'],
                            'market_type': signal_data['market_type'],
                            'timeframe': signal_data['timeframe'],
                            'timestamp': signal_data['timestamp']
                        },
                        {'$set': signal_data},
                        upsert=True
                    )
                    logger.info(f"Saved SIGNAL data{signal_data} for {signal_data['symbol']}/{signal_data['market_type']}/{signal_data['timeframe']} at {signal_data['timestamp']}")
                    return
            except pymongo.errors.DuplicateKeyError as e:
                raise DatabaseDuplicateKeyError(f"Duplicate key error for OHLCV: {str(e)}")
            except Exception as e:
                logger.error(f"Save OHLCV attempt {attempt + 1} failed: {str(e)}")
                if attempt < self.MAX_RETRIES - 1:
                    await asyncio.sleep(self.RETRY_DELAY)
                    self.RETRY_DELAY *= 2
                else:
                    raise

    async def save_ohlcv_batch(self, ohlcv_data_list: List[Dict[str, Any]]) -> None:
        """
        Save or update multiple OHLCV data entries to the ohlcv collection asynchronously.
        """
        for attempt in range(self.MAX_RETRIES):
            try:
                lock = asyncio.Lock()
                async with lock:
                    for ohlcv_data in ohlcv_data_list:
                        if "timestamp" in ohlcv_data:
                            if isinstance(ohlcv_data["timestamp"], (int, float)):
                                ohlcv_data["timestamp"] = datetime.fromtimestamp(ohlcv_data["timestamp"] / 1000, tz=timezone(timedelta(hours=8)))
                            elif isinstance(ohlcv_data["timestamp"], str):
                                ohlcv_data["timestamp"] = datetime.fromisoformat(ohlcv_data["timestamp"]).astimezone(timezone(timedelta(hours=8)))
                        await self.db['ohlcv'].update_one(
                            {
                                'symbol': ohlcv_data['symbol'],
                                'market_type': ohlcv_data['market_type'],
                                'timeframe': ohlcv_data['timeframe'],
                                'timestamp': ohlcv_data['timestamp']
                            },
                            {'$set': ohlcv_data},
                            upsert=True
                        )
                    logger.info(f"Saved {len(ohlcv_data_list)} OHLCV data entries (upsert) for {ohlcv_data_list[0]['symbol']}/{ohlcv_data_list[0]['market_type']}/{ohlcv_data_list[0]['timeframe']}")
                    return
            except pymongo.errors.BulkWriteError as e:
                raise DatabaseDuplicateKeyError(f"Bulk write error for OHLCV: {str(e)}")
            except Exception as e:
                logger.error(f"Save OHLCV batch attempt {attempt + 1} failed: {str(e)}")
                if attempt < self.MAX_RETRIES - 1:
                    await asyncio.sleep(self.RETRY_DELAY)
                    self.RETRY_DELAY *= 2
                else:
                    raise

    async def delete(self, collection: str, query: Dict) -> None:
        """
        Delete documents from a collection asynchronously.

        Args:
            collection (str): Collection name
            query (dict): Query filter
        """
        for attempt in range(self.MAX_RETRIES):
            try:
                lock = asyncio.Lock()
                async with lock:
                    result = await self.db[collection].delete_many(query)
                    logger.debug(f"Deleted {result.deleted_count} documents from {collection}")
                    return
            except Exception as e:
                logger.error(f"Delete attempt {attempt + 1} failed for {collection}: {str(e)}")
                if attempt < self.MAX_RETRIES - 1:
                    await asyncio.sleep(self.RETRY_DELAY)
                    self.RETRY_DELAY *= 2
                else:
                    raise

    async def save_trade(self, subaccount_id: str, exchange_name: str, symbol: str, market_type: str,
                         signal: str, price: float, quantity: float, fee: float, timestamp: float,
                         order_id: str, leverage: float = None, timeframe: str = None):
        """Save a trade to the trades collection."""
        for attempt in range(self.MAX_RETRIES):
            try:
                lock = asyncio.Lock()
                async with lock:
                    await self._validate_subaccount(subaccount_id)
                    document = {
                        "subaccount_id": subaccount_id,
                        "exchange_name": exchange_name,
                        "symbol": symbol,
                        "market_type": market_type,
                        "signal": signal,
                        "timestamp": datetime.fromtimestamp(timestamp / 1000, tz=timezone(timedelta(hours=8))),
                        "price": price,
                        "quantity": quantity,
                        "fee": fee,
                        "order_id": order_id,
                        "leverage": leverage or 1.0,
                        "timeframes": [timeframe] if timeframe else ["3m", "15m", "30m"]
                    }
                    await self.insert("trades", document)
                    logger.info(f"Saved trade for {subaccount_id}/{symbol}/{market_type} with leverage {leverage}")
                    return
            except Exception as e:
                logger.error(f"Save trade attempt {attempt + 1} failed for {subaccount_id}/{symbol}/{market_type}: {str(e)}")
                if attempt < self.MAX_RETRIES - 1:
                    await asyncio.sleep(self.RETRY_DELAY)
                    self.RETRY_DELAY *= 2
                else:
                    raise

    async def delete_pending_order(self, order_id: str):
        """Delete a pending order from the pending_orders collection."""
        for attempt in range(self.MAX_RETRIES):
            try:
                lock = asyncio.Lock()
                async with lock:
                    query = {"order_id": order_id}
                    await self.delete("pending_orders", query)
                    logger.debug(f"Deleted pending order {order_id}")
                    return
            except Exception as e:
                logger.error(f"Delete pending order attempt {attempt + 1} failed for {order_id}: {str(e)}")
                if attempt < self.MAX_RETRIES - 1:
                    await asyncio.sleep(self.RETRY_DELAY)
                    self.RETRY_DELAY *= 2
                else:
                    raise

    async def save_position(self, subaccount_id: str, symbol: str, market_type: str, position: str,
                            entry_price: float, quantity: float, leverage: float = None):
        """Save or update a position to the positions collection."""
        for attempt in range(self.MAX_RETRIES):
            try:
                lock = asyncio.Lock()
                async with lock:
                    await self._validate_subaccount(subaccount_id)
                    document = {
                        "subaccount_id": subaccount_id,
                        "symbol": symbol,
                        "market_type": market_type,
                        "position": position,
                        "entry_price": entry_price,
                        "quantity": quantity,
                        "leverage": leverage or 1.0,
                        "timeframes": ["3m", "15m", "30m"],
                        "timestamp": datetime.now(timezone(timedelta(hours=8)))
                    }
                    query = {
                        "subaccount_id": subaccount_id,
                        "symbol": symbol,
                        "market_type": market_type
                    }
                    await self.db.positions.replace_one(query, document, upsert=True)
                    logger.debug(f"Saved position for {subaccount_id}/{symbol}/{market_type} with leverage {leverage}")
                    return
            except Exception as e:
                logger.error(f"Save position attempt {attempt + 1} failed for {subaccount_id}/{symbol}/{market_type}: {str(e)}")
                if attempt < self.MAX_RETRIES - 1:
                    await asyncio.sleep(self.RETRY_DELAY)
                    self.RETRY_DELAY *= 2
                else:
                    raise

    async def get_position(self, subaccount_id: str, symbol: str, market_type: str) -> Dict[str, Any]:
        """Retrieve a position from the positions collection."""
        for attempt in range(self.MAX_RETRIES):
            try:
                lock = asyncio.Lock()
                async with lock:
                    await self._validate_subaccount(subaccount_id)
                    query = {
                        "subaccount_id": subaccount_id,
                        "symbol": symbol,
                        "market_type": market_type
                    }
                    result = await self.db.positions.find_one(query)
                    if result:
                        result["_id"] = str(result["_id"])
                        logger.debug(f"Retrieved position for {subaccount_id}/{symbol}/{market_type}")
                        return result
                    return {
                        "position": None,
                        "entry_price": 0.0,
                        "quantity": 0.0,
                        "leverage": 1.0,
                        "timeframes": ["3m", "15m", "30m"]
                    }
            except Exception as e:
                logger.error(f"Get position attempt {attempt + 1} failed for {subaccount_id}/{symbol}/{market_type}: {str(e)}")
                if attempt < self.MAX_RETRIES - 1:
                    await asyncio.sleep(self.RETRY_DELAY)
                    self.RETRY_DELAY *= 2
                else:
                    raise

    async def save_subaccount(self, subaccount_id: str,
                              exchange: str,
                              xwings_uid: str,
                              api_key: str = "",
                              api_secret: str = "",
                              passphrase: str = "",
                              initial_capital: float = 0.0,
                              timestamp: datetime = None):
        """Save or update a subaccount to the subaccounts collection."""
        for attempt in range(self.MAX_RETRIES):
            try:
                lock = asyncio.Lock()
                async with lock:
                    document = {
                        "_id": subaccount_id,
                        "exchange": exchange,
                        "xwings_uid": xwings_uid,
                        "api_key": api_key,
                        "api_secret": api_secret,
                        "passphrase": passphrase,
                        "initial_capital": initial_capital,
                        "timestamp": timestamp or datetime.now(timezone(timedelta(hours=8)))
                    }
                    await self.db.subaccounts.replace_one({"_id": subaccount_id}, document, upsert=True)
                    logger.debug(f"Saved subaccount: {subaccount_id}")
                    return
            except Exception as e:
                logger.error(f"Save subaccount attempt {attempt + 1} failed for {subaccount_id}: {str(e)}")
                if attempt < self.MAX_RETRIES - 1:
                    await asyncio.sleep(self.RETRY_DELAY)
                    self.RETRY_DELAY *= 2
                else:
                    raise

    async def save_balance(self, subaccount_id: str, exchange_name: str, asset: str, total: float, available: float,
                           futures_allocation: float = 0.0, spot_allocation: float = 0.0, reserve_allocation: float = 0.0):
        """Save a balance record to the balances collection."""
        for attempt in range(self.MAX_RETRIES):
            try:
                lock = asyncio.Lock()
                async with lock:
                    await self._validate_subaccount(subaccount_id)
                    document = {
                        "subaccount_id": subaccount_id,
                        "exchange_name": exchange_name,
                        "asset": asset,
                        "total": total,
                        "available": available,
                        "futures_allocation": futures_allocation,
                        "spot_allocation": spot_allocation,
                        "reserve_allocation": reserve_allocation,
                        "timestamp": datetime.now(timezone(timedelta(hours=8)))
                    }
                    await self.insert("balances", document)
                    logger.info(f"Saved balance for {subaccount_id}/{asset}")
                    return
            except Exception as e:
                logger.error(f"Save balance attempt {attempt + 1} failed for {subaccount_id}/{asset}: {str(e)}")
                if attempt < self.MAX_RETRIES - 1:
                    await asyncio.sleep(self.RETRY_DELAY)
                    self.RETRY_DELAY *= 2
                else:
                    raise

    async def save_api_stats(self, exchange_name: str, endpoint: str, call_count: int, error_count: int):
        """Save API call statistics to the api_stats collection."""
        for attempt in range(self.MAX_RETRIES):
            try:
                lock = asyncio.Lock()
                async with lock:
                    document = {
                        "exchange_name": exchange_name,
                        "endpoint": endpoint,
                        "call_count": call_count,
                        "error_count": error_count,
                        "timestamp": datetime.now(timezone(timedelta(hours=8)))
                    }
                    await self.insert("api_stats", document)
                    logger.debug(f"Saved API stats for {exchange_name}/{endpoint}")
                    return
            except Exception as e:
                logger.error(f"Save API stats attempt {attempt + 1} failed for {exchange_name}/{endpoint}: {str(e)}")
                if attempt < self.MAX_RETRIES - 1:
                    await asyncio.sleep(self.RETRY_DELAY)
                    self.RETRY_DELAY *= 2
                else:
                    raise

    async def save_order_book(self, symbol: str, market_type: str, bids: List[List[float]], asks: List[List[float]]):
        """Save an order book snapshot to the order_books collection."""
        for attempt in range(self.MAX_RETRIES):
            try:
                lock = asyncio.Lock()
                async with lock:
                    document = {
                        "symbol": symbol,
                        "market_type": market_type,
                        "bids": bids,
                        "asks": asks,
                        "timestamp": datetime.now(timezone(timedelta(hours=8)))
                    }
                    await self.insert("order_books", document)
                    logger.debug(f"Saved order book for {symbol}/{market_type}")
                    return
            except Exception as e:
                logger.error(f"Save order book attempt {attempt + 1} failed for {symbol}/{market_type}: {str(e)}")
                if attempt < self.MAX_RETRIES - 1:
                    await asyncio.sleep(self.RETRY_DELAY)
                    self.RETRY_DELAY *= 2
                else:
                    raise

    async def save_plugin_metadata(self, exchange_name: str, version: str, supported_methods: List[str], enabled: bool):
        """Save or update plugin metadata to the plugin_metadata collection."""
        for attempt in range(self.MAX_RETRIES):
            try:
                lock = asyncio.Lock()
                async with lock:
                    document = {
                        "exchange_name": exchange_name,
                        "version": version,
                        "supported_methods": supported_methods,
                        "enabled": enabled,
                        "timestamp": datetime.now(timezone(timedelta(hours=8)))
                    }
                    query = {"exchange_name": exchange_name}
                    await self.db.plugin_metadata.replace_one(query, document, upsert=True)
                    logger.debug(f"Saved plugin metadata for {exchange_name}")
                    return
            except Exception as e:
                logger.error(f"Save plugin metadata attempt {attempt + 1} failed for {exchange_name}: {str(e)}")
                if attempt < self.MAX_RETRIES - 1:
                    await asyncio.sleep(self.RETRY_DELAY)
                    self.RETRY_DELAY *= 2
                else:
                    raise

    async def get_plugin_metadata(self, exchange_name: str) -> Dict[str, Any]:
        """Retrieve plugin metadata from the plugin_metadata collection."""
        for attempt in range(self.MAX_RETRIES):
            try:
                lock = asyncio.Lock()
                async with lock:
                    query = {"exchange_name": exchange_name}
                    result = await self.db.plugin_metadata.find_one(query)
                    if result:
                        result["_id"] = str(result["_id"])
                        logger.debug(f"Retrieved plugin metadata for {exchange_name}")
                        return result
                    return {}
            except Exception as e:
                logger.error(f"Get plugin metadata attempt {attempt + 1} failed for {exchange_name}: {str(e)}")
                if attempt < self.MAX_RETRIES - 1:
                    await asyncio.sleep(self.RETRY_DELAY)
                    self.RETRY_DELAY *= 2
                else:
                    raise

    async def save_signal(self, symbol: str, market_type: str, timeframe: str,
                          timestamp: datetime, subaccount_id: str = None, signal_text: str = None):
        """
        Save signal data to the signals collection.

        Args:
            symbol (str): Trading pair
            market_type (str): Market type (spot/perpetual)
            timeframe (str): Timeframe (3m/15m/30m/1h)
            timestamp (datetime): Signal timestamp
            subaccount_id (str, optional): Subaccount identifier
            signal_text (str): Full signal text
        """
        try:
            document = {
                "symbol": symbol,
                "market_type": market_type,
                "timeframe": timeframe,
                "timestamp": timestamp,
                "signal_text": signal_text,
                "created_at": datetime.now(timezone(timedelta(hours=8)))
            }

            # Add to the document only when subaccount_id is not None
            if subaccount_id:
                document["subaccount_id"] = subaccount_id
                await self.insert("signals", document)
                logger.info(
                    f"Saved signal for {subaccount_id}/{symbol}/{market_type}/{timeframe}: {signal_text}")
            else:
                await self.insert("signals", document)
                logger.info(f"Saved signal for {symbol}/{market_type}/{timeframe}: {signal_text}")

        except Exception as e:
            account_info = f"{subaccount_id}/" if subaccount_id else ""
            logger.error(f"Failed to save signal for {account_info}{symbol}/{market_type}/{timeframe}: {str(e)}")
            raise

    async def save_signals_batch(self, signals_data: List[Dict[str, Any]]) -> None:
        """
        Save multiple signals to the signals collection in batch.
        """
        try:
            if not signals_data:
                return
            # Verify subaccount (only verify signals with subaccount_id)
            for signal in signals_data:
                if "subaccount_id" in signal and signal["subaccount_id"]:
                    await self._validate_subaccount(signal["subaccount_id"])
            # Batch upsert
            for signal in signals_data:
                await self.db['signals'].update_one(
                    {
                        'symbol': signal['symbol'],
                        'market_type': signal['market_type'],
                        'timeframe': signal['timeframe'],
                        'timestamp': signal['timestamp']
                    },
                    {'$set': signal},
                    upsert=True
                )
            logger.info(f"Saved {len(signals_data)} signals in batch (upsert)")
        except Exception as e:
            logger.error(f"Failed to save signals batch: {str(e)}")
            raise

    async def get_signals(self, subaccount_id: str = None, symbol: str = None, 
                         market_type: str = None, timeframe: str = None,
                         signal_type: str = None, start_time: datetime = None,
                         end_time: datetime = None, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Retrieve signals from the signals collection.

        Args:
            subaccount_id (str, optional): Filter by subaccount
            symbol (str, optional): Filter by symbol
            market_type (str, optional): Filter by market type
            timeframe (str, optional): Filter by timeframe
            signal_type (str, optional): Filter by signal type
            start_time (datetime, optional): Start time filter
            end_time (datetime, optional): End time filter
            limit (int): Maximum number of signals to return

        Returns:
            List[Dict]: List of signal documents
        """
        try:
            query = {}
            
            if subaccount_id:
                query["subaccount_id"] = subaccount_id
            if symbol:
                query["symbol"] = symbol
            if market_type:
                query["market_type"] = market_type
            if timeframe:
                query["timeframe"] = timeframe
            if signal_type:
                query["signal_type"] = signal_type
            if start_time or end_time:
                query["timestamp"] = {}
                if start_time:
                    query["timestamp"]["$gte"] = start_time
                if end_time:
                    query["timestamp"]["$lte"] = end_time
            
            return await self.find("signals", query, sort=[("timestamp", -1)], limit=limit)
            
        except Exception as e:
            logger.error(f"Failed to get signals: {str(e)}")
            raise

    async def get_csv_signals(self, subaccount_id: str = None, symbol: str = None,
                          market_type: str = None, timeframe: str = None,
                          signal_type: str = None, start_time: datetime = None,
                          end_time: datetime = None,
                          confirm: int = 1, limit: int = 60) -> List[Dict[str, Any]]:
        """
        Retrieve signals from the signals collection.

        Args:
            subaccount_id (str, optional): Filter by subaccount
            symbol (str, optional): Filter by symbol
            market_type (str, optional): Filter by market type
            timeframe (str, optional): Filter by timeframe
            signal_type (str, optional): Filter by signal type
            start_time (datetime, optional): Start time filter
            end_time (datetime, optional): End time filter
            confirm (int): Filter confirm
            limit (int): Maximum number of signals to return

        Returns:
            List[Dict]: List of signal documents
        """
        try:
            query = {}

            if subaccount_id:
                query["subaccount_id"] = subaccount_id
            if symbol:
                query["symbol"] = symbol
            if market_type:
                query["market_type"] = market_type
            if timeframe:
                query["timeframe"] = timeframe
            if signal_type:
                query["signal_type"] = signal_type
            if start_time or end_time:
                query["timestamp"] = {}
                if start_time:
                    query["timestamp"]["$gt"] = start_time
                if end_time:
                    query["timestamp"]["$lt"] = end_time
            query["confirm"] = confirm
            logger.info(f"start_time :{start_time} query: {query}")
            return await self.find("signals", query, sort=[("timestamp", 1)], limit=limit)

        except Exception as e:
            logger.error(f"Failed to get signals: {str(e)}")
            raise

    async def get_latest_signal(self, subaccount_id: str, symbol: str, 
                               market_type: str, timeframe: str) -> Optional[Dict[str, Any]]:
        """
        Get the latest signal for a specific configuration.

        Args:
            subaccount_id (str): Subaccount identifier
            symbol (str): Trading pair
            market_type (str): Market type
            timeframe (str): Timeframe

        Returns:
            Optional[Dict]: Latest signal document or None
        """
        try:
            query = {
                "subaccount_id": subaccount_id,
                "symbol": symbol,
                "market_type": market_type,
                "timeframe": timeframe
            }
            
            return await self.find_one("signals", query)
            
        except Exception as e:
            logger.error(f"Failed to get latest signal for {subaccount_id}/{symbol}/{market_type}/{timeframe}: {str(e)}")
            raise

    async def save_metrics(self, subaccount_id: str, symbol: str, market_type: str, timeframe: str, metrics: Dict[str, float], timestamp: datetime = None) -> None:
        """
        Save analytics metrics to the analytics_metrics collection.

        Args:
            subaccount_id (str): Subaccount identifier
            symbol (str): Trading pair
            market_type (str): Market type
            timeframe (str): Timeframe
            metrics (dict): Analytics metrics (e.g., base_win_rate, sharpe_ratio, net_profit)
            timestamp (datetime, optional): Timestamp for the metrics, defaults to current time in UTC+8

        Raises:
            Exception: If saving metrics fails after retries
        """
        for attempt in range(self.MAX_RETRIES):
            try:
                lock = asyncio.Lock()
                async with lock:
                    await self._validate_subaccount(subaccount_id)
                    document = {
                        "subaccount_id": subaccount_id,
                        "symbol": symbol,
                        "market_type": market_type,
                        "timeframe": timeframe,
                        "metrics": metrics,
                        "timestamp": timestamp or datetime.now(timezone(timedelta(hours=8)))
                    }
                    await self.db['analytics_metrics'].update_one(
                        {
                            "subaccount_id": subaccount_id,
                            "symbol": symbol,
                            "market_type": market_type,
                            "timeframe": timeframe,
                            "timestamp": document["timestamp"]
                        },
                        {"$set": document},
                        upsert=True
                    )
                    logger.info(f"Saved metrics for {subaccount_id}/{symbol}/{market_type}/{timeframe}")
                    return
            except Exception as e:
                logger.error(f"Save metrics attempt {attempt + 1} failed: {str(e)}")
                if attempt < self.MAX_RETRIES - 1:
                    await asyncio.sleep(self.RETRY_DELAY)
                    self.RETRY_DELAY *= 2
                else:
                    raise

    async def get_metrics(self, subaccount_id: str = None, symbol: str = None, 
                         market_type: str = None, timeframe: str = None,
                         start_time: datetime = None, end_time: datetime = None, 
                         limit: int = 100) -> List[Dict[str, Any]]:
        """
        Retrieve analytics metrics from the analytics_metrics collection.

        Args:
            subaccount_id (str, optional): Filter by subaccount
            symbol (str, optional): Filter by symbol
            market_type (str, optional): Filter by market type
            timeframe (str, optional): Filter by timeframe
            start_time (datetime, optional): Start time filter
            end_time (datetime, optional): End time filter
            limit (int): Maximum number of metrics to return

        Returns:
            List[Dict]: List of metrics documents
        """
        try:
            query = {}
            if subaccount_id:
                query["subaccount_id"] = subaccount_id
            if symbol:
                query["symbol"] = symbol
            if market_type:
                query["market_type"] = market_type
            if timeframe:
                query["timeframe"] = timeframe
            if start_time or end_time:
                query["timestamp"] = {}
                if start_time:
                    query["timestamp"]["$gte"] = start_time
                if end_time:
                    query["timestamp"]["$lte"] = end_time
            
            return await self.find("analytics_metrics", query, sort=[("timestamp", -1)], limit=limit)
            
        except Exception as e:
            logger.error(f"Failed to get metrics: {str(e)}")
            raise

    async def close(self):
        """Close the database connection."""
        async with self._lock:
            if hasattr(self, "client") and self.client is not None:
                try:
                    await self.client.close()
                    # Use safe logging function
                    from src.core.logging_config import safe_log
                    safe_log(logger.info, "Database connection closed")
                except ImportError:
                    # Python is shutting down, modules may be unloaded, handle silently
                    pass
                except Exception as e:
                    # Use safe logging function
                    from src.core.logging_config import safe_log
                    safe_log(logger.error, f"Failed to close database connection: {str(e)}")
            else:
                from src.core.logging_config import safe_log
                safe_log(logger.error, f"Failed to close database connection: {str(e)}")

    def __del__(self):
        """Ensure connection is closed when object is deleted."""
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.create_task(self.close())
            else:
                asyncio.run(self.close())
        except Exception:
            # All exceptions are silently ignored during Python interpreter shutdown
            pass