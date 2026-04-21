# File: src/tools/ohlc_collector.py
# Purpose: Export OHLCV data from MongoDB to CSV format for backtesting and testing with daily incremental updates
# Dependencies: pandas, core.database, core.config_manager, core.logging_config, core.utils, core.exchanges
# Notes: Uses UTC+8 timestamps for consistency with signal_generator.py.
#        Exports data for specified exchange, symbol, market type, and timeframe.
#        Supports optional --collect flag to fetch OHLCV data from exchanges and store in MongoDB.
#        Runs daily at 00:00 UTC+8 for incremental updates, using global logging configuration.
# Updated: 2025-08-05

import sys
import logging
import asyncio
import pandas as pd
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, List
import argparse
import json
from pathlib import Path
from pymongo.errors import ServerSelectionTimeoutError, OperationFailure, ConfigurationError
import traceback
import os

# Set working directory to project root
os.chdir(Path(__file__).resolve().parents[2])

# Define project root and paths
PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_PATH = PROJECT_ROOT / 'src'
CORE_PATH = SRC_PATH / 'core'
CONFIG_PATH = SRC_PATH / 'config'
DATA_PROCESSED_PATH = PROJECT_ROOT / 'data' / 'processed'

# Adjust sys.path
for path in [PROJECT_ROOT, SRC_PATH, CORE_PATH, CONFIG_PATH]:
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

# Verify paths
for path in [PROJECT_ROOT, SRC_PATH, CORE_PATH, CONFIG_PATH]:
    if not path.exists():
        logging.error(f"Directory not found: {path}")
        raise FileNotFoundError(f"Directory not found: {path}")

try:
    from core.database import Database
    from core.config_manager import ConfigManager
    from core.utils import PathManager
    from core.exchanges.okx import OkxExchange
    from core.exchanges.binance import BinanceExchange
except ImportError as e:
    logging.error(f"Failed to import core modules: {str(e)}. Ensure src/core/ contains __init__.py")
    raise

# Ensure data directory exists
DATA_PROCESSED_PATH.mkdir(parents=True, exist_ok=True)

# Initialize logger
logger = logging.getLogger(__name__)
logger.debug(f"OHLCCollector logging initialized with project_root: {PROJECT_ROOT}")

parser = argparse.ArgumentParser(description="Export OHLC data from MongoDB or collect from exchanges for testing and backtesting.")
parser.add_argument('--exchange', type=str, default=None,
                    help="Specific exchange to collect/export data for (e.g., okx). Default: all enabled exchanges in markets_encrypted.yaml")
parser.add_argument('--since', type=str, default=None,
                    help="Start time for data collection/export (ISO format, e.g., '2025-01-01T00:00:00+08:00'). Default: last export time or 30 days ago")
parser.add_argument('--debug', action='store_true',
                    help="Enable debug mode to log detailed configuration and query information")
parser.add_argument('--collect', action='store_true',
                    help="Collect OHLCV data from exchanges and store in MongoDB before exporting")
args = parser.parse_args()

# Set logging level based on debug flag
if args.debug:
    logging.getLogger().setLevel(logging.DEBUG)

def load_start_time() -> datetime:
    """
    Load last export time from start_time.json or return default (30 days ago).

    Returns:
        datetime: Start time in UTC+8
    """
    start_time_file = CONFIG_PATH / 'start_time.json'
    try:
        if start_time_file.exists():
            with start_time_file.open('r') as f:
                data = json.load(f)
                start_time_str = data.get('start_time')
                if start_time_str:
                    start_time = datetime.fromisoformat(start_time_str.replace('Z', '+08:00')).replace(tzinfo=timezone(timedelta(hours=8)))
                    logger.info(f"Loaded start_time from {start_time_file}: {start_time}")
                    return start_time
        logger.warning(f"start_time.json not found or invalid, using default (30 days ago)")
    except (json.JSONDecodeError, ValueError) as e:
        logger.error(f"Error parsing start_time.json: {str(e)}, stack={traceback.format_exc()}")
    return datetime.now(timezone(timedelta(hours=8))) - timedelta(days=30)

def save_start_time(start_time: datetime) -> None:
    """
    Save current export time to start_time.json for incremental updates.

    Args:
        start_time (datetime): Current export time in UTC+8
    """
    start_time_file = CONFIG_PATH / 'start_time.json'
    try:
        with start_time_file.open('w') as f:
            json.dump({'start_time': start_time.isoformat()}, f)
        logger.info(f"Saved start_time to {start_time_file}: {start_time}")
    except Exception as e:
        logger.error(f"Failed to save start_time to {start_time_file}: {str(e)}, stack={traceback.format_exc()}")

# Read start_time from configuration or arguments
path_manager = PathManager(PROJECT_ROOT)
config_manager = ConfigManager(key_file=str(path_manager.get_config_path('encryption.key')))
params = config_manager.get_config('parameters_encrypted')
db_config = params.get('database')
start_time_config = params.get('data', {}).get('start_time', 'dynamic')
if args.since:
    try:
        start_time = datetime.fromisoformat(args.since.replace('Z', '+08:00')).replace(tzinfo=timezone(timedelta(hours=8)))
        logger.info(f"Using start_time from arguments: {start_time}")
    except ValueError as e:
        logger.error(f"Invalid --since argument: {str(e)}, using start_time.json, stack={traceback.format_exc()}")
        start_time = load_start_time()
elif start_time_config == 'dynamic':
    start_time = load_start_time()
else:
    try:
        start_time = datetime.fromisoformat(start_time_config.replace('Z', '+08:00')).replace(tzinfo=timezone(timedelta(hours=8)))
        logger.info(f"Using start_time from config: {start_time}")
    except (ValueError, AttributeError):
        logger.error(f"Invalid start_time in config: {start_time_config}, using start_time.json, stack={traceback.format_exc()}")
        start_time = load_start_time()
logger.info(f"OHLC data collection/export start time (UTC+8): {start_time}")

async def collect_ohlc_from_exchange(db: Database, exchange_name: str, symbol: str, market_type: str, timeframe: str, since: datetime, subaccount_id: str, retries: int = 3) -> None:
    """
    Collect OHLCV data from exchange and store in MongoDB.

    Args:
        db (Database): Database instance
        exchange_name (str): Exchange name (e.g., okx, binance)
        symbol (str): Trading pair symbol
        market_type (str): Market type (spot, perpetual)
        timeframe (str): Timeframe (e.g., '5m', '15m')
        since (datetime): Start time for data collection
        subaccount_id (str): Subaccount identifier
        retries (int): Number of retries for data collection
    """
    for attempt in range(retries):
        try:
            exchange_class = BinanceExchange if exchange_name == 'binance' else OkxExchange
            exchange = exchange_class(subaccount_id)
            logger.debug(f"Initialized {exchange_name} exchange for {subaccount_id}")
            since_ms = int(since.timestamp() * 1000)
            limit = min(params.get('data', {}).get('max_data_points', 60), 1000)
            ohlcv = await exchange.fetch_ohlcv(symbol, timeframe, market_type, since=since_ms, limit=limit)
            if not ohlcv:
                logger.warning(f"No OHLCV data fetched for {exchange_name}/{symbol}/{market_type}/{timeframe}")
                return
            ohlcv_data = [
                {
                    "exchange": exchange_name,
                    "symbol": symbol,
                    "market_type": market_type,
                    "timeframe": timeframe,
                    "timestamp": datetime.fromtimestamp(candle[0] / 1000, tz=timezone(timedelta(hours=8))),
                    "open": float(candle[1]),
                    "high": float(candle[2]),
                    "low": float(candle[3]),
                    "close": float(candle[4]),
                    "volume": float(candle[5])
                } for candle in ohlcv
            ]
            await db.save_ohlcv_batch(ohlcv_data)
            logger.info(f"Stored {len(ohlcv_data)} OHLCV records for {exchange_name}/{symbol}/{market_type}/{timeframe}")
            return
        except Exception as e:
            logger.error(f"Failed to collect OHLCV data for {exchange_name}/{symbol}/{market_type}/{timeframe}: {str(e)}, attempt {attempt + 1}/{retries}, stack={traceback.format_exc()}")
            if attempt < retries - 1:
                await asyncio.sleep(2 ** attempt)
    logger.error(f"Failed to collect OHLCV data for {exchange_name}/{symbol}/{market_type}/{timeframe} after {retries} attempts")

async def get_ohlc_data(db: Database, exchange: str, symbol: str, market_type: str, timeframe: str, since: datetime, retries: int = 3) -> List[Dict]:
    """
    Retrieve OHLC data from MongoDB for a given exchange, symbol, market type, and timeframe.

    Args:
        db (Database): Database instance
        exchange (str): Exchange name (e.g., okx, binance)
        symbol (str): Trading pair symbol
        market_type (str): Market type (spot, perpetual)
        timeframe (str): Timeframe (e.g., '5m', '15m')
        since (datetime): Start time for data collection
        retries (int): Number of retries for database query

    Returns:
        List[Dict]: OHLCV data
    """
    for attempt in range(retries):
        try:
            query = {
                "exchange": exchange,
                "symbol": symbol,
                "market_type": market_type,
                "timeframe": timeframe,
                "timestamp": {"$gte": since}
            }
            logger.debug(f"Executing query for {exchange}/{symbol}/{market_type}/{timeframe}: {query}")
            data = await asyncio.wait_for(db.find("ohlcv", query, sort=[("timestamp", 1)]), timeout=30.0)
            if not data:
                logger.warning(f"No OHLC data found for {exchange}/{symbol}/{market_type}/{timeframe} since {since}")
                return []
            ohlcv_data = [
                {
                    "timestamp": int(row["timestamp"].timestamp() * 1000),
                    "open": row["open"],
                    "high": row["high"],
                    "low": row["low"],
                    "close": row["close"],
                    "volume": row.get("volume", 0.0),
                    "signal": row.get("signal"),
                    "trade_price": row.get("trade_price")
                } for row in data
            ]
            logger.info(f"Fetched {len(ohlcv_data)} OHLC records for {exchange}/{symbol}/{market_type}/{timeframe} from database")
            return ohlcv_data
        except asyncio.TimeoutError:
            logger.error(f"Query timeout for {exchange}/{symbol}/{market_type}/{timeframe}, attempt {attempt + 1}/{retries}, stack={traceback.format_exc()}")
            if attempt < retries - 1:
                await asyncio.sleep(2 ** attempt)
        except OperationFailure as e:
            logger.error(f"MongoDB operation failed for {exchange}/{symbol}/{market_type}/{timeframe}: {str(e)}, attempt {attempt + 1}/{retries}, stack={traceback.format_exc()}")
            if attempt < retries - 1:
                await asyncio.sleep(2 ** attempt)
        except Exception as e:
            logger.error(f"Failed to fetch OHLC data for {exchange}/{symbol}/{market_type}/{timeframe}: {str(e)}, attempt {attempt + 1}/{retries}, stack={traceback.format_exc()}")
            if attempt < retries - 1:
                await asyncio.sleep(2 ** attempt)
    logger.error(f"Failed to fetch OHLC data for {exchange}/{symbol}/{market_type}/{timeframe} after {retries} attempts")
    return []

def store_ohlc_to_files(ohlcv_data: List[Dict], exchange: str, symbol: str, market_type: str, timeframe: str) -> None:
    """
    Store OHLCV data to data/processed (CSV) for testing.

    Args:
        ohlcv_data (List[Dict]): List of OHLCV data
        exchange (str): Exchange name (e.g., okx, binance)
        symbol (str): Trading pair symbol
        market_type (str): Market type (spot, perpetual)
        timeframe (str): Timeframe (e.g., '5m', '15m')
    """
    try:
        if not ohlcv_data:
            logger.warning(f"No OHLC data to store for {exchange}/{symbol}/{market_type}/{timeframe}")
            return
        safe_symbol = symbol.replace('/', '_')
        timestamp = datetime.now(timezone(timedelta(hours=8))).strftime('%Y%m%d_%H%M%S')
        # Store processed data as CSV
        df = pd.DataFrame(ohlcv_data)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('Asia/Shanghai')
        
        # Save as CSV
        csv_file = DATA_PROCESSED_PATH / f"ohlc_{exchange}_{safe_symbol}_{market_type}_{timeframe}_{timestamp}.csv"
        df.to_csv(csv_file, index=False)
        logger.info(f"Saved processed OHLC data to {csv_file}")

        # Save as Parquet
        parquet_file = DATA_PROCESSED_PATH / f"ohlc_{exchange}_{safe_symbol}_{market_type}_{timeframe}_{timestamp}.parquet"
        df.to_parquet(parquet_file, engine='pyarrow', index=False)
        logger.info(f"Saved processed OHLC data to {parquet_file}")
    except Exception as e:
        logger.error(f"Failed to store OHLC data for {exchange}/{symbol}/{market_type}/{timeframe}: {str(e)}, stack={traceback.format_exc()}")
        raise

async def check_ohlc_data(db: Database, retries: int = 3) -> bool:
    """
    Check if OHLC data exists in MongoDB.

    Args:
        db (Database): Database instance
        retries (int): Number of retries for database query

    Returns:
        bool: True if data exists, False otherwise
    """
    for attempt in range(retries):
        try:
            count = await asyncio.wait_for(db.db['ohlcv'].count_documents({}), timeout=10.0)
            logger.info(f"Found {count} records in ohlcv collection")
            if count == 0:
                logger.warning("No data found in ohlcv collection. Ensure data is populated.")
                return False
            return True
        except asyncio.TimeoutError:
            logger.error(f"Timeout checking ohlcv collection, attempt {attempt + 1}/{retries}, stack={traceback.format_exc()}")
            if attempt < retries - 1:
                await asyncio.sleep(2 ** attempt)
        except Exception as e:
            logger.error(f"Failed to check ohlcv collection: {str(e)}, attempt {attempt + 1}/{retries}, stack={traceback.format_exc()}")
            if attempt < retries - 1:
                await asyncio.sleep(2 ** attempt)
    logger.error(f"Failed to check ohlcv collection after {retries} attempts")
    return False

async def validate_exchange_configs(exchanges: Dict, params: Dict, credentials: Dict) -> bool:
    """
    Validate exchange configurations against parameters and credentials.

    Args:
        exchanges (Dict): Exchange configurations from markets_encrypted.yaml
        params (Dict): Parameters configuration
        credentials (Dict): Client credentials configuration

    Returns:
        bool: True if valid, False otherwise
    """
    try:
        if not exchanges:
            logger.error("Exchange configurations are empty")
            return False
        allowed_symbols = params.get('trading', {}).get('allowed_symbols', ['BTC/USDT', 'ETH/USDT', 'SOL/USDT'])
        allowed_market_types = params.get('trading', {}).get('allowed_market_types', ['spot', 'perpetual'])
        allowed_timeframes = params.get('trading', {}).get('timeframe', ['5m', '15m', '30m'])
        for exchange, developments in exchanges.items():
            if not developments.get('enabled', False):
                logger.info(f"Exchange {exchange} is disabled, skipping")
                continue
            symbols = developments.get('markets', {}).get('crypto', {}).get('spot', []) + developments.get('markets', {}).get('crypto', {}).get('perpetual', [])
            if not symbols:
                logger.warning(f"No symbols defined for {exchange}, using default: {allowed_symbols}")
                developments['symbols'] = allowed_symbols
            else:
                developments['symbols'] = list(set(symbols))  # Deduplicate symbols
            for symbol in developments['symbols']:
                if symbol not in allowed_symbols:
                    logger.error(f"Symbol {symbol} not in allowed_symbols for {exchange}")
                    return False
            if not developments.get('timeframes', []):
                logger.warning(f"No timeframes defined for {exchange}, using default: {allowed_timeframes}")
                developments['timeframes'] = allowed_timeframes
            # Validate subaccounts
            subaccounts = credentials.get('subaccounts', {}).get(exchange, {})
            if not subaccounts:
                logger.error(f"No subaccounts defined for {exchange} in client_credentials_encrypted.yaml")
                return False
        logger.debug(f"Validated exchange configurations: {exchanges}")
        return True
    except Exception as e:
        logger.error(f"Exchange configuration validation failed: {str(e)}, stack={traceback.format_exc()}")
        return False

async def main():
    """
    Main function to collect (optional) and export OHLC data from MongoDB for all enabled exchanges or a specific exchange.
    """
    try:
        config_manager = ConfigManager(key_file=str(path_manager.get_config_path('encryption.key')))
        params = config_manager.get_config('parameters_encrypted')
        credentials = config_manager.get_config('client_credentials_encrypted')
        db_config = params.get('database')
        required_keys = ['host', 'port', 'database', 'user', 'password']
        missing_keys = [key for key in required_keys if key not in db_config or not db_config[key]]
        if missing_keys:
            logger.error(f"Missing database configuration keys: {missing_keys}")
            raise ConfigurationError(f"Missing database configuration keys: {missing_keys}")
        markets = config_manager.get_config('markets_encrypted')
        logger.debug(f"Loaded markets configuration: {markets}")
        max_retries = params.get('data', {}).get('retries', 3)

        for attempt in range(max_retries):
            try:
                db = Database(db_config)
                logger.info(f"Successfully connected to MongoDB: {db_config.get('database')}")
                break
            except ServerSelectionTimeoutError as e:
                logger.error(f"MongoDB connection attempt {attempt + 1} failed: {str(e)}, stack={traceback.format_exc()}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                else:
                    raise

        # Get exchanges, symbols, and timeframes from markets_encrypted.yaml
        exchanges = markets.get('exchanges', {})
        if args.exchange:
            exchanges = {k: v for k, v in exchanges.items() if k == args.exchange}
            if not exchanges:
                logger.error(f"Exchange {args.exchange} not found in markets_encrypted.yaml")
                db.close()
                return

        logger.info(f"Processing exchanges: {list(exchanges.keys())}")
        if not await validate_exchange_configs(exchanges, params, credentials):
            logger.error("Invalid exchange configurations, exiting")
            db.close()
            return

        # Collect data from exchanges if --collect flag is set
        if args.collect:
            market_types = params.get('trading', {}).get('allowed_market_types', ['spot', 'perpetual'])
            collect_tasks = []
            for exchange, developments in exchanges.items():
                if not developments.get('enabled', False):
                    logger.info(f"Exchange {exchange} is disabled, skipping")
                    continue
                symbols = developments.get('symbols', params.get('trading', {}).get('allowed_symbols', ['BTC/USDT', 'ETH/USDT', 'SOL/USDT']))
                timeframes = developments.get('timeframes', params.get('trading', {}).get('timeframe', ['5m', '15m', '30m']))
                subaccounts = credentials.get('subaccounts', {}).get(exchange, {})
                if not subaccounts:
                    logger.error(f"No subaccounts defined for {exchange}, skipping collection")
                    continue
                subaccount_id = list(subaccounts.keys())[0]  # Use first subaccount
                for symbol in symbols:
                    for market_type in market_types:
                        for timeframe in timeframes:
                            collect_tasks.append(collect_ohlc_from_exchange(db, exchange, symbol, market_type, timeframe, start_time, subaccount_id, retries=max_retries))
            if collect_tasks:
                logger.debug(f"Created {len(collect_tasks)} tasks for OHLC data collection")
                await asyncio.gather(*collect_tasks, return_exceptions=True)
            else:
                logger.warning("No collection tasks created. Check subaccount configurations.")

        # Check ohlcv collection
        if not await check_ohlc_data(db, retries=max_retries):
            logger.error("No OHLC data available in collection 'ohlcv'. Please populate data and retry.")
            db.close()
            return

        # Export data from MongoDB
        market_types = params.get('trading', {}).get('allowed_market_types', ['spot', 'perpetual'])
        tasks = []
        for exchange, developments in exchanges.items():
            if not developments.get('enabled', False):
                logger.info(f"Exchange {exchange} is disabled, skipping")
                continue
            symbols = developments.get('symbols', params.get('trading', {}).get('allowed_symbols', ['BTC/USDT', 'ETH/USDT', 'SOL/USDT']))
            timeframes = developments.get('timeframes', params.get('trading', {}).get('timeframe', ['5m', '15m', '30m']))
            for symbol in symbols:
                for market_type in market_types:
                    for timeframe in timeframes:
                        tasks.append(get_ohlc_data(db, exchange, symbol, market_type, timeframe, start_time, retries=max_retries))

        if not tasks:
            logger.error("No tasks created for OHLC data retrieval. Check exchange configurations in markets_encrypted.yaml.")
            db.close()
            return

        logger.debug(f"Created {len(tasks)} tasks for OHLC data retrieval")
        ohlcv_data_list = await asyncio.gather(*tasks, return_exceptions=True)
        task_configs = [
            (exchange, symbol, market_type, timeframe)
            for exchange, developments in exchanges.items()
            if developments.get('enabled', False)
            for symbol in developments.get('symbols', params.get('trading', {}).get('allowed_symbols', ['BTC/USDT', 'ETH/USDT', 'SOL/USDT']))
            for market_type in market_types
            for timeframe in developments.get('timeframes', params.get('trading', {}).get('timeframe', ['5m', '15m', '30m']))
        ]

        for i, (exchange, symbol, market_type, timeframe) in enumerate(task_configs):
            if isinstance(ohlcv_data_list[i], Exception):
                logger.error(f"Error fetching OHLC data for {exchange}/{symbol}/{market_type}/{timeframe}: {ohlcv_data_list[i]}, stack={traceback.format_exc()}")
                continue
            store_ohlc_to_files(ohlcv_data_list[i], exchange, symbol, market_type, timeframe)

        # Update start_time for next incremental export
        save_start_time(datetime.now(timezone(timedelta(hours=8))))

        db.close()
        logger.info("OHLC data export completed successfully")
    except Exception as e:
        logger.error(f"OHLC export failed: {str(e)}, stack={traceback.format_exc()}")
        db.close()
        raise
    finally:
        # Ensure event loop cleanup
        loop = asyncio.get_event_loop()
        if not loop.is_closed():
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()
            logger.debug("Event loop closed")

async def run_periodically():
    """
    Run OHLC data export daily at 00:00 UTC+8 for incremental updates.
    """
    while True:
        now = datetime.now(timezone(timedelta(hours=8)))
        # Calculate seconds until next 00:00 UTC+8
        next_run = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        seconds_until_next = (next_run - now).total_seconds()
        logger.info(f"Next OHLC export scheduled at {next_run}")
        await asyncio.sleep(seconds_until_next)
        logger.info("Starting daily OHLC data export")
        await main()

if __name__ == "__main__":
    asyncio.run(run_periodically())