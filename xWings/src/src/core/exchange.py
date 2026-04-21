# File: src/core/exchange.py
# Purpose: Abstract interface for exchange interactions in xWings trading system
# Notes: Timestamps in UTC+8. Supports OKX and Binance.US with extensible error handling
#        and hot-reloading of exchange plugins. Enhanced with thread safety and async optimization.
#        Added fetch_position and cancel_open_orders for FundsManager integration.
# Updated: 2025-08-05 20:13 PDT

import sys
import os
import asyncio
import threading
import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional
from time import time
from datetime import datetime, timezone, timedelta
from functools import wraps
from pathlib import Path
import importlib
import ccxt.async_support as ccxt

# Set working directory to project root and ensure paths
PROJECT_ROOT = Path(__file__).resolve().parents[2]
os.chdir(PROJECT_ROOT)
SRC_PATH = PROJECT_ROOT / 'src'
CORE_PATH = SRC_PATH / 'core'
for path in [PROJECT_ROOT, SRC_PATH, CORE_PATH]:
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)
        logging.debug(f"Added {path} to sys.path")

from core.utils import PathManager
from core.database import Database
from core.config_manager import ConfigManager

logger = logging.getLogger(__name__)

class ExchangeError(Exception):
    """Custom exception for exchange errors with standardized codes."""
    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")

class RateLimiter:
    def __init__(self, calls: int, period: float):
        self.calls = calls
        self.period = period
        self.timestamps = []
        self.lock = asyncio.Lock()

    def __call__(self, func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            async with self.lock:
                current = time()
                self.timestamps = [t for t in self.timestamps if current - t < self.period]
                if len(self.timestamps) >= self.calls:
                    await asyncio.sleep(self.period - (current - self.timestamps[0]))
                self.timestamps.append(time())
            return await func(*args, **kwargs)
        return wrapper


class ExchangeAPI(ABC):
    def __init__(self, api_key: str, api_secret: str, passphrase: str = None):
        self.lock = threading.Lock()
        self.client = None
        self.db_manager: Optional[Database] = None
        self.exchange_name = None
        self.rate_limiter = None
        self.credentials = {}
        path_manager = PathManager(PROJECT_ROOT)
        config_path = path_manager.get_config_path('encryption.key')
        if not config_path.exists():
            logger.error(f"Encryption key not found at {config_path}")
            raise FileNotFoundError(f"Encryption key not found at {config_path}")
        self.config_manager = ConfigManager(key_file=str(config_path))
        if not passphrase and self.exchange_name == 'okx':
            logger.warning("Passphrase missing for OKX initialization")
        logger.debug(f"ExchangeAPI initialized for {self.exchange_name}")

    def set_db_manager(self, db_manager: Database):
        with self.lock:
            self.db_manager = db_manager
            logger.info(f"Database manager set for {self.exchange_name}")

    def get_current_account_name(self):
        pass

    @RateLimiter(calls=10, period=1.0)
    @abstractmethod
    async def fetch_ohlcv(self, symbol: str, timeframe: str, market_type: str, since: int = None, limit: int = None) -> List[List[float]]:
        pass

    @RateLimiter(calls=5, period=1.0)
    @abstractmethod
    async def place_order(self, subaccount_id: str, symbol: str, market_type: str, side: str, amount: float, price: Optional[float], client_order_id: str = None) -> Dict:
        pass

    @RateLimiter(calls=3, period=1.0)
    @abstractmethod
    async def place_batch_orders(self, subaccount_id: str, orders: List[Dict], is_protection: bool = False) -> List[Dict]:
        pass

    @RateLimiter(calls=5, period=1.0)
    @abstractmethod
    async def place_protection_order(self, subaccount_id: str, symbol: str, market_type: str, side: str, quantity: float, stop_price: float, client_order_id: str) -> Dict:
        pass

    @RateLimiter(calls=2, period=1.0)
    @abstractmethod
    async def set_leverage(self, symbol: str, leverage: int, margin_mode: str = 'isolated') -> None:
        pass

    @RateLimiter(calls=10, period=1.0)
    @abstractmethod
    async def fetch_ticker(self, symbol: str, params: Dict = {}) -> Dict:
        pass

    @RateLimiter(calls=5, period=1.0)
    @abstractmethod
    async def get_instrument_info(self, symbol: str, market_type: str) -> Dict:
        pass

    @RateLimiter(calls=5, period=1.0)
    @abstractmethod
    async def fetch_balance(self, subaccount_id: str, asset: str = 'USDT') -> Dict:
        pass

    @RateLimiter(calls=5, period=1.0)
    @abstractmethod
    async def fetch_trading_fees(self, symbol: str, params: Dict = {}) -> Dict:
        pass

    @RateLimiter(calls=5, period=1.0)
    @abstractmethod
    async def fetch_asset_valuation(self, subaccount_id: str) -> Dict:
        pass

    @RateLimiter(calls=5, period=1.0)
    @abstractmethod
    async def fetch_position(self, subaccount_id: str, symbol: str, market_type: str) -> Dict:
        pass

    @RateLimiter(calls=5, period=1.0)
    @abstractmethod
    async def cancel_open_orders(self, subaccount_id: str, symbol: str, market_type: str) -> None:
        pass

    @RateLimiter(calls=5, period=1.0)
    @abstractmethod
    async def get_position_mode(self, subaccount_id: str) -> str:
        pass

    @RateLimiter(calls=5, period=1.0)
    @abstractmethod
    async def set_position_mode(self, subaccount_id: str, mode: str = 'net_mode') -> None:
        pass

    async def place_order_execution(self, subaccount_id: str, symbol: str, market_type: str, side: str, amount: float, price: float, timeframe: str, is_protection: bool = False, split_index: int = None) -> Dict:
        """
        Execute order with validation and logging.

        Args:
            subaccount_id (str): Subaccount identifier
            symbol (str): Trading pair symbol
            market_type (str): Market type (spot, perpetual)
            side (str): Order side (buy/sell)
            amount (float): Order quantity
            price (float): Order price
            timeframe (str): Timeframe for trading
            is_protection (bool): Whether this is a protection order
            split_index (int, optional): Index for split orders

        Returns:
            Dict: Order details
        """
        async with asyncio.Lock():
            try:
                credentials = self.config_manager.get_config('client_credentials_encrypted')
                subaccount_config = self._find_subaccount(credentials.get('subaccounts', {}), subaccount_id)
                if not subaccount_config:
                    logger.error(f"Subaccount {subaccount_id} not found in client_credentials_encrypted.yaml for {self.exchange_name}")
                    raise ExchangeError("INVALID_SUBACCOUNT", f"Subaccount {subaccount_id} not found")
                trading_config = subaccount_config.get('trading_config', {}).get('symbols', {}).get(symbol, {})
                if not trading_config and market_type == 'perpetual':
                    logger.error(f"No trading_config for {subaccount_id}/{symbol}/{market_type}")
                    raise ExchangeError("INVALID_CONFIG", f"No trading_config for {subaccount_id}/{symbol}/{market_type}")
                if timeframe not in trading_config.get('timeframes', ['3m', '15m', '30m']):
                    logger.error(f"Timeframe {timeframe} not allowed for {subaccount_id}/{symbol}")
                    raise ExchangeError("INVALID_TIMEFRAME", f"Timeframe {timeframe} not allowed")
                leverage = trading_config.get('leverage', 1) if market_type == 'perpetual' else 1
                client_order_id = f"{subaccount_id}{int(time())}".replace(".", "") + (f"{split_index}" if split_index is not None else "")
                order = await self.place_protection_order(subaccount_id, symbol, market_type, side, amount, price, client_order_id) if is_protection else await self.place_order(subaccount_id, symbol, market_type, side, amount, price, client_order_id)
                if order.get('status') == 'open' and self.db_manager:
                    trade_document = {
                        'subaccount_id': subaccount_id,
                        'exchange': self.exchange_name,
                        'symbol': symbol,
                        'market_type': market_type,
                        'side': side,
                        'price': price,
                        'quantity': amount,
                        'leverage': leverage,
                        'timeframe': timeframe,
                        'order_id': order['id'],
                        'timestamp': datetime.now(timezone(timedelta(hours=8))).isoformat(),
                        'is_protection': is_protection,
                        'split_index': split_index
                    }
                    await self.db_manager.save_trade(**trade_document)
                logger.info(f"Executed {'protection' if is_protection else 'main'} order for {subaccount_id}/{symbol}/{market_type}: order={order}")
                return order
            except ExchangeError as e:
                logger.error(f"Exchange-specific error {e.code}: {e.message} for {subaccount_id}/{symbol}/{market_type}")
                raise
            except Exception as e:
                error_code = getattr(e, 'code', 'UNKNOWN_ERROR')
                logger.error(f"Order execution failed for {subaccount_id}/{symbol}/{market_type}: {str(e)}")
                raise ExchangeError(error_code, str(e))

    def _find_subaccount(self, subaccounts: Dict, subaccount_id: str) -> Optional[Dict]:
        """
        Recursively find a subaccount by ID in the configuration.

        Args:
            subaccounts (Dict): Subaccount configuration dictionary
            subaccount_id (str): Subaccount identifier

        Returns:
            Optional[Dict]: Subaccount configuration or None
        """
        parts = subaccount_id.split(".")
        current = subaccounts
        for part in parts:
            found = None
            for sid, details in current.items():
                if sid == part or details.get("login_name") == part:
                    found = details
                    break
            if not found:
                return None
            current = found.get("subaccounts", {})
        return found

    @classmethod
    def get_exchange(cls, exchange_name: str, subaccount_id: str) -> 'ExchangeAPI':
        """
        Factory method to load exchange implementations.

        Args:
            exchange_name (str): Name of the exchange (e.g., 'binance', 'okx')
            subaccount_id (str): Subaccount identifier

        Returns:
            ExchangeAPI: Exchange instance
        """
        try:
            # Import the corresponding exchange module
            module = importlib.import_module(f"core.exchanges.{exchange_name.lower()}")
            # this will import okx or binance
            exchange_class = getattr(module, f"{exchange_name.title()}Exchange")
            # Initialize exchange instance with subaccount info
            return exchange_class(subaccount_id)
        except (ImportError, AttributeError) as e:
            logger.error(f"Failed to load exchange {exchange_name}: {str(e)}")
            raise ExchangeError("INVALID_EXCHANGE", f"Exchange {exchange_name} not supported")

    @classmethod
    async def reload_exchange(cls, exchange_name: str, subaccount_id: str, current_instance: Optional['ExchangeAPI'] = None) -> 'ExchangeAPI':
        """
        Reload exchange plugin dynamically.

        Args:
            exchange_name (str): Name of the exchange
            subaccount_id (str): Subaccount identifier
            current_instance (Optional[ExchangeAPI]): Current exchange instance to reload

        Returns:
            ExchangeAPI: Reloaded exchange instance
        """
        async with asyncio.Lock():
            try:
                if current_instance and hasattr(current_instance, 'stop_ws'):
                    await current_instance.stop_ws()
                    logger.info(f"Stopped WebSocket for {exchange_name}")
                module_name = f"core.exchanges.{exchange_name.lower()}"
                module = sys.modules.get(module_name)
                if module:
                    importlib.reload(module)
                else:
                    module = importlib.import_module(module_name)
                exchange_class = getattr(module, f"{exchange_name.title()}Exchange")
                new_instance = exchange_class(subaccount_id)
                logger.info(f"Reloaded {exchange_name} plugin for {subaccount_id} at {datetime.now(timezone(timedelta(hours=8))).isoformat()}")
                return new_instance
            except (ImportError, AttributeError) as e:
                logger.error(f"Failed to reload {exchange_name}: {str(e)}")
                raise ExchangeError("RELOAD_FAILED", f"Failed to reload {exchange_name}: {str(e)}")

    async def close(self):
        """
        Close exchange connections.
        """
        try:
            if self.client:
                await self.client.close()
                logger.debug(f"Closed exchange connection for {self.exchange_name}")
        except Exception as e:
            logger.error(f"Failed to close exchange connection for {self.exchange_name}: {str(e)}")