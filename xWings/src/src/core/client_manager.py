# File: src/core/client_manager.py
# Purpose: Manage client subaccounts and execute trades across exchanges using timeframe-specific signals
# Dependencies: core.exchange, core.exchanges.binance, core.exchanges.okx, core.utils, core.config_manager, core.funds_manager, core.funds_allocation_manager, core.signal_generator, config.schema, tools.file_monitor
# Notes: Timestamps in UTC+8. Supports OKX and Binance with deterministic split orders and protection orders.
#        Uses client_credentials_encrypted.yaml for subaccount-specific timeframes and leverage.
#        Uses parameters_encrypted.yaml for futures_protection and database config.
#        Enforces fixed fund_allocations and symbol_allocations from database.
# Updated: 2025-08-05 20:00 PDT

import sys
import os
import asyncio
import random
import threading
import logging
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional
from flask import Flask, jsonify
import pandas as pd
import yaml
import traceback
# Set project root and ensure paths
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.extend([str(path) for path in [PROJECT_ROOT, PROJECT_ROOT / 'src', PROJECT_ROOT / 'src/core'] if str(path) not in sys.path])

from src.core.exchange import ExchangeAPI, ExchangeError
from src.core.exchanges.okx import OkxExchange
from src.core.exchanges.binance import BinanceExchange
from src.core.utils import PathManager
from src.core.config_manager import ConfigManager
from src.core.database import Database
from src.core.funds_manager import FundsManager
from src.core.funds_allocation_manager import FundsAllocationManager
from src.core.signal_generator import SignalGenerator
from src.config.schema import AllocationConfig, SymbolAllocationConfig
from src.tools.file_monitor import FileMonitor
from src.core.market_data_center import MarketDataCenter

# Initialize logging
path_manager = PathManager(PROJECT_ROOT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class ClientManager:
    def __init__(self, key_file: str, database: Database, file_monitor: Optional[FileMonitor] = None, exchange_name: str = "okx", markets_config: Optional[Dict] = None):
        """
        Initialize ClientManager with database instance, signal generators, and file monitor.
        Args:
            key_file (str): Path to encryption key
            database (Database): An initialized Database instance
            file_monitor (FileMonitor, optional): FileMonitor instance for configuration updates
            exchange_name (str): Name of the exchange to use (e.g., 'okx', 'binance')
            markets_config (dict, optional): Content of markets.yaml or markets_encrypted.yaml
        """
        self.exchanges: Dict[str, ExchangeAPI] = {}
        self.funds_managers: Dict[str, FundsManager] = {}
        self.funds_allocation_managers: Dict[str, FundsAllocationManager] = {}
        self.signal_generators: Dict[str, SignalGenerator] = {}
        self.subaccounts: Dict[str, Dict] = {}
        self.lock = threading.Lock()
        self.db = database  # Use provided Database instance directly

        # Initialize ConfigManager with key file
        self.config_manager = ConfigManager(key_file=key_file)

        # File monitor handle
        self.file_monitor = file_monitor

        # Load configuration files with error handling
        self.credentials = self.config_manager.get_config('client_credentials_encrypted') or {}
        self.markets = self.config_manager.get_config('markets_encrypted') or {}
        self.params = self.config_manager.get_config('parameters_encrypted') or {}
        if not self.credentials or not self.markets or not self.params:
            logger.error("Failed to load configuration files")
            raise FileNotFoundError("Failed to load configuration files")

        # Validate futures_protection
        if not self.params.get('futures_protection'):
            logger.error("Missing futures_protection in parameters_encrypted.yaml")
            raise ValueError("Missing futures_protection configuration")

        # Start time
        self.start_time = datetime.now(timezone(timedelta(hours=8))).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()

        # Report directory
        reports_dir = path_manager.get_custom_path('reports', 'client_statements')
        reports_dir.mkdir(parents=True, exist_ok=True)
        if not os.access(reports_dir, os.W_OK):
            logger.error(f"Directory {reports_dir} is not writable")
            raise PermissionError(f"Directory {reports_dir} is not writable")

        # self.app = Flask(__name__)
        # self.setup_flask_routes()
        # self._start_flask_with_retry()
        logger.info("ClientManager initialized")

        # Check if exchange is enabled
        if markets_config is not None:
            ex_cfg = markets_config.get('exchanges', {}).get(exchange_name, {})
            if not ex_cfg.get('enabled', True):
                raise RuntimeError(f"Exchange {exchange_name} is not enabled, cannot initialize!")
        # Initialize REST exchange instance (for historical K-line data)
        if exchange_name == "okx":
            self.exchange = OkxExchange(public_only=True)
        elif exchange_name == "binance":
            self.exchange = BinanceExchange(public_only=True)
        else:
            raise NotImplementedError(f"Unsupported exchange: {exchange_name}")
        # Initialize market data center
        self.market_data_center = MarketDataCenter(self.exchange)
        self.ws_market_data_center = None  # WebSocket instance for real-time ticker
        self.public_exchanges = {} # Cache for public exchange instances

    def get_public_exchange(self, exchange_name: str) -> ExchangeAPI:
        """
        Get a public exchange instance without private authentication.
        """
        if exchange_name not in self.public_exchanges:
            if exchange_name == 'okx':
                self.public_exchanges[exchange_name] = OkxExchange(public_only=True)
            elif exchange_name == 'binance':
                self.public_exchanges[exchange_name] = BinanceExchange(public_only=True)
            else:
                raise ValueError(f"Unsupported exchange: {exchange_name}")
        return self.public_exchanges[exchange_name]

    async def load_credentials(self, accounts: List[Dict]):
        """
        Initialize exchange instances and funds managers based on provided accounts list.
        Avoid re-parsing accounts from file.
        """
        self.subaccounts = {acc['account_id']: acc['config'] for acc in accounts}
        
        for acc in accounts:
            account_id = acc['account_id']
            config = acc['config']
            exchange_name = account_id.split('.')[0]
            logger.info(f"确认参数 {exchange_name} {account_id} {config}")
            try:
                if exchange_name == 'okx':
                    exchange_instance = OkxExchange(subaccount_id=account_id, config=config)
                elif exchange_name == 'binance':
                    exchange_instance = BinanceExchange(subaccount_id=account_id, config=config)
                else:
                    logger.warning(f"Unsupported exchange: {exchange_name} for account {account_id}")
                    continue

                exchange_instance.set_db_manager(self.db)
                self.exchanges[account_id] = exchange_instance
                await exchange_instance.set_position_mode(subaccount_id=account_id, mode='net_mode')
                # Initialize funds manager with database instance
                funds_manager = await FundsManager.create(
                    subaccount_id=account_id,
                    exchange=exchange_instance,
                    config_manager=self.config_manager,
                    account_config=config,
                    database=self.db
                )
                self.funds_managers[account_id] = funds_manager
                logger.info(f"Successfully initialized managers for account {account_id}")
            except Exception as e:
                logger.error(f"Failed to initialize client for account {account_id}: {e}")

    async def async_init(self, accounts: List[Dict]):
        """
        Unified async initialization entry point.
        """
        await self.load_credentials(accounts)

    def setup_flask_routes(self):
        """
        Set up Flask routes for API endpoints.
        """
        @self.app.route('/health', methods=['GET'])
        def health():
            try:
                return jsonify({"status": "healthy", "timestamp": datetime.now(timezone(timedelta(hours=8))).isoformat()})
            except Exception as e:
                logger.error(f"Health check failed: {str(e)}")
                return jsonify({"status": "error", "message": str(e)}), 500

    def _start_flask_with_retry(self, max_attempts: int = 3, retry_delay: int = 5):
        """
        Start Flask app with port conflict retry mechanism.

        Args:
            max_attempts (int): Maximum retry attempts
            retry_delay (int): Delay between retries in seconds
        """
        flask_port = self.params.get('flask', {}).get('port', 5000)
        for attempt in range(max_attempts):
            try:
                self.app.run(host='0.0.0.0', port=flask_port, debug=False)
                logger.info(f"Flask app started on port {flask_port}")
                return
            except OSError as e:
                if 'Address already in use' in str(e):
                    logger.warning(f"Port {flask_port} in use, retrying attempt {attempt+1}/{max_attempts} after {retry_delay}s")
                    flask_port += 1
                    asyncio.sleep(retry_delay)
                else:
                    logger.error(f"Failed to start Flask app: {str(e)}")
                    raise
        logger.error(f"Failed to start Flask after {max_attempts} attempts")
        raise OSError("Failed to start Flask due to port conflicts")

    def validate_subaccount(self, subaccount_id: str, exchange_name: str) -> bool:
        """
        Validate subaccount exists and matches exchange.
        Supports nested subaccounts.

        Args:
            subaccount_id (str): Subaccount identifier (e.g., 'okx.main.subaccount1')
            exchange_name (str): Exchange name

        Returns:
            bool: True if valid, False otherwise
        """
        with self.lock:
            # Check subaccount_id format: requires at least exchange and account name
            if not subaccount_id.count('.') >= 1:
                logger.error(f"Invalid subaccount_id format: {subaccount_id}, expected exchange.login_name or exchange.parent.child")
                return False
            
            # Check exchange name match
            parts = subaccount_id.split('.')
            if parts[0].lower() != exchange_name.lower():
                logger.error(f"Exchange name mismatch: {parts[0]} != {exchange_name}")
                return False
            
            # Check if subaccount exists
            if subaccount_id not in self.exchanges:
                logger.error(f"Subaccount {subaccount_id} not found in exchanges")
                return False
            
            # Check exchange instance match
            if self.exchanges[subaccount_id].exchange_name != exchange_name.lower():
                logger.error(f"Exchange instance mismatch for {subaccount_id}")
                return False
            
            return True

    def validate_leverage(self, subaccount_id: str, symbol: str, leverage: float, market_type: str) -> None:
        """
        Validate leverage for perpetual market.

        Args:
            subaccount_id (str): Subaccount identifier
            symbol (str): Trading pair
            leverage (float): Leverage value
            market_type (str): Market type
        """
        if market_type == 'perpetual':
            trading_config = self.get_trading_config(subaccount_id, symbol)
            max_leverage = trading_config.get('leverage', 1)
            if leverage > max_leverage:
                logger.error(f"Leverage {leverage} exceeds max {max_leverage} for {subaccount_id}/{symbol}/{market_type}")
                raise ValueError(f"Leverage {leverage} exceeds max {max_leverage}")

    def get_trading_config(self, subaccount_id: str, symbol: str) -> Dict:
        """
        Retrieve trading configuration for subaccount and symbol.
        Supports nested subaccounts.
        """
        with self.lock:
            try:
                # Parse subaccount_id
                parts = subaccount_id.split('.')
                exchange_name = parts[0]
                
                # Get exchange configuration
                exchange_config = self.credentials.get('subaccounts', {}).get(exchange_name, {})
                logger.info(f"client_manager exchange_config {exchange_config}" )
                logger.info(f"client_manager parts[1:] {parts[1:]}")
                # Recursively find trading config
                config = self._find_trading_config_recursive(exchange_config, parts[1:], symbol)
                
                if not config:
                    logger.warning(f"No trading config found for {subaccount_id}/{symbol}")
                    return {}
                
                return config
                
            except Exception as e:
                logger.error(f"Error getting trading config for {subaccount_id}/{symbol}: {str(e)}")
                return {}

    def _find_trading_config_recursive(self, accounts_config: dict, account_parts: list, symbol: str) -> Dict:
        """
        Recursively find trading configuration.

        Args:
            accounts_config (dict): Account configuration dictionary
            account_parts (list): Account path parts
            symbol (str): Trading symbol
        
        Returns:
            Dict: Trading configuration
        """
        if not account_parts:
            logger.warning("No account parts provided")
            return {}
        
        current_account = account_parts[0]
        remaining_parts = account_parts[1:]
        logger.info(f"client_manager current_account {current_account}")
        logger.info(f"client_manager remaining_parts {remaining_parts}")
        # Find account in current level
        if current_account in accounts_config:
            account_config = accounts_config[current_account]
            logger.info(f"client_manager account_config {account_config}")
            if not remaining_parts:
                # Reached target account, return trading config
                logger.info(f"client_manager trading_config {account_config.get('trading_config', {}).get('symbols', {}).get(symbol, {})}")
                return account_config.get('trading_config', {}).get('symbols', {}).get(symbol, {})
            else:
                # Continue searching in subaccounts
                subaccounts = account_config.get('subaccounts', {})
                logger.info(f"client_manager subaccounts {subaccounts}")
                if subaccounts:
                    return self._find_trading_config_recursive(subaccounts, remaining_parts, symbol)
                else:
                    logger.warning(f"No subaccounts found for {current_account}")
                    return {}
        else:
            logger.warning(f"Account {current_account} not found in accounts config")
            return {}

    async def fetch_balance(self, subaccount_id: str, exchange_name: str) -> Dict:
        """
        Fetch total balance from exchange.

        Args:
            subaccount_id (str): Subaccount identifier
            exchange_name (str): Exchange name

        Returns:
            Dict: Balance details
        """
        if not self.validate_subaccount(subaccount_id, exchange_name):
            logger.error(f"Invalid subaccount {subaccount_id} for {exchange_name}")
            raise ValueError(f"Invalid subaccount {subaccount_id}")
        exchange = self.exchanges.get(subaccount_id)
        if not exchange:
            logger.error(f"No exchange for subaccount {subaccount_id}")
            raise ValueError(f"No exchange for subaccount {subaccount_id}")
        try:
            balance = await exchange.fetch_balance(subaccount_id, 'USDT')
            logger.debug(f"Fetched balance for {subaccount_id}: {balance}")
            return balance
        except ExchangeError as e:
            logger.error(f"Fetch balance failed for {subaccount_id}: {e.message}")
            raise

    def _calculate_protection_price(self, leverage: float, signal: str, entry_price: float) -> float:
        """
        Calculate protection price based on leverage and signal for liquidation protection.

        Args:
            leverage (float): Leverage value
            signal (str): Trading signal (buy/sell)
            entry_price (float): Entry price

        Returns:
            float: Protection price
        """
        try:
            leverage_key = f"leverage_{int(leverage)}"
            protection_config = self.params.get('futures_protection', {}).get(leverage_key, {})
            if not protection_config:
                logger.warning(f"No protection configuration for leverage {leverage}. Using default.")
                return entry_price
            signal_key = 'long' if signal.lower() == 'buy' else 'short'
            protection_factor = protection_config.get(signal_key, 1.0)
            protection_price = entry_price * protection_factor
            logger.debug(f"Calculated protection price for {signal} at leverage {leverage}: {protection_price}")
            return protection_price
        except Exception as e:
            logger.error(f"Calculate protection price error: {str(e)}")
            return entry_price
    def safe_float(value, default=0.0):
        try:
            return float(value)
        except (ValueError, TypeError):
            return default
    async def execute_trade_async(self, subaccount_id: str, exchange_name: str, symbol: str, 
                             market_type: str = 'spot', timeframe: str = '15m', 
                             signal: str = "Hold", entry_price: float = None) -> List[Dict]:
        """
        Execute trade using signals from the specified timeframe in client_credentials_encrypted.yaml.

        Args:
            subaccount_id (str): Subaccount identifier (e.g., 'okx.xWings')
            exchange_name (str): Exchange name (e.g., 'okx')
            symbol (str): Trading pair (e.g., 'BTC/USDT')
            market_type (str): Market type (spot, perpetual)
            timeframe (str): Timeframe (e.g., '5m', '15m', '30m', '1h')
            signal (str): signal (e.g., 'time Buy at price')

        Returns:
            List[Dict]: Trade records
        """
        async with asyncio.Lock():
            trade_records = []
            try:
                if not self.validate_subaccount(subaccount_id, exchange_name):
                    logger.error(f"Invalid subaccount {subaccount_id} for {exchange_name}")
                    raise ValueError(f"Invalid subaccount {subaccount_id}")
                exchange = self.exchanges.get(subaccount_id)
                if not exchange:
                    logger.error(f"No exchange for {subaccount_id}")
                    raise ValueError(f"No exchange for {subaccount_id}")

                trading_config = self.get_trading_config(subaccount_id, symbol)
                if not trading_config:
                    logger.error(f"No trading config for {subaccount_id}/{symbol}")
                    raise ValueError(f"No trading config for {subaccount_id}/{symbol}")
                timeframes = trading_config.get('timeframes', ['3m', '5m', '15m', '30m', '1h'])
                if timeframe not in timeframes:
                    logger.error(f"Invalid timeframe {timeframe} for {subaccount_id}/{symbol}, allowed: {timeframes}")
                    raise ValueError(f"Invalid timeframe {timeframe}")

                exchange_config = self.markets.get('exchanges', {}).get(exchange_name, {})
                market_config = exchange_config.get('markets', {}).get('crypto', {}).get(market_type, {})
                if not market_config:
                    logger.error(f"No market config for {exchange_name}/{market_type}")
                    raise ValueError(f"No market config for {exchange_name}/{market_type}")
                if symbol not in market_config.get('symbols', []):
                    logger.error(f"Symbol {symbol} not supported for {exchange_name}/{market_type}")
                    raise ValueError(f"Symbol {symbol} not supported")

                leverage = trading_config.get('leverage', 1) if market_type == 'perpetual' else 1
                self.validate_leverage(subaccount_id, symbol, leverage, market_type)

                instrument_info = await exchange.get_instrument_info(symbol, market_type)
                min_quantity = float(instrument_info.get('minQty', market_config.get('min_quantity', {}).get(symbol, 0.01)))
                lot_size = float(instrument_info.get('lotSize', 0.0001))
                max_order_quantity = market_config.get('max_order_quantity', {}).get(symbol, 10)

                # signal_gen_key = f"{exchange_name}_{symbol}_{market_type}"
                # signal_gen = self.signal_generators.get(signal_gen_key)
                # if not signal_gen:
                #     logger.error(f"No signal generator for {subaccount_id}/{symbol}/{market_type}")
                #     raise ValueError(f"No signal generator for {subaccount_id}/{market_type}")
                # signal = await signal_gen.get_signal(timeframe)
                # if not signal:
                #     logger.info(f"No signal available for {subaccount_id}/{symbol}/{market_type}/{timeframe}")
                #     return []
                signal_type = 'buy' if 'Buy' in signal else 'sell' if 'Sell' in signal else None
                if not signal_type:
                    logger.error(f"Invalid signal format: {signal}")
                    return []
                price = entry_price  # 直接使用挂单价

                previous_state = await self.funds_managers[subaccount_id].get_position(subaccount_id, symbol,
                                                                                       market_type)
                if previous_state['side'] == None:
                    quantity = await self.funds_managers[subaccount_id].allocate_funds(
                        subaccount_id=subaccount_id,
                        symbol=symbol,
                        signal=signal_type,
                        price=price,
                        market_type=market_type,
                        leverage=leverage
                    )
                    if quantity <= 0:
                        logger.warning(f"No funds allocated for {subaccount_id}/{symbol}/{market_type}")
                        return []

                    if quantity < min_quantity:
                        logger.error(f"Quantity {quantity} below minimum {min_quantity} for {subaccount_id}/{symbol}/{market_type}")
                        raise ValueError(f"Quantity below minimum")
                    if quantity > max_order_quantity:
                        logger.warning(f"Quantity {quantity} exceeds max {max_order_quantity}, adjusting")
                        quantity = max_order_quantity
                else:
                    quantity = previous_state['quantity'] * 2
                # previous_state = await self.funds_managers[subaccount_id].get_position(subaccount_id, symbol, market_type)

                async def execute_order(order: Dict, is_protection: bool = False) -> Dict:
                    """
                    Execute a single order with retries.

                    Args:
                        order (Dict): Order details
                        is_protection (bool): Whether this is a protection order

                    Returns:
                        Dict: Order result
                    """
                    max_retries = self.params.get('data', {}).get('retries', 3)
                    for attempt in range(max_retries):
                        try:
                            if is_protection:
                                result = await exchange.place_protection_order(
                                    subaccount_id=order['subaccount_id'],
                                    symbol=order['symbol'],
                                    market_type=order['market_type'],
                                    side=order['side'],
                                    quantity=order['amount'],
                                    stop_price=order['stop_price'],
                                    client_order_id=order['client_order_id']
                                )
                            else:
                                result = await exchange.place_order(
                                    subaccount_id=order['subaccount_id'],
                                    symbol=order['symbol'],
                                    market_type=order['market_type'],
                                    side=order['side'],
                                    amount=order['amount'],
                                    price=order['price'],
                                    client_order_id=order['client_order_id']
                                )
                            if result.get('code', '0') == '0':
                                return result
                            logger.warning(f"{'Protection' if is_protection else 'Main'} order attempt {attempt+1} failed: {result.get('msg')}")
                            await asyncio.sleep(self.params.get('data', {}).get('delay', 1))
                        except ExchangeError as e:
                            if e.code == 'RATE_LIMIT_EXCEEDED':
                                logger.warning(f"Rate limit exceeded, retrying attempt {attempt+1}/{max_retries}")
                                await asyncio.sleep(self.params.get('data', {}).get('delay', 1))
                            else:
                                raise
                    logger.error(f"Max retries reached for {'protection' if is_protection else 'main'} order")
                    return {'code': 'ERROR', 'msg': 'Max retries reached'}

                # 增强版修复代码
                try:
                    seed_str = f"{int(datetime.now(timezone(timedelta(hours=8))).timestamp())}{subaccount_id.replace('.', '')}"
                    seed = hash(seed_str)
                    random.seed(seed)
                    logger.debug(f"Generated random seed for {subaccount_id}: {seed} (from string: {seed_str})")
                except Exception as e:
                    logger.error(f"Failed to generate random seed: {str(e)}")
                    # 使用默认种子作为后备方案
                    random.seed(42)
                # seed = int(f"{int(datetime.now(timezone(timedelta(hours=8))).timestamp())}{subaccount_id.replace('.', '')}")
                # random.seed(seed)
                if quantity > max_order_quantity:
                    num_splits = max(2, int(quantity / max_order_quantity) + (1 if quantity % max_order_quantity > 0 else 0))
                    split_quantities = []
                    remaining_quantity = quantity
                    for i in range(num_splits - 1):
                        min_split = max(0.5 * max_order_quantity, min_quantity)
                        max_split = min(max_order_quantity, remaining_quantity - (num_splits - i - 1) * min_quantity)
                        split_quantity = random.uniform(min_split, max_split)
                        split_quantity = round(split_quantity / lot_size) * lot_size
                        split_quantities.append(split_quantity)
                        remaining_quantity -= split_quantity
                    split_quantities.append(round(remaining_quantity / lot_size) * lot_size)

                    for i, qty in enumerate(split_quantities):
                        order = {
                            'subaccount_id': subaccount_id,
                            'symbol': symbol,
                            'side': signal_type.lower(),
                            'amount': qty,
                            'price': price,
                            'client_order_id': f"{symbol}".split("/")[0] + f"{int(time.time())}" + f"{i}",
                            'market_type': market_type
                        }
                        result = await execute_order(order, False)
                        if result.get('code', '0') != '0':
                            logger.error(f"Split trade {i+1}/{num_splits} failed: {result.get('msg', 'Unknown error')}")
                            await self.funds_managers[subaccount_id].rollback_position(subaccount_id, symbol, market_type, previous_state)
                            return trade_records
                        try:
                            fee = float(result.get('fee', 0.0))
                        except Exception as e:
                            fee = 0.0
                        trade_record = {
                            'subaccount_id': subaccount_id,
                            'exchange_name': exchange_name,
                            'symbol': symbol,
                            'market_type': market_type,
                            'signal': signal_type,
                            'price': price,
                            'quantity': qty,
                            'fee': fee,
                            'order_id': result.get('orderId', ''),
                            'leverage': leverage,
                            'timeframe': timeframe,
                            'timestamp': datetime.now(timezone(timedelta(hours=8))).timestamp() * 1000,
                            'is_split': True,
                            'split_index': i
                        }
                        await self.db.save_trade(**trade_record)
                        trade_records.append(trade_record)

                        if market_type == 'perpetual':
                            protection_price = self._calculate_protection_price(leverage, signal_type, price)
                            opposite_side = 'sell' if signal_type == 'buy' else 'buy'
                            protection_order = {
                                'subaccount_id': subaccount_id,
                                'symbol': symbol,
                                'side': opposite_side,
                                'amount': qty,
                                'stop_price': protection_price,
                                'client_order_id': f"{symbol}".split("/")[0] + f"{int(time.time())}" + f"{i}protection",
                                'market_type': market_type
                            }
                            logger.info( f"GreatLGX-1 {protection_order}")
                            result = await execute_order(protection_order, True)
                            logger.info(f"GreatLGX-ok-1")
                            if result.get('code', '0') != '0':
                                logger.error(f"Protection order {i+1}/{num_splits} failed: {result.get('msg', 'Unknown error')}")
                                await self.funds_managers[subaccount_id].rollback_position(subaccount_id, symbol, market_type, previous_state)
                            else:
                                try:
                                    fee = float(result.get('fee', 0.0))
                                except Exception as e:
                                    fee = 0.0
                                trade_record = {
                                    'subaccount_id': subaccount_id,
                                    'exchange_name': exchange_name,
                                    'symbol': symbol,
                                    'market_type': market_type,
                                    'signal': f"{opposite_side}_protection",
                                    'price': protection_price,
                                    'quantity': qty,
                                    'fee': fee,
                                    'order_id': result.get('orderId', ''),
                                    'leverage': leverage,
                                    'timeframe': timeframe,
                                    'timestamp': datetime.now(timezone(timedelta(hours=8))).timestamp() * 1000,
                                    'is_split': True,
                                    'split_index': i,
                                    'is_protection': True
                                }
                                await self.db.save_trade(**trade_record)
                                trade_records.append(trade_record)
                else:
                    quantity = round(quantity / lot_size) * lot_size
                    order = {
                        'subaccount_id': subaccount_id,
                        'symbol': symbol,
                        'side': signal_type.lower(),
                        'amount': quantity,
                        'price': price,
                        'client_order_id': f"{symbol}".split("/")[0] + f"{int(time.time())}" + "single",
                        'market_type': market_type
                    }
                    logger.info(f"GreatLGX-2 {order}")
                    result = await execute_order(order, False)
                    logger.info(f"GreatLGX-ok-2")
                    if result.get('code', '0') != '0':
                        logger.error(f"Single trade failed: {result.get('msg', 'Unknown error')}")
                        await self.funds_managers[subaccount_id].rollback_position(subaccount_id, symbol, market_type, previous_state)
                        return trade_records
                    try:
                        fee = float(result.get('fee', 0.0))
                    except Exception as e:
                        fee = 0.0
                    trade_record = {
                        'subaccount_id': subaccount_id,
                        'exchange_name': exchange_name,
                        'symbol': symbol,
                        'market_type': market_type,
                        'signal': signal_type,
                        'price': price,
                        'quantity': quantity,
                        'fee': fee,
                        'order_id': result.get('orderId', ''),
                        'leverage': leverage,
                        'timeframe': timeframe,
                        'timestamp': datetime.now(timezone(timedelta(hours=8))).timestamp() * 1000,
                        'is_split': False
                    }
                    logger.info(f"GreatLGX-2-1 {trade_record}")
                    await self.db.save_trade(**trade_record)
                    logger.info(f"GreatLGX-2-1 ok ")
                    trade_records.append(trade_record)

                    if market_type == 'perpetual':
                        protection_price = self._calculate_protection_price(leverage, signal_type, price)
                        opposite_side = 'sell' if signal_type == 'buy' else 'buy'
                        protection_order = {
                            'subaccount_id': subaccount_id,
                            'symbol': symbol,
                            'side': opposite_side,
                            'amount': quantity,
                            'stop_price': protection_price,
                            'client_order_id': f"{symbol}".split("/")[0] + f"{int(time.time())}" + "singleprotection",
                            'market_type': market_type
                        }
                        logger.info(f"GreatLGX-3 {protection_order}")
                        result = await execute_order(protection_order, True)
                        logger.info(f"GreatLG-ok-3")
                        if result.get('code', '0') != '0':
                            logger.error(f"Protection order failed: {result.get('msg', 'Unknown error')}")
                            await self.funds_managers[subaccount_id].rollback_position(subaccount_id, symbol, market_type, previous_state)
                        else:
                            try:
                                fee = float(result.get('fee', 0.0))
                            except Exception as e:
                                fee = 0.0
                            trade_record = {
                                'subaccount_id': subaccount_id,
                                'exchange_name': exchange_name,
                                'symbol': symbol,
                                'market_type': market_type,
                                'signal': f"{opposite_side}_protection",
                                'price': protection_price,
                                'quantity': quantity,
                                'fee': fee,
                                'order_id': result.get('orderId', ''),
                                'leverage': leverage,
                                'timeframe': timeframe,
                                'timestamp': datetime.now(timezone(timedelta(hours=8))).timestamp() * 1000,
                                'is_split': False,
                                'is_protection': True
                            }
                            await self.db.save_trade(**trade_record)
                            trade_records.append(trade_record)

                logger.info(f"Executed {len(trade_records)} trades for {subaccount_id}/{symbol}/{market_type}/{timeframe}: {signal}")
                return trade_records
            except Exception as e:
                error_msg = traceback.format_exc()
                logger.error(f"发生错误1:\n{error_msg}")
                logger.error(f"Trade execution failed for {subaccount_id}/{symbol}/{market_type}/{timeframe}: {str(e)}")
                await self.funds_managers[subaccount_id].rollback_position(subaccount_id, symbol, market_type, previous_state)
                return trade_records

    async def initialize_market_data(self, symbols, timeframes, market_types, max_data_points=100):
        """
        Initialize market data center, fetch required symbol/timeframe/market_type data, and start WebSocket monitoring.
        Args:
            symbols: List of trading pairs
            timeframes: List of K-line timeframes
            market_types: List of market types
            max_data_points: Maximum number of K-lines to cache
        """
        # Fetch historical K-line data (REST)
        await self.market_data_center.start(symbols, timeframes, market_types, max_data_points)
        # Start WebSocket ticker monitoring (same symbols as historical K-lines)
        ws_center = self.ensure_ws_market_data_center()
        # await ws_center.start_websocket_ticker(symbols=symbols, market_types=market_types)

    def get_kline_for_account(self, symbol, timeframe, market_type):
        """
        Retrieve latest K-line data from market data center.
        """
        if self.market_data_center is None:
            raise RuntimeError("MarketDataCenter not initialized. Call initialize_market_data_center first.")
        return self.market_data_center.get_latest_kline(symbol, timeframe, market_type)

    def subscribe_market_data(self, symbol, timeframe, market_type, callback):
        """
        Subscribe to K-line data updates from market data center.
        """
        if self.market_data_center is None:
            raise RuntimeError("MarketDataCenter not initialized. Call initialize_market_data_center first.")
        self.market_data_center.subscribe(symbol, timeframe, market_type, callback)

    def ensure_ws_market_data_center(self):
        """
        Ensure WebSocket market data center is initialized only once.
        """
        if self.ws_market_data_center is None:
            # Initialize WebSocket market data center
            from src.core.market_data_center import MarketDataCenter as WSMarketDataCenter
            self.ws_market_data_center = WSMarketDataCenter(self.exchange)
        return self.ws_market_data_center


# if __name__ == '__main__':
#     async def main():
#         try:
#             key_file = str(path_manager.get_config_path('encryption.key'))
#             client_manager = ClientManager(
#                 key_file=key_file,
#                 db_config=parameters['database'],
#                 file_monitor=FileMonitor(key_file=str(key_file), config_data=parameters),
#                 exchange_name="okx"  # or "binance"
#             )
#             trade_records = await client_manager.execute_trade_async(
#                 subaccount_id='okx.xWings',
#                 exchange_name='okx',
#                 symbol='BTC/USDT',
#                 market_type='spot',
#                 timeframe='5m'
#             )
#             logger.info(f"Trade execution result: {trade_records}")
#         except Exception as e:
#             logger.error(f"Main execution failed: {str(e)}")
#             sys.exit(1)
#
#     asyncio.run(main())