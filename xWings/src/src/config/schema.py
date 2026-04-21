# File: src/config/schema.py
# Purpose: Validate and load configuration schemas for the xWings trading system
# Dependencies: typing, python-dateutil, yaml, cryptography, pydantic
# Notes: Timestamps in UTC+8. Validates configurations for xWings trading system.
#        Supports markets_encrypted.yaml with crypto.spot, crypto.perpetual, url fields.
#        Adjusted SymbolAllocationConfig to allow symbol allocations as a subset of markets_template.yaml.
#        Updated validate_dict_of_floats to remove sum-to-1 validation for max_order_quantity and min_quantity.
#        Added CryptoConfig to handle markets_template.yaml crypto structure (symbols, max_order_quantity, min_quantity, leverages).
#        Added subaccount_id to SymbolAllocationConfig for detailed logging.
#        Made perpetual parameter optional in SymbolAllocationConfig, defaults to empty dict.
# Updated: 2025-08-05

import sys
from pathlib import Path
import os
from typing import List, Dict, Optional, Union, Any
from dateutil.parser import parse
import datetime
from zoneinfo import ZoneInfo
import logging
import threading
import inspect
import yaml
import time

# Add xWings/ to sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.core.utils import PathManager

# Initialize logging
path_manager = PathManager(PROJECT_ROOT)
logger = logging.getLogger(__name__)

def load_markets_config() -> Dict:
    """Load markets_encrypted.yaml configuration."""
    try:
        key_file = path_manager.get_config_path("encryption.key")
        if not key_file.exists():
            logger.error(f"Encryption key file not found: {key_file}")
            raise FileNotFoundError(f"Encryption key file not found: {key_file}")
        with key_file.open('rb') as f:
            key = f.read()
        from cryptography.fernet import Fernet
        fernet = Fernet(key)
        markets_file = path_manager.get_config_path("markets_encrypted.yaml")
        if not markets_file.exists():
            logger.error(f"Markets config file not found: {markets_file}")
            raise FileNotFoundError(f"Markets config file not found: {markets_file}")
        with markets_file.open('rb') as f:
            encrypted_data = f.read()
        decrypted_data = fernet.decrypt(encrypted_data)
        return yaml.safe_load(decrypted_data)
    except Exception as e:
        logger.error(f"Failed to load markets_encrypted.yaml: {str(e)}")
        return {}

def load_market_timeframes(exchange: str) -> List[str]:
    """Load valid timeframes from markets_encrypted.yaml for a given exchange."""
    markets_config = load_markets_config()
    timeframes = markets_config.get('exchanges', {}).get(exchange, {}).get('timeframes', [])
    if not timeframes:
        logger.error(f"No timeframes defined for exchange {exchange} in markets_encrypted.yaml")
    return timeframes

def load_market_leverages(exchange: str, market_type: str) -> List[int]:
    """Load valid leverages from markets_encrypted.yaml for a given exchange and market type."""
    markets_config = load_markets_config()
    leverages = markets_config.get('exchanges', {}).get(exchange, {}).get('markets', {}).get('crypto', {}).get(market_type, {}).get('leverages', [])
    try:
        leverages = [int(l) for l in leverages]
    except (ValueError, TypeError):
        logger.error(f"Invalid leverages for {exchange}.{market_type}: must be integers. Got: {leverages}")
        raise ValueError(f"Invalid leverages for {exchange}.{market_type}: must be integers")
    if not leverages and market_type == 'spot':
        return [1]  # Spot always has leverage 1
    if not all(isinstance(l, int) and l > 0 for l in leverages):
        logger.error(f"Invalid leverages for {exchange}.{market_type}: must be positive integers. Got: {leverages}")
        raise ValueError(f"Invalid leverages for {exchange}.{market_type}: must be positive integers")
    return leverages

def validate_positive_int(value: Any, field_name: str) -> int:
    """Validate that a value is a positive integer."""
    if not isinstance(value, int) or value <= 0:
        logger.error(f"{field_name} must be a positive integer. Got: {value}")
        raise ValueError(f"{field_name} must be a positive integer")
    return value

def validate_dict_of_floats(value: Any, field_name: str, allowed_keys: List[str] = None, sum_to_one: bool = False) -> Dict[str, float]:
    """Validate a dictionary of non-negative floats with optional key restrictions."""
    if not isinstance(value, dict):
        logger.error(f"{field_name} must be a dictionary. Got: {value}")
        raise ValueError(f"{field_name} must be a dictionary")
    if allowed_keys and not all(k in allowed_keys for k in value.keys()):
        logger.error(f"{field_name} keys must be in {allowed_keys}. Got: {value.keys()}")
        raise ValueError(f"{field_name} keys must be in {allowed_keys}")
    if not all(isinstance(v, (int, float)) and v >= 0 for v in value.values()):
        logger.error(f"{field_name} values must be non-negative floats. Got: {value}")
        raise ValueError(f"{field_name} values must be non-negative floats")
    if sum_to_one:
        total = sum(value.values())
        if not 0.99 <= total <= 1.01:
            logger.error(f"{field_name} values must sum to approximately 1. Got: {total}")
            raise ValueError(f"{field_name} values must sum to approximately 1")
    return {k: float(v) for k, v in value.items()}

class SymbolConfig:
    """Configuration for market symbols."""
    def __init__(self, symbols: List[str], leverages: List[Union[int, float]], 
                 max_order_quantity: Dict[str, float], min_quantity: Dict[str, float]):
        self.symbols = symbols
        if not all(isinstance(s, str) and '/' in s for s in symbols):
            logger.error(f"symbols must be a list of valid symbol strings (e.g., 'BTC/USDT'). Got: {symbols}")
            raise ValueError(f"symbols must be a list of valid symbol strings")
        self.leverages = [int(l) for l in leverages]
        if not all(isinstance(l, int) and l > 0 for l in self.leverages):
            logger.error(f"leverages must be positive integers. Got: {leverages}")
            raise ValueError(f"leverages must be positive integers")
        self.max_order_quantity = validate_dict_of_floats(max_order_quantity, "max_order_quantity", symbols, sum_to_one=False)
        self.min_quantity = validate_dict_of_floats(min_quantity, "min_quantity", symbols, sum_to_one=False)

class CryptoConfig:
    """Configuration for crypto market settings (spot and perpetual)."""
    def __init__(self, spot: Dict[str, Any], perpetual: Dict[str, Any]):
        self.spot = SymbolConfig(**spot)
        self.perpetual = SymbolConfig(**perpetual)

class SymbolAllocationConfig:
    """Configuration for symbol-specific allocations."""
    def __init__(self,
                 spot: Dict[str, Any],
                 perpetual: Optional[Dict[str, Any]] = None,
                 market_conf_valid_symbols: Optional[Dict[str, List[str]]] = None,
                 subaccount_id: Optional[str] = None):

        # Validate if spot configuration is provided
        if spot is None:
            logger.error(f"Subaccount {subaccount_id or 'unknown'}: spot configuration is missing")
            raise ValueError(f"Subaccount {subaccount_id or 'unknown'}: spot configuration is missing")

        # Validate spot allocations
        if not isinstance(spot, dict):
            raise ValueError(f"spot (subaccount {subaccount_id or 'unknown'}) must be a dictionary")

        spot_validated = {}
        market_spot_symbols = market_conf_valid_symbols.get('spot')   # Market config info
        total_spot = 0.0
        # Validate spot symbols in subaccount configuration
        for symbol, value in spot.items():
            # symbol: Trading pair name
            # value: Allocation ratio for the symbol, should sum to 1
            # Check if symbol is valid
            if market_spot_symbols and symbol not in market_spot_symbols:
                logger.error(f"Subaccount {subaccount_id} configuration error: {symbol} not allowed in spot market")
                raise ValueError(f"Invalid symbol {symbol} in spot (subaccount {subaccount_id or 'unknown'})")

            # Check if value is a float or int
            if not isinstance(value, (float, int)):
                raise ValueError(
                    f"Value for {symbol} in spot (subaccount {subaccount_id or 'unknown'}) must be a float or integer")

            # Check if value is non-negative
            if value < 0:
                raise ValueError(
                    f"Value for {symbol} in spot (subaccount {subaccount_id or 'unknown'}) must be non-negative")

            # Accumulate total allocation
            spot_validated[symbol] = float(value)
            total_spot += float(value)

        # Check if sum equals 1.0
        if abs(total_spot - 1.0) > 1e-9:
            logger.error(f"Sum of values in spot (subaccount {subaccount_id}) must be 1.0, got {total_spot}")
            raise ValueError(f"Sum of values in spot (subaccount {subaccount_id}) must be 1.0, got {total_spot}")
        else:
            self.spot = spot_validated
            logger.info(f"Account: {subaccount_id} SPOT Allocation: PASS.")

        # Validate perpetual allocations
        if perpetual is None:
            self.perpetual = {}
            logger.info(f"Perpetual configuration is empty for subaccount {subaccount_id}")
        else:
            if not isinstance(perpetual, dict):
                raise ValueError(f"perpetual (subaccount {subaccount_id or 'unknown'}) must be a dictionary")
            perpetual_validated = {}
            market_perpetual_symbols = market_conf_valid_symbols.get('perpetual')
            total_perpetual = 0.0

            for symbol, value in perpetual.items():
                # Check if symbol is valid
                if market_perpetual_symbols and symbol not in market_perpetual_symbols:
                    raise ValueError(f"Invalid symbol {symbol} in perpetual (subaccount {subaccount_id or 'unknown'})")
                # Check if value is a float or int
                if not isinstance(value, (float, int)):
                    raise ValueError(
                        f"Value for {symbol} in perpetual (subaccount {subaccount_id or 'unknown'}) must be a float or integer")
                # Check if value is non-negative
                if value < 0:
                    raise ValueError(
                        f"Value for {symbol} in perpetual (subaccount {subaccount_id or 'unknown'}) must be non-negative")
                perpetual_validated[symbol] = float(value)
                total_perpetual += float(value)

            # Check if sum equals 1.0
            if abs(total_perpetual - 1) > 1e-9:
                logger.error(f"Sum of values in perpetual (subaccount {subaccount_id}) must be 1.0, got {total_perpetual}")
                raise ValueError(
                    f"Sum of values in perpetual (subaccount {subaccount_id}) must be 1.0, got {total_perpetual}")
            else:
                self.perpetual = perpetual_validated
                logger.info(f"Account: {subaccount_id} Perpetual Allocation: PASS.")

        # Validate market configuration symbols (commented out due to incorrect logic, to be optimized later)
        # if market_conf_valid_symbols:
        #     for market_type, config in [('spot', self.spot), ('perpetual', self.perpetual)]:
        #         if config:
        #             symbols_not_in_subacc = []
        #             for symbol in market_conf_valid_symbols.get(market_type):
        #                 if symbol not in config.keys():
        #                     logger.error(f"Invalid symbols in {market_type}: {symbol}. Must be in sub account conf.")
        #                     symbols_not_in_subacc.append(symbol)
        #             logger.error(f"Account: {subaccount_id}-{market_type} Symbols Check: Failed.")
        #             logger.error(f"{symbols_not_in_subacc} not in Market YAML.")
        # logger.info(f"Account: {subaccount_id} Symbols Check: PASS.")
        logger.info(f"Validated Symbols: SPOT={self.spot}, Perpetual={self.perpetual}")

class MarketConfigWrapper:
    """Wrapper for market settings."""
    def __init__(self, crypto: Dict[str, Any]):
        self.crypto = CryptoConfig(**crypto)

class ExchangeConfig:
    """Configuration for exchange settings."""
    def __init__(self, enabled: bool, plugin: Optional[str] = None, version: Optional[str] = None, 
                 module: Optional[str] = None, class_: Optional[str] = None,
                 api: Optional[Dict[str, Any]] = None, 
                 markets: Optional[Dict[str, Dict[str, Any]]] = None, 
                 timeframes: Optional[List[str]] = None, **kwargs):
        self.enabled = enabled
        self.plugin = plugin
        self.version = version
        self.class_ = class_
        self.module = module
        if api and self.enabled:
            self.api = ApiConfig(**api, enabled=enabled)
        else:
            self.api = None
        self.markets = MarketConfigWrapper(**markets) if markets else {}
        self.timeframes = timeframes if timeframes else []
        if kwargs:
            logger.warning(f"Ignored unexpected ExchangeConfig fields: {list(kwargs.keys())}")

class MarketConfig:
    """Configuration for market settings."""
    def __init__(self, exchanges: Optional[Dict[str, dict]] = None):
        self.exchanges = {k: ExchangeConfig(**v) for k, v in exchanges.items()} if exchanges else {}

class XConfig:
    """Configuration for the 'x' computation parameters."""
    def __init__(self, long_period: int, k_smooth: int, **kwargs):
        self.long_period = validate_positive_int(long_period, "long_period")
        self.k_smooth = validate_positive_int(k_smooth, "k_smooth")
        if kwargs:
            logger.warning(f"Ignored unexpected XConfig fields: {list(kwargs.keys())}")

class WingsConfig:
    """Configuration for the 'Wings' computation parameters."""
    def __init__(self, period: int, smooth_period: int, second_smooth_period: int, **kwargs):
        self.period = validate_positive_int(period, "period")
        self.smooth_period = validate_positive_int(smooth_period, "smooth_period")
        self.second_smooth_period = validate_positive_int(second_smooth_period, "second_smooth_period")
        if kwargs:
            logger.warning(f"Ignored unexpected WingsConfig fields: {list(kwargs.keys())}")

class TradingConfig:
    """Configuration for trading parameters."""
    def __init__(self, allowed_symbols: Optional[List[str]] = None, 
                 timeframes: Optional[List[str]] = None, exchange: Optional[str] = None):
        self.allowed_symbols = allowed_symbols or []
        if self.allowed_symbols and not all(isinstance(s, str) for s in self.allowed_symbols):
            logger.error("allowed_symbols must be a list of strings")
            raise ValueError("allowed_symbols must be a list of strings")
        self.timeframes = timeframes or []
        if self.timeframes and exchange:
            valid_timeframes = load_market_timeframes(exchange)
            if not valid_timeframes:
                logger.error(f"No timeframes defined for exchange {exchange} in markets_encrypted.yaml")
                raise ValueError(f"No timeframes defined for exchange {exchange}")
            if not all(tf in valid_timeframes for tf in self.timeframes):
                logger.error(f"Invalid timeframes for {exchange}: must be in {valid_timeframes}. Got: {self.timeframes}")
                raise ValueError(f"Invalid timeframes for {exchange}: must be in {valid_timeframes}")

class DataConfig:
    """Configuration for data settings."""
    def __init__(self, max_data_points: int, start_time: Optional[str] = 'dynamic', 
                 retries: int = 3, delay: int = 1):
        self.max_data_points = validate_positive_int(max_data_points, "max_data_points")
        self.retries = validate_positive_int(retries, "retries")
        self.delay = validate_positive_int(delay, "delay")
        if start_time is not None:
            if start_time.lower() == "dynamic":
                self.start_time = start_time
            else:
                try:
                    dt = parse(start_time, fuzzy=True)
                    if dt.tzinfo is None or dt.utcoffset() != datetime.timedelta(hours=8):
                        logger.warning(f"start_time {start_time} is not in UTC+8. Converting to UTC+8")
                        dt = dt.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
                    self.start_time = dt.isoformat()
                except ValueError as e:
                    logger.error(f"Invalid start_time format: {start_time}. Use ISO 8601 UTC+8 or 'dynamic'")
                    raise ValueError(f"Invalid start_time format: {start_time}. Use ISO 8601 UTC+8 or 'dynamic'")
        else:
            self.start_time = start_time

class SignalConfig:
    """Configuration for signal settings."""
    def __init__(self, price_type: str):
        valid_types = ['prev_close', 'current_close']
        if price_type not in valid_types:
            logger.error(f"price_type must be one of {valid_types}. Got: {price_type}")
            raise ValueError(f"price_type must be one of {valid_types}")
        self.price_type = price_type

class AllocationConfig:
    """Configuration for fund allocations."""
    def __init__(self, spot_trading: float, futures_trading: float, 
                 futures_protection: float, reserve: float, subaccount_id: str):
        for field, value in [("spot_trading", spot_trading), ("futures_trading", futures_trading),
                             ("futures_protection", futures_protection), ("reserve", reserve)
                             ]:
            # Validate individual configuration field
            if not (isinstance(value, (int, float)) and 0 <= value <= 1):
                logger.error(f"{field}:{value} must be between 0 and 1")
                raise ValueError(f"{field}:{value} must be between 0 and 1")
            setattr(self, field, value)
        # Validate total allocation sums to 1
        total = spot_trading + futures_trading + futures_protection + reserve
        if abs(total - 1.0) > 1e-9:
            logger.error(f"Total allocation must sum to approximately 1. Got: {total}")
            raise ValueError(f"Total allocation must sum to approximately 1")
        else:
            logger.info(f"Account: {subaccount_id} Fund Allocation: PASS.")

class FuturesProtectionConfig:
    """Configuration for futures protection levels."""
    def __init__(self, leverage_50: Dict[str, float], leverage_40: Dict[str, float], 
                 leverage_30: Dict[str, float], leverage_20: Dict[str, float], 
                 leverage_10: Dict[str, float]):
        valid_keys = ['long', 'short']
        for field, value in [
            ("leverage_50", leverage_50),
            ("leverage_40", leverage_40),
            ("leverage_30", leverage_30),
            ("leverage_20", leverage_20),
            ("leverage_10", leverage_10)
        ]:
            if not isinstance(value, dict) or not all(k in valid_keys for k in value.keys()):
                logger.error(f"{field} must contain 'long' and 'short' keys")
                raise ValueError(f"{field} must contain 'long' and 'short' keys")
            if not all(isinstance(v, (int, float)) and 0.9 <= v <= 1.1 for v in value.values()):
                logger.error(f"{field} values must be between 0.9 and 1.1. Got: {value}")
                raise ValueError(f"{field} values must be between 0.9 and 1.1")
            setattr(self, field, value)

class ApiConfig:
    """Configuration for exchange API credentials."""
    def __init__(self, api_key_name: str, api_key: str, api_secret: str, 
                 passphrase: Optional[str] = None, Permissions: Optional[List[str]] = None, 
                 url: Optional[str] = None, environment: Optional[str] = None, 
                 account_endpoint: Optional[str] = None, headers: Optional[Dict[str, str]] = None,
                 ip_binding_required: Optional[bool] = False, enabled: Optional[bool] = True):
        if not all(isinstance(x, str) for x in [api_key_name, api_key, api_secret]):
            logger.error("api_key_name, api_key, api_secret must be strings")
            raise ValueError("api_key_name, api_key, api_secret must be strings")
        if enabled and (api_key.startswith("your_") or api_secret.startswith("your_")):
            logger.error("API key or secret cannot be placeholder values when enabled is True")
            raise ValueError("API key or secret cannot be placeholder values when enabled is True")
        self.api_key_name = api_key_name
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        self.Permissions = Permissions or ['read', 'trade']
        if not all(p in ['read', 'trade'] for p in self.Permissions):
            logger.error(f"Permissions must be in ['read', 'trade']. Got: {self.Permissions}")
            raise ValueError(f"Permissions must be in ['read', 'trade']")
        self.url = url or "https://www.okx.com"
        self.environment = environment
        self.account_endpoint = account_endpoint
        self.headers = headers or {}
        self.ip_binding_required = ip_binding_required
        self.enabled = enabled

class SubaccountConfig:
    """Configuration for subaccounts."""
    def __init__(self, api_key: str,
                 api_secret: str,
                 exchange: Optional[str] = None,
                 subaccount_id: Optional[str] = None,
                 api_key_name: Optional[str] = None,
                 passphrase: Optional[str] = None,
                 Permissions: Optional[List[str]] = None,
                 xwings_uid: Optional[str] = None,
                 enabled: Optional[bool] = True,
                 fund_allocations: Optional[dict] = None,
                 login_name: Optional[str] = None,
                 account_mode: Optional[str] = None,
                 account_type: Optional[str] = None,
                 subaccounts: Optional[Dict[str, Any]] = None, 
                 trading_config: Dict[str, Dict[str, Any]] = None,
                 IP: Optional[str] = None,
                 symbol_allocations: Optional[Dict[str, Any]] = None, **kwargs):
        if not all(isinstance(x, str) for x in [api_key, api_secret]):
            logger.error("api_key, api_secret must be strings")
            raise ValueError("api_key, api_secret must be strings")
        if enabled and (api_key.startswith("your_") or api_secret.startswith("your_")):
            logger.error("API key or secret cannot be placeholder values when enabled is True")
            raise ValueError("API key or secret cannot be placeholder values when enabled is True")
        self.api_key = api_key
        self.api_secret = api_secret
        self.exchange = exchange
        self.subaccount_id = subaccount_id
        self.api_key_name = api_key_name
        self.passphrase = passphrase
        self.Permissions = Permissions or ['read', 'trade']
        if not all(p in ['read', 'trade'] for p in self.Permissions):
            logger.error(f"Permissions must be in ['read', 'trade']. Got: {self.Permissions}")
            raise ValueError(f"Permissions must be in ['read', 'trade']")
        self.xwings_uid = xwings_uid
        self.enabled = enabled
        self.fund_allocations = AllocationConfig(**fund_allocations) if fund_allocations else None
        self.login_name = login_name
        self.account_mode = account_mode
        if account_mode and account_mode not in ['Futures mode', 'Spot mode']:
            logger.error(f"account_mode must be 'Futures mode' or 'Spot mode'. Got: {account_mode}")
            raise ValueError(f"account_mode must be 'Futures mode' or 'Spot mode'")
        self.account_type = account_type
        if account_type and account_type not in ['self', 'managed']:
            logger.error(f"account_type must be 'self' or 'managed'. Got: {account_type}")
            raise ValueError(f"account_type must be 'self' or 'managed'")
        self.trading_config = trading_config or {}
        lock = threading.Lock()
        with lock:
            for symbol, config in self.trading_config.get('symbols', {}).items():
                if 'leverage' in config and exchange:
                    valid_leverages = load_market_leverages(exchange, 'spot' if account_mode == 'Spot mode' else 'perpetual')
                    if not valid_leverages:
                        logger.error(f"No leverages defined for exchange {exchange} in markets_encrypted.yaml")
                        raise ValueError(f"No leverages defined for exchange {exchange}")
                    leverage = config['leverage']
                    try:
                        leverage = int(leverage)
                    except (ValueError, TypeError):
                        logger.error(f"Invalid leverage type for {symbol}: must be integer. Got: {leverage}")
                        raise ValueError(f"Invalid leverage type for {symbol}: must be integer")
                    if leverage not in valid_leverages:
                        logger.error(f"Invalid leverage for {symbol}: must be in {valid_leverages}. Got: {leverage}")
                        raise ValueError(f"Invalid leverage for {symbol}: must be in {valid_leverages}")
                if 'timeframes' in config and isinstance(config['timeframes'], list) and exchange:
                    valid_timeframes = load_market_timeframes(exchange)
                    if not valid_timeframes:
                        logger.error(f"No timeframes defined for exchange {exchange} in markets_encrypted.yaml")
                        raise ValueError(f"No timeframes defined for exchange {exchange}")
                    if not all(tf in valid_timeframes for tf in config['timeframes']):
                        logger.error(f"Invalid timeframes for {symbol}: must be in {valid_timeframes}. Got: {config['timeframes']}")
                        raise ValueError(f"Invalid timeframes for {symbol}: must be in {valid_timeframes}")
        self.subaccounts = None
        if subaccounts:
            self.subaccounts = {}
            for k, v in subaccounts.items():
                v['subaccount_id'] = k if not v.get('subaccount_id') else v['subaccount_id']
                v['exchange'] = exchange
                self.subaccounts[k] = SubaccountConfig(**v)
        self.IP = IP
        if not IP:
            logger.warning(f"IP field is empty for subaccount {subaccount_id or login_name}. Ensure exchange does not require IP binding.")
        markets_config = load_markets_config()
        valid_symbols = {'spot': [], 'perpetual': []}
        if markets_config and exchange:
            for market_type in ['spot', 'perpetual']:
                symbols = markets_config.get('exchanges', {}).get(exchange, {}).get('markets', {}).get('crypto', {}).get(market_type, {}).get('symbols', [])
                valid_symbols[market_type] = symbols
        self.symbol_allocations = SymbolAllocationConfig(**symbol_allocations, market_conf_valid_symbols=valid_symbols, subaccount_id=subaccount_id) if symbol_allocations else None
        if kwargs:
            logger.warning(f"Ignored unexpected SubaccountConfig fields: {list(kwargs.keys())}")

class ClientCredentialsConfig:
    """Configuration for client credentials."""
    def __init__(self, subaccounts: Dict[str, dict], **kwargs):
        if not isinstance(subaccounts, dict):
            logger.error("subaccounts must be a dictionary")
            raise ValueError("subaccounts must be a dictionary")
        self.subaccounts = {}
        subaccount_params = set(inspect.signature(SubaccountConfig.__init__).parameters.keys()) - {'self'}

        for exchange, config in subaccounts.items():
            if isinstance(config, dict) and 'api_key' in config:
                sub_key = f"{exchange}.{config.get('login_name', config.get('api_key_name', 'unknown'))}"
                if sub_key in self.subaccounts:
                    logger.error(f"Duplicate subaccount key: {sub_key}")
                    raise ValueError(f"Duplicate subaccount key: {sub_key}")
                filtered_config = {k: v for k, v in config.items() if k in subaccount_params}
                filtered_config['subaccount_id'] = config.get('login_name', config.get('api_key_name', 'unknown'))
                filtered_config['exchange'] = exchange
                self.subaccounts[sub_key] = SubaccountConfig(**filtered_config)
            if 'subaccounts' in config:
                for sub_id, sub_config in config['subaccounts'].items():
                    sub_key = f"{exchange}.{sub_id}"
                    if sub_key in self.subaccounts:
                        logger.error(f"Duplicate subaccount key: {sub_key}")
                        raise ValueError(f"Duplicate subaccount key: {sub_key}")
                    filtered_config = {k: v for k, v in sub_config.items() if k in subaccount_params}
                    filtered_config['subaccount_id'] = sub_id
                    filtered_config['exchange'] = exchange
                    self.subaccounts[sub_key] = SubaccountConfig(**filtered_config)
        if kwargs:
            logger.warning(f"Ignored unexpected ClientCredentialsConfig fields: {list(kwargs.keys())}")

class FileMonitorConfig:
    """Configuration for file and API monitoring."""
    def __init__(self, exchanges: Dict[str, Dict[str, str]], check_interval: int = 86400, 
                 max_retries: int = 3, retry_delay: int = 5):
        lock = threading.Lock()
        with lock:
            if not isinstance(exchanges, dict):
                logger.error("exchanges must be a dictionary")
                raise ValueError("exchanges must be a dictionary")
            for exchange, config in exchanges.items():
                if 'url' not in config:
                    logger.error(f"Exchange {exchange} must include 'url' field")
                    raise ValueError(f"Exchange {exchange} must include 'url' field")
            self.exchanges = exchanges
        self.check_interval = validate_positive_int(check_interval, "check_interval")
        self.max_retries = validate_positive_int(max_retries, "max_retries")
        self.retry_delay = validate_positive_int(retry_delay, "retry_delay")

class PluginMetaConfig:
    """Configuration for plugin metadata."""
    def __init__(self, plugins: List[Dict[str, Any]]):
        if not isinstance(plugins, list):
            logger.error("plugins must be a list")
            raise ValueError("plugins must be a list")
        self.plugins = []
        valid_subaccount_envs = ['test', 'live']
        for plugin in plugins:
            if not isinstance(plugin, dict):
                logger.error("Each plugin must be a dictionary")
                raise ValueError("Each plugin must be a dictionary")
            name = plugin.get('name')
            if not name or not isinstance(name, str):
                logger.error("Plugin must have a 'name' field of type string")
                raise ValueError("Plugin must have a 'name' field")
            enabled = plugin.get('enabled', True)
            if not isinstance(enabled, bool):
                logger.error(f"Plugin {name}: enabled must be a boolean")
                raise ValueError(f"Plugin {name}: enabled must be a boolean")
            version = plugin.get('version')
            if version and not isinstance(version, str):
                logger.error(f"Plugin {name}: version must be a string")
                raise ValueError(f"Plugin {name}: version must be a string")
            module = plugin.get('module')
            if module and not isinstance(module, str):
                logger.error(f"Plugin {name}: module must be a string")
                raise ValueError(f"Plugin {name}: module must be a string")
            class_ = plugin.get('class')
            if class_ and not isinstance(class_, str):
                logger.error(f"Plugin {name}: class must be a string")
                raise ValueError(f"Plugin {name}: class must be a string")
            supported_methods = plugin.get('supported_methods', [])
            if not isinstance(supported_methods, list) or not all(isinstance(m, str) for m in supported_methods):
                logger.error(f"Plugin {name}: supported_methods must be a list of strings")
                raise ValueError(f"Plugin {name}: supported_methods must be a list of strings")
            subaccounts = plugin.get('subaccounts', [])
            if not isinstance(subaccounts, list):
                logger.error(f"Plugin {name}: subaccounts must be a list")
                raise ValueError(f"Plugin {name}: subaccounts must be a list")
            for sub in subaccounts:
                if not isinstance(sub, dict) or 'name' not in sub or 'environment' not in sub:
                    logger.error(f"Plugin {name}: each subaccount must have 'name' and 'environment' fields")
                    raise ValueError(f"Plugin {name}: each subaccount must have 'name' and 'environment' fields")
                if sub['environment'] not in valid_subaccount_envs:
                    logger.error(f"Plugin {name}: subaccount environment must be in {valid_subaccount_envs}. Got: {sub['environment']}")
                    raise ValueError(f"Plugin {name}: subaccount environment must be in {valid_subaccount_envs}")
            self.plugins.append({
                'name': name,
                'enabled': enabled,
                'version': version,
                'module': module,
                'class': class_,
                'supported_methods': supported_methods,
                'subaccounts': subaccounts
            })

class HistoricalMinConfig:
    """Configuration for historical minimum amplitude values."""
    def __init__(self, config: Dict[str, Any]):
        """
        校验 historical_min 配置
        
        Args:
            config: historical_min 配置字典，格式如：
                {
                    "BTC/USDT": {
                        "perpetual": {"30m": 0.035, "1h": 0.05},
                        "spot": {"30m": 0.035, "1h": 0.05}
                    }
                }
        """
        if not isinstance(config, dict):
            logger.error("historical_min must be a dictionary")
            raise ValueError("historical_min must be a dictionary")
        
        self.data = {}
        
        # 遍历每个交易对
        for symbol, market_types in config.items():
            # 校验交易对格式
            if not isinstance(symbol, str) or '/' not in symbol:
                logger.error(f"Symbol {symbol} must be a valid trading pair (e.g., 'BTC/USDT')")
                raise ValueError(f"Invalid symbol format: {symbol}")
            
            if not isinstance(market_types, dict):
                logger.error(f"Market types for {symbol} must be a dictionary")
                raise ValueError(f"Invalid market types for {symbol}")
            
            self.data[symbol] = {}
            
            # 遍历市场类型 (spot/perpetual)
            for market_type, timeframes in market_types.items():
                if market_type not in ['spot', 'perpetual']:
                    logger.error(f"Market type for {symbol} must be 'spot' or 'perpetual', got: {market_type}")
                    raise ValueError(f"Invalid market type: {market_type}")
                
                if not isinstance(timeframes, dict):
                    logger.error(f"Timeframes for {symbol}.{market_type} must be a dictionary")
                    raise ValueError(f"Invalid timeframes for {symbol}.{market_type}")
                
                self.data[symbol][market_type] = {}
                
                # 遍历时间框架
                for timeframe, value in timeframes.items():
                    # 校验时间框架格式
                    valid_timeframes = ['3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h']
                    if timeframe not in valid_timeframes:
                        logger.warning(f"Timeframe {timeframe} for {symbol}.{market_type} is not standard. Should be one of {valid_timeframes}")
                    
                    # 校验值是否为有效的数字
                    try:
                        val = float(value)
                        if val <= 0 or val >= 1:
                            logger.warning(f"hist_min value for {symbol}.{market_type}.{timeframe} is {val}, should typically be between 0 and 1 (percentage)")
                        if val < 0:
                            logger.error(f"hist_min value for {symbol}.{market_type}.{timeframe} must be >= 0, got: {val}")
                            raise ValueError(f"Invalid hist_min value: {val}")
                    except (TypeError, ValueError):
                        logger.error(f"hist_min value for {symbol}.{market_type}.{timeframe} must be a number, got: {value}")
                        raise ValueError(f"Invalid hist_min value type: {value}")
                    
                    self.data[symbol][market_type][timeframe] = val
        
        logger.info("Historical min configuration validated successfully")

class ParametersConfig:
    """Configuration for trading parameters."""
    def __init__(self, indicators: Dict[str, Any], data: dict, signal: dict,
                 historical_min: Optional[Dict[str, Any]] = None,  
                 futures_protection: Optional[dict] = None, 
                 database: Optional[Dict[str, Any]] = None, 
                 api_monitor: Optional[dict] = None, **kwargs):
        self.indicators = {}
        lock = threading.Lock()
        with lock:
            if not isinstance(indicators, dict):
                logger.error("indicators must be a dictionary")
                raise ValueError("indicators must be a dictionary")
            if 'x' in indicators:
                self.indicators['x'] = XConfig(**indicators['x'])
            if 'Wings' in indicators:
                self.indicators['Wings'] = WingsConfig(**indicators['Wings'])
        self.data = DataConfig(**data)
        self.signal = SignalConfig(**signal)
        if historical_min is not None:
            self.historical_min = HistoricalMinConfig(historical_min).data
        else:
            self.historical_min = {}
            logger.info("No historical_min configuration found, using defaults")
        self.futures_protection = FuturesProtectionConfig(**futures_protection) if futures_protection else None
        self.database = database
        self.api_monitor = FileMonitorConfig(**api_monitor) if api_monitor else None