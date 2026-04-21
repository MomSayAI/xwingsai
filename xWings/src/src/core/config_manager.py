# File: src/core/config_manager.py
# Purpose: Manage encrypted configuration files for xWings trading system
# Dependencies: yaml, cryptography, pathlib, logging, requests
# Notes: Loads and decrypts YAML configuration files using Fernet encryption.
#        Timestamps in UTC+8. Uses synchronous loading to avoid event loop errors.
#        Adapted for Windows/Linux path compatibility.
#        Uses fund_allocations exclusively, removed default_allocations hardcoding.
#        Validates configurations using schema.py classes.
#        Inherit symbol_allocations from client_credentials_template.yaml if available.
#        Added IP binding check with warning for empty IP, allows empty IP if ip_binding_required is false.
#        Optimized get_subaccount_config for robust subaccount retrieval.
#        Enhanced error handling for markets_encrypted.yaml validation.
#        Fixed markets.crypto mapping to prevent SymbolConfig mapping error.
#        Added subaccount_id to SymbolAllocationConfig validation for detailed logging.
# Updated: 2025-07-09
# artifact_id: e9f4b2c1-7d8e-4a9f-b5c3-2a1f6d8e9c7a

import sys
import os
import logging
import time
from pathlib import Path
from typing import Dict, Optional, List
import yaml
from cryptography.fernet import Fernet, InvalidToken
import requests
import hmac
import hashlib
import base64
from datetime import datetime, timezone, timedelta
import threading
import asyncio

# Ensure project root and core are in sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_PATH = PROJECT_ROOT / 'src'
CORE_PATH = SRC_PATH / 'core'
for path in [PROJECT_ROOT, SRC_PATH, CORE_PATH]:
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from src.core.utils import PathManager
from src.config.schema import ClientCredentialsConfig, ExchangeConfig, ParametersConfig, AllocationConfig, SymbolAllocationConfig, MarketConfigWrapper, SymbolConfig

# Initialize logging
path_manager = PathManager(PROJECT_ROOT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class ConfigManager:
    """Manage loading and decryption of configuration files."""
    def __init__(self, key_file: str):
        """
        Initialize ConfigManager with encryption key file.

        Args:
            key_file (str): Path to encryption key file

        Raises:
            FileNotFoundError: If key file not found
            ValueError: If decryption fails or key is invalid
        """
        self.key_file = path_manager.get_config_path(key_file)
        self.configs = {}
        self.file_monitor = None
        self.lock = threading.Lock()
        self.client_credentials_config_obj = ''
        self.parameter_config_obj = ''
        self.exchange_config_obj = ''

        # 秘钥文件检测处理及校验
        if not self.key_file.exists():
            logger.error(f"Encryption key file not found: {self.key_file}")
            raise FileNotFoundError(f"Encryption key file not found: {self.key_file}")
        try:
            with self.key_file.open('rb') as f:
                self.key = f.read()
            if len(self.key) != 44:  # Fernet key must be 44 bytes (base64-encoded)
                logger.error(f"Invalid key length: {len(self.key)} bytes, expected 44")
                raise ValueError(f"Invalid key length: {len(self.key)} bytes, expected 44")
            key_hash = hashlib.sha256(self.key).hexdigest()[:8]
            logger.debug(f"Loaded encryption key (length: {len(self.key)} bytes, hash: {key_hash}) from {self.key_file}")
            self.fernet = Fernet(self.key)
            logger.debug(f"Fernet initialized with key from {self.key_file}")
            # Test key with a dummy encryption/decryption
            test_data = b"test"
            try:
                encrypted = self.fernet.encrypt(test_data)
                decrypted = self.fernet.decrypt(encrypted)
                if decrypted != test_data:
                    logger.error("Key integrity test failed: decryption mismatch")
                    raise ValueError("Key integrity test failed")
            except InvalidToken:
                logger.error("Key integrity test failed: invalid token")
                raise ValueError("Key integrity test failed")
            # 自动加载初始化配置文件，不需要，
            # 因为外部调用可是使用get_config,而且主函数又显示的使用了load_initial_configs调用了一次，保留一个即可
            # self.load_initial_configs()
        except Exception as e:
            logger.error(f"Failed to load encryption key: {str(e)}")
            raise ValueError(f"Failed to load encryption key: {str(e)}")

    def load_initial_configs(self):
        """Load initial configuration files."""
        config_files = ['parameters_encrypted', 'client_credentials_encrypted', 'markets_encrypted']
        for config in config_files:
            try:
                # Load and validate configuration
                config_data = self._load_config(config)
                if config_data:
                    self.configs[config] = config_data
                    # logger.info(f"Configuration loaded successfully: {config}.yaml")
                else:
                    logger.warning(f"Configuration {config} not found or invalid, skipping")
            except Exception as e:
                logger.error(f"Failed to load {config}: {str(e)}")
                if config != 'markets_encrypted':
                    raise

    def _load_config(self, config_name: str) -> Optional[Dict]:
        """
        Load and decrypt a YAML configuration file.

        Args:
            config_name (str): Name of the configuration file (e.g., 'parameters_encrypted')

        Returns:
            Optional[Dict]: Decrypted configuration data or None if failed
        """
        try:
            config_path = path_manager.get_config_path(f"{config_name}.yaml")
            if not config_path.exists():
                logger.warning(f"Configuration file not found: {config_path}")
                return None
            if not config_path.is_file() or config_path.stat().st_size == 0:
                logger.error(f"Configuration file is empty or invalid: {config_path}")
                return None
            with config_path.open('rb') as f:
                encrypted_data = f.read()
            if not encrypted_data:
                logger.error(f"Configuration file is empty: {config_path}")
                return None
            try:
                decrypted_data = self.fernet.decrypt(encrypted_data)
                config_data = yaml.safe_load(decrypted_data)
                if not isinstance(config_data, dict):
                    logger.error(f"Invalid configuration format in {config_path}: expected a dictionary")
                    return None
                # Validate configuration based on type
                if config_name == 'client_credentials_encrypted':
                    ClientCredentialsConfig(**config_data)
                    self.client_credentials_config_obj = ClientCredentialsConfig(**config_data)
                elif config_name == 'parameters_encrypted':
                    ParametersConfig(**config_data)
                    self.parameter_config_obj = ParametersConfig(**config_data)
                elif config_name == 'markets_encrypted':
                    # Validate each exchange configuration
                    exchanges = config_data.get('exchanges', {})
                    if not exchanges:
                        logger.error(f"No exchanges defined in {config_path}")
                        return None
                    for exchange_name, exchange_config in exchanges.items():
                        try:
                            # Ensure markets.crypto is passed as a dictionary
                            markets = exchange_config.get('markets', {})
                            if 'crypto' in markets:
                                # Validate markets.crypto structure before passing to ExchangeConfig
                                crypto_config = markets['crypto']
                                if not isinstance(crypto_config, dict):
                                    logger.error(f"markets.crypto must be a dictionary for {exchange_name}. Got: {crypto_config}")
                                    raise ValueError(f"markets.crypto must be a dictionary for {exchange_name}")
                                # Validate spot and perpetual subfields
                                for market_type in ['spot', 'perpetual']:
                                    if market_type in crypto_config:
                                        market_config = crypto_config[market_type]
                                        if not isinstance(market_config, dict):
                                            logger.error(f"markets.crypto.{market_type} must be a dictionary for {exchange_name}. Got: {market_config}")
                                            raise ValueError(f"markets.crypto.{market_type} must be a dictionary for {exchange_name}")
                                        if 'symbols' not in market_config or not isinstance(market_config['symbols'], list):
                                            logger.error(f"markets.crypto.{market_type}.symbols must be a list for {exchange_name}")
                                            raise ValueError(f"markets.crypto.{market_type}.symbols must be a list for {exchange_name}")
                                        # Validate SymbolConfig structure
                                        SymbolConfig(**market_config)
                                # Keep markets as a dictionary for ExchangeConfig
                                markets = {'crypto': crypto_config}
                                exchange_config['markets'] = markets
                            ExchangeConfig(**exchange_config)
                            self.exchage_config_obj = ExchangeConfig(**exchange_config)
                            logger.debug(f"Validated exchange config: {exchange_name}")
                        except TypeError as e:
                            logger.error(f"Invalid exchange config for {exchange_name}: {str(e)}")
                            raise ValueError(f"Invalid exchange config for {exchange_name}: {str(e)}")
                        except ValueError as e:
                            if not exchange_config.get('enabled', True):
                                logger.warning(f"Skipping validation for disabled exchange {exchange_name}: {str(e)}")
                            else:
                                logger.error(f"Validation failed for {exchange_name}: {str(e)}")
                                raise ValueError(f"Validation failed for {exchange_name}: {str(e)}")
                logger.debug(f"Loaded and decrypted configuration from {config_path}")
                return config_data
            except InvalidToken:
                logger.error(f"Decryption failed for {config_name}: invalid encryption key")
                return None
            except yaml.YAMLError as e:
                logger.error(f"YAML parsing error for {config_name}: {str(e)}")
                return None
            except ValueError as e:
                logger.error(f"Configuration validation failed for {config_name}: {str(e)}")
                return None
        except Exception as e:
            logger.error(f"Error accessing {config_name}.yaml: {str(e)}")
            return None

    def get_config(self, config_name: str) -> Optional[Dict]:
        """
        Retrieve a configuration by name.

        Args:
            config_name (str): Name of the configuration file (without .yaml)

        Returns:
            Optional[Dict]: Configuration data or None if not found
        """
        if config_name not in self.configs:
            config_data = self._load_config(config_name)
            if config_data:
                self.configs[config_name] = config_data
        config_data = self.configs.get(config_name)
        if not config_data:
            logger.warning(f"Configuration {config_name} not found, returning None")
        return config_data

    def get_subaccount_config(self, subaccount_id: str, exchange_name: str) -> Dict:
        """
        Retrieve subaccount configuration for a given subaccount_id and exchange.

        Args:
            subaccount_id (str): Subaccount identifier (e.g., 'okx.xWings')
            exchange_name (str): Exchange name (e.g., 'okx')

        Returns:
            Dict: Subaccount configuration

        Raises:
            ValueError: If subaccount or exchange not found
        """
        try:
            client_config = self.get_config('client_credentials_encrypted')
            if not client_config or 'subaccounts' not in client_config:
                logger.error("Client credentials configuration not found")
                raise ValueError("Client credentials configuration not found")

            # Parse subaccount_id, support nested subaccounts
            parts = subaccount_id.split('.')
            if len(parts) < 2:
                logger.error(f"Invalid subaccount_id format: {subaccount_id}. Expected 'exchange.subaccount' or 'exchange.parent.child'")
                raise ValueError(f"Invalid subaccount_id format: {subaccount_id}")
            
            exchange = parts[0]
            sub_id = '.'.join(parts[1:])  # Support nested subaccounts
            
            if exchange != exchange_name.lower():
                logger.error(f"Exchange mismatch: {exchange} != {exchange_name}")
                raise ValueError(f"Exchange mismatch: {exchange} != {exchange_name}")

            subaccount_config = None
            if sub_id == 'default':
                subaccount_config = client_config.get('subaccounts', {}).get(exchange, {})
            else:
                # Handle nested subaccounts
                exchange_config = client_config.get('subaccounts', {}).get(exchange, {})
                subaccount_parts = sub_id.split('.')  # Support nested subaccounts
                
                # Recursively find subaccount configuration
                current = exchange_config
                for i, part in enumerate(subaccount_parts):
                    # Try direct key match first
                    if part in current:
                        current = current[part]
                    else:
                        # Try matching via login_name
                        found = None
                        for key, value in current.items():
                            if isinstance(value, dict) and value.get('login_name') == part:
                                found = value
                                break
                        if not found:
                            logger.error(f"Account {part} not found in {subaccount_id}")
                            raise ValueError(f"Account {part} not found in {subaccount_id}")
                        current = found
                    
                    # If not the last part, move to subaccounts level
                    if i < len(subaccount_parts) - 1:
                        if 'subaccounts' not in current:
                            logger.error(f"No subaccounts found for {part} in {subaccount_id}")
                            raise ValueError(f"No subaccounts found for {part} in {subaccount_id}")
                        current = current['subaccounts']
                
                subaccount_config = current

            if not subaccount_config:
                logger.error(f"Subaccount {subaccount_id} not found in client_credentials_encrypted.yaml")
                raise ValueError(f"Subaccount {subaccount_id} not found")

            # Validate fund_allocations
            if 'fund_allocations' not in subaccount_config:
                logger.error(f"Subaccount {subaccount_id} missing fund_allocations")
                raise ValueError(f"Subaccount {subaccount_id} missing fund_allocations")
            AllocationConfig(**subaccount_config['fund_allocations'], subaccount_id=subaccount_id)

            # Validate symbol_allocations
            if 'symbol_allocations' not in subaccount_config:
                logger.error(f"Subaccount {subaccount_id} missing symbol_allocations")
                raise ValueError(f"Subaccount {subaccount_id} missing symbol_allocations")
            markets_config = self.get_config('markets_encrypted')
            valid_symbols = {'spot': [], 'perpetual': []}
            for market_type in ['spot', 'perpetual']:
                symbols = markets_config.get('exchanges', {}).get(exchange, {}).get('markets', {}).get('crypto', {}).get(market_type, {}).get('symbols', [])
                valid_symbols[market_type] = symbols
            try:
                SymbolAllocationConfig(
                    **subaccount_config['symbol_allocations'],
                    market_conf_valid_symbols=valid_symbols,
                    subaccount_id=subaccount_id
                )
            except TypeError as e:
                logger.error(f"SymbolAllocationConfig validation failed for subaccount {subaccount_id}: {str(e)}")
                raise ValueError(f"SymbolAllocationConfig validation failed for subaccount {subaccount_id}: {str(e)}")

            logger.debug(f"Retrieved and validated subaccount config for {subaccount_id}")
            return subaccount_config
        except Exception as e:
            logger.error(f"Failed to retrieve subaccount config for {subaccount_id}: {str(e)}")
            raise

    def reload_config(self, config_name: str):
        """
        Reload a specific configuration file.

        Args:
            config_name (str): Name of the configuration file (without .yaml)
        """
        with self.lock:
            try:
                config_data = self._load_config(config_name)
                if config_data:
                    self.configs[config_name] = config_data
                    logger.info(f"Reloaded configuration: {config_name}.yaml")
                else:
                    self.configs.pop(config_name, None)
                    logger.warning(f"Failed to reload {config_name}.yaml, removed from cache")
            except Exception as e:
                logger.error(f"Failed to reload {config_name}: {str(e)}")
                raise

    def validate_subaccount(self, subaccount_id: str, symbol: str, timeframes: List[str]):
        """
        Validate subaccount configuration for a given symbol and timeframes.

        Args:
            subaccount_id (str): Subaccount identifier
            symbol (str): Trading symbol
            timeframes (List[str]): List of timeframes to validate

        Raises:
            ValueError: If subaccount configuration is invalid
        """
        with self.lock:
            try:
                client_config = self.get_config('client_credentials_encrypted')
                if not client_config or 'subaccounts' not in client_config:
                    logger.error("Client credentials configuration not found")
                    raise ValueError("Client credentials configuration not found")
                
                # Parse subaccount_id, support nested subaccounts
                parts = subaccount_id.split('.')
                if len(parts) < 2:
                    logger.error(f"Invalid subaccount_id format: {subaccount_id}. Expected 'exchange.subaccount' or 'exchange.parent.child'")
                    raise ValueError(f"Invalid subaccount_id format: {subaccount_id}")
                
                exchange = parts[0]
                sub_id = '.'.join(parts[1:])  # Support nested subaccounts
                
                subaccount_config = None
                if sub_id == 'default':
                    subaccount_config = client_config.get('subaccounts', {}).get(exchange, {})
                else:
                    # Handle nested subaccounts
                    exchange_config = client_config.get('subaccounts', {}).get(exchange, {})
                    subaccount_parts = sub_id.split('.')  # Use sub_id for nested paths
                    
                    # Recursively find subaccount configuration
                    current = exchange_config
                    for part in subaccount_parts:
                        if part in current:
                            current = current[part]
                        else:
                            # Try matching via login_name
                            found = None
                            for key, value in current.items():
                                if isinstance(value, dict) and value.get('login_name') == part:
                                    found = value
                                    break
                            if not found:
                                logger.error(f"Account {part} not found in {subaccount_id}")
                                raise ValueError(f"Account {part} not found in {subaccount_id}")
                            current = found
                        
                        # If not the last part, move to subaccounts
                        if part != subaccount_parts[-1]:
                            if 'subaccounts' not in current:
                                logger.error(f"No subaccounts found for {part} in {subaccount_id}")
                                raise ValueError(f"No subaccounts found for {part} in {subaccount_id}")
                            current = current['subaccounts']
                    
                    subaccount_config = current
                
                if not subaccount_config:
                    logger.error(f"Subaccount {subaccount_id} not found in client_credentials_encrypted.yaml")
                    raise ValueError(f"Subaccount {subaccount_id} not found")
                
                markets_config = self.get_config('markets_encrypted')
                valid_symbols = []
                market_types = ['spot', 'perpetual']
                for market_type in market_types:
                    symbols = markets_config.get('exchanges', {}).get(exchange, {}).get('markets', {}).get('crypto', {}).get(market_type, {}).get('symbols', [])
                    valid_symbols.extend(symbols)
                if symbol not in valid_symbols:
                    logger.error(f"Invalid symbol {symbol} for {subaccount_id}. Must be in {valid_symbols}")
                    raise ValueError(f"Invalid symbol {symbol} for {subaccount_id}")
                
                valid_timeframes = markets_config.get('exchanges', {}).get(exchange, {}).get('timeframes', [])
                # Convert timeframe format to match load_market_timeframes logic
                def tf_convert(t):
                    if t.endswith('h'):
                        return f"{int(t[:-1]) * 60}m"
                    return t
                converted_valid_timeframes = [tf_convert(t) for t in valid_timeframes]
                # Allow '1h' and '60m' equivalence
                def tf_equiv(tf):
                    if tf.endswith('m') and int(tf[:-1]) % 60 == 0 and int(tf[:-1]) >= 60:
                        return [f"{int(tf[:-1])//60}h", tf]
                    if tf.endswith('h'):
                        return [f"{int(tf[:-1])*60}m", tf]
                    return [tf]
                # Check if each timeframe is in valid_timeframes or its equivalent
                for tf in timeframes:
                    equivs = tf_equiv(tf)
                    if not any(e in converted_valid_timeframes for e in equivs):
                        logger.error(f"Invalid timeframe {tf} for {subaccount_id}: allowed {valid_timeframes} (equivalent: {converted_valid_timeframes})")
                        raise ValueError(f"Invalid timeframe {tf} for {subaccount_id}")
                
                trading_config = subaccount_config.get('trading_config', {}).get('symbols', {})
                if symbol in trading_config:
                    symbol_config = trading_config[symbol]
                    if 'timeframes' in symbol_config and not all(tf in symbol_config['timeframes'] for tf in timeframes):
                        logger.error(f"Timeframes {timeframes} not allowed for {symbol} in {subaccount_id}. Must be in {symbol_config['timeframes']}")
                        raise ValueError(f"Timeframes {timeframes} not allowed for {symbol} in {subaccount_id}")
                else:
                    logger.warning(f"No specific trading_config for {symbol} in {subaccount_id}. Using default exchange timeframes")
                
                logger.debug(f"Validated subaccount {subaccount_id} for symbol {symbol} and timeframes {timeframes}")
            except Exception as e:
                logger.error(f"Failed to validate subaccount {subaccount_id}: {str(e)}")
                raise

    def update_client_credentials(self, api_key: str, api_secret: str, passphrase: str, exchange: str = "okx", subaccount_name: Optional[str] = None):
        """
        Update client_credentials_encrypted.yaml with main/subaccount info from exchange API.

        Args:
            api_key (str): Exchange API key
            api_secret (str): Exchange API secret key
            passphrase (str): Exchange API passphrase
            exchange (str): Exchange name (e.g., 'okx', 'binance')
            subaccount_name (Optional[str]): Name of the subaccount to update (e.g., 'MomSayAI')

        Raises:
            ValueError: If API credentials are invalid or exchange not found
            requests.exceptions.RequestException: If API request fails
        """
        with self.lock:
            try:
                if not all([api_key, api_secret, passphrase]) or any(x.startswith("your_") for x in [api_key, api_secret, passphrase]):
                    logger.error(f"Invalid {exchange} API credentials provided")
                    raise ValueError(f"Invalid {exchange} API credentials provided")

                markets_config = self.get_config("markets_encrypted")
                if not markets_config or exchange not in markets_config.get("exchanges", {}):
                    logger.error(f"Exchange {exchange} not found in markets_encrypted.yaml")
                    raise ValueError(f"Exchange {exchange} not found in markets_encrypted.yaml")
                exchange_config = markets_config["exchanges"][exchange]
                if not exchange_config.get("enabled", True):
                    logger.warning(f"Exchange {exchange} is disabled. Enabling it for update.")
                    exchange_config["enabled"] = True

                base_url = exchange_config.get("api", {}).get("url", "https://www.okx.com")
                api_path = exchange_config.get("api", {}).get("account_endpoint", "/api/v5/account/config")
                headers_template = exchange_config.get("api", {}).get("headers", {
                    "key": "OK-ACCESS-KEY",
                    "sign": "OK-ACCESS-SIGN",
                    "timestamp": "OK-ACCESS-TIMESTAMP",
                    "passphrase": "OK-ACCESS-PASSPHRASE"
                })

                ip_binding_required = exchange_config.get("api", {}).get("ip_binding_required", False)
                ip_address = ""  # API response may provide IP, default empty
                if ip_binding_required and not ip_address:
                    logger.error(f"Exchange {exchange} requires IP binding, but IP is empty")
                    raise ValueError(f"Exchange {exchange} requires IP binding, but IP is empty")
                elif not ip_address:
                    logger.warning(f"IP is empty for {exchange} account {subaccount_name or 'main'}. Ensure exchange allows empty IP.")

                timestamp = datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace("+00:00", "Z")
                method = "GET"
                request_path = api_path
                body = ""
                sign_str = timestamp + method + request_path + body
                signature = base64.b64encode(
                    hmac.new(
                        api_secret.encode('utf-8'),
                        sign_str.encode('utf-8'),
                        hashlib.sha256
                    ).digest()
                ).decode('utf-8')

                headers = {
                    headers_template.get("key"): api_key,
                    headers_template.get("sign"): signature,
                    headers_template.get("timestamp"): timestamp,
                    headers_template.get("passphrase"): passphrase,
                    "Content-Type": "application/json"
                }

                try:
                    response = requests.get(f"{base_url}{request_path}", headers=headers)
                    response.raise_for_status()
                except requests.exceptions.HTTPError as e:
                    logger.error(f"HTTP error fetching {exchange} account config: {str(e)}")
                    if response.status_code == 401:
                        logger.error("401 Unauthorized: Check API key, secret, passphrase, or IP binding")
                    raise
                except requests.exceptions.RequestException as e:
                    logger.error(f"Request error fetching {exchange} account config: {str(e)}")
                    raise

                data = response.json()
                if exchange == "okx" and data.get("code") != "0":
                    logger.error(f"Failed to fetch {exchange} account config: {data.get('msg')}")
                    raise ValueError(f"API error: {data.get('msg')}")
                elif exchange == "binance" and "code" in data and data["code"] != 0:
                    logger.error(f"Failed to fetch {exchange} account config: {data.get('msg')}")
                    raise ValueError(f"API error: {data.get('msg')}")

                account_data = data["data"][0] if exchange == "okx" else data

                market_types = ['spot', 'perpetual']
                supported_symbols = []
                supported_timeframes = exchange_config.get("timeframes", ['3m', '15m', '30m', '1h'])
                for market_type in market_types:
                    symbols = exchange_config.get("markets", {}).get("crypto", {}).get(market_type, {}).get("symbols", [])
                    supported_symbols.extend(symbols)

                client_config = self.get_config("client_credentials_encrypted") or {"subaccounts": {exchange: {}}}
                existing_allocations = client_config.get("subaccounts", {}).get(exchange, {}).get("fund_allocations", {})
                existing_symbol_allocations = client_config.get("subaccounts", {}).get(exchange, {}).get("symbol_allocations", {})

                account_mode = "Futures mode" if subaccount_name else "Spot mode"
                market_type = 'spot' if account_mode == "Spot mode" else 'perpetual'
                from src.config.schema import load_market_leverages
                valid_leverages = load_market_leverages(exchange, market_type)
                default_leverage = 1 if account_mode == "Spot mode" else valid_leverages[0] if valid_leverages else 10

                # Use existing fund_allocations if available, else raise error
                if not existing_allocations:
                    logger.error(f"No fund_allocations defined for {exchange}.{subaccount_name or 'main'}")
                    raise ValueError(f"No fund_allocations defined for {exchange}.{subaccount_name or 'main'}")
                AllocationConfig(**existing_allocations)

                # Use existing symbol_allocations if available, else create minimal
                if not existing_symbol_allocations:
                    logger.error(f"No symbol_allocations defined for {exchange}.{subaccount_name or 'main'}")
                    raise ValueError(f"No symbol_allocations defined for {exchange}.{subaccount_name or 'main'}")
                SymbolAllocationConfig(**existing_symbol_allocations, valid_symbols={'spot': supported_symbols, 'perpetual': supported_symbols}, subaccount_id=f"{exchange}.{subaccount_name or 'main'}")

                new_subaccount = {
                    "api_key": api_key,
                    "api_secret": api_secret,
                    "passphrase": passphrase,
                    "api_key_name": account_data.get("apiKeyName", "xWings"),
                    "login_name": account_data.get("loginName", subaccount_name or "unknown"),
                    "trading_config": {
                        "symbols": {
                            symbol: {
                                "timeframes": supported_timeframes,
                                "leverage": default_leverage,
                                "min_quantity": exchange_config.get("markets", {}).get("crypto", {}).get(market_type, {}).get("min_quantity", {}).get(symbol, 0.001)
                            }
                            for symbol in supported_symbols
                            for mt in market_types
                            if symbol in exchange_config.get("markets", {}).get("crypto", {}).get(mt, {}).get("symbols", [])
                        }
                    },
                    "account_type": "self" if not subaccount_name else "managed",
                    "account_mode": account_mode,
                    "xwings_uid": account_data.get("mainUid", account_data.get("accountId", "unknown")),
                    "exchange": exchange,
                    "IP": ip_address,
                    "Permissions": ["read", "trade"],
                    "fund_allocations": existing_allocations,
                    "symbol_allocations": existing_symbol_allocations,
                }

                try:
                    ClientCredentialsConfig(subaccounts={exchange: {new_subaccount["login_name"]: new_subaccount}})
                except ValueError as e:
                    logger.error(f"Invalid subaccount configuration for {exchange}.{new_subaccount['login_name']}: {str(e)}")
                    raise

                client_config_path = path_manager.get_config_path("client_credentials_encrypted.yaml")
                current_config = self.get_config("client_credentials_encrypted") or {"subaccounts": {exchange: {}}}
                if subaccount_name:
                    if exchange not in current_config["subaccounts"]:
                        current_config["subaccounts"][exchange] = {"subaccounts": {}}
                    current_config["subaccounts"][exchange]["subaccounts"][subaccount_name] = new_subaccount
                    logger.info(f"Updated nested subaccount {exchange}.{subaccount_name} with xwings_uid: {new_subaccount['xwings_uid']}")
                else:
                    current_config["subaccounts"][exchange] = current_config["subaccounts"].get(exchange, {})
                    current_config["subaccounts"][exchange][new_subaccount["login_name"]] = new_subaccount
                    logger.info(f"Updated main account {exchange}.{new_subaccount['login_name']} with xwings_uid: {new_subaccount['xwings_uid']}")

                try:
                    with client_config_path.open('wb') as f:
                        yaml_data = yaml.safe_dump(current_config, allow_unicode=True, sort_keys=False)
                        encrypted_data = self.fernet.encrypt(yaml_data.encode('utf-8'))
                        f.write(encrypted_data)
                    logger.info(f"Updated client_credentials_encrypted.yaml for {exchange}.{new_subaccount['login_name']} at {datetime.now(timezone(timedelta(hours=8))).isoformat()}")
                    self.reload_config("client_credentials_encrypted")
                except Exception as e:
                    logger.error(f"Failed to write client_credentials_encrypted.yaml: {str(e)}")
                    raise
            except Exception as e:
                logger.error(f"Failed to update {exchange} client credentials: {str(e)}")
                raise

    def update_markets_config(self, exchange: str, api_key: str, api_secret: str, passphrase: Optional[str] = None):
        """
        Update markets_encrypted.yaml with new API credentials for a specific exchange.

        Args:
            exchange (str): Exchange name (e.g., 'binance')
            api_key (str): New API key
            api_secret (str): New API secret
            passphrase (Optional[str]): New passphrase (if applicable)

        Raises:
            ValueError: If exchange not found or invalid credentials
        """
        with self.lock:
            try:
                markets_config = self.get_config("markets_encrypted")
                if not markets_config or exchange not in markets_config.get("exchanges", {}):
                    logger.error(f"Exchange {exchange} not found in markets_encrypted.yaml")
                    raise ValueError(f"Exchange {exchange} not found in markets_encrypted.yaml")
                
                exchange_config = markets_config["exchanges"][exchange]
                exchange_config["enabled"] = True
                if "api" not in exchange_config:
                    exchange_config["api"] = {}
                exchange_config["api"]["api_key"] = api_key
                exchange_config["api"]["api_secret"] = api_secret
                if passphrase:
                    exchange_config["api"]["passphrase"] = passphrase

                # Validate updated config
                ExchangeConfig(**exchange_config)

                # Save updated config
                config_path = path_manager.get_config_path("markets_encrypted.yaml")
                try:
                    with config_path.open('wb') as f:
                        yaml_data = yaml.safe_dump(markets_config, allow_unicode=True, sort_keys=False)
                        encrypted_data = self.fernet.encrypt(yaml_data.encode('utf-8'))
                        f.write(encrypted_data)
                    logger.info(f"Updated markets_encrypted.yaml for {exchange} at {datetime.now(timezone(timedelta(hours=8))).isoformat()}")
                    self.reload_config("markets_encrypted")
                except Exception as e:
                    logger.error(f"Failed to write markets_encrypted.yaml: {str(e)}")
                    raise
            except Exception as e:
                logger.error(f"Failed to update markets config for {exchange}: {str(e)}")
                raise

    def close_handlers(self):
        """Close all logging handlers and FileMonitor to release resources."""
        with self.lock:
            if self.file_monitor:
                self.file_monitor.stop()
                logger.debug("Stopped FileMonitor")
            for handler in logging.root.handlers[:]:
                try:
                    handler.close()
                    logging.root.removeHandler(handler)
                except Exception as e:
                    logger.error(f"Failed to close handler {handler}: {str(e)}")