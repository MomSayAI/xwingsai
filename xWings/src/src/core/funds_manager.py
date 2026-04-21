# File: src/core/funds_manager.py
# Purpose: Manage funds allocation and balance tracking for trading subaccounts
# Dependencies: asyncio, threading, yaml, pymongo, pandas, cryptography
# Notes: Timestamps in UTC+8. Supports both spot and perpetual markets.
#        Uses async factory pattern for initialization to handle database operations.
#        Integrated with FundsAllocationManager for persistent storage.
#        Supports nested subaccounts and dynamic fund allocation.
# Updated: 2025-08-05 20:25 PDT

import sys
import os
import time
from pathlib import Path
import logging
import asyncio
import threading
from typing import Dict, Optional, Any, List
from datetime import datetime, timezone, timedelta

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

try:
    from src.core.config_manager import ConfigManager
    from src.core.database import Database
    from src.core.funds_allocation_manager import FundsAllocationManager
    from src.core.trading_state import TradingState
    from src.config.schema import AllocationConfig, SymbolAllocationConfig
    from src.core.utils import PathManager
except ModuleNotFoundError as e:
    print(f"Module import error: {e}. Ensure src/core/ contains __init__.py and all required modules.")
    raise

def get_nested_value(data: Dict, keys: List[str], default: Any = None) -> Any:
    """
    Safely retrieve a nested value from a dictionary.
    """
    for key in keys:
        if isinstance(data, dict) and key in data:
            data = data[key]
        else:
            return default
    return data

logger = logging.getLogger(__name__)

class FundsManager:
    """
    Funds manager - responsible for subaccount funds allocation, balance tracking, and position management.

    Uses async factory pattern:
    - __init__: Only declares attributes, no business initialization.
    - create: Async factory method, completes all initialization tasks.
    - All FundsManager instances must be created via await FundsManager.create().
    """
    
    def __init__(self, subaccount_id: str, exchange: Any, config_manager: Any, account_config: Dict, database: Database):
        """
        Initialize FundsManager.

        Args:
            subaccount_id (str): Subaccount ID.
            exchange (Any): Exchange instance.
            config_manager (Any): Configuration manager instance.
            account_config (Dict): Specific account configuration.
            database (Database): Database instance.
        """
        # Base attributes
        self.subaccount_id = subaccount_id
        self.exchange = exchange
        self.config_manager = config_manager
        self.account_config = account_config
        self.db = database
        
        # Fund management attributes
        self.fund_allocations = {}
        self.symbol_allocations = {}
        self.total_balance = 0.0
        self.allocation_history = {}
        
        # Account configuration attributes
        self.account_type = None
        self.api_key = None
        self.api_secret = None
        self.trading_config = {}
        self.credentials = None
        
        # Component instances
        self.funds_allocation_manager = FundsAllocationManager(db=self.db)
        self.trading_state = TradingState(
            account_id=subaccount_id,
            database=self.db,
            exchange=self.exchange,
            config_manager=self.config_manager
        )
        
        # State-related attributes
        self.positions = {}
        self.lock = asyncio.Lock()
        
        logger.info(f"FundsManager for {subaccount_id} initialized with base attributes.")

    @classmethod
    async def create(cls, subaccount_id: str, exchange: Any, config_manager: Any, account_config: Dict, database: Database):
        """
        Async factory method to create and initialize a FundsManager instance.
        """
        instance = cls(subaccount_id, exchange, config_manager, account_config, database)
        await instance.initialize()
        return instance

    async def initialize(self):
        """
        Async initialization. Performs the following steps:
        1. Load and validate account credentials
        2. Validate account type and configuration
        3. Load and validate fund allocation rules
        4. Load historical positions
        """
        try:
            # 1. Load credentials
            await self._load_credentials()
            logger.info(f"Credentials loaded for {self.subaccount_id}")

            # 2. Validate account configuration
            await self._validate_subaccount_config()
            logger.info(f"Account config validated for {self.subaccount_id}")

            # 3. Load fund allocation rules
            await self.load_fund_allocations()
            logger.info(f"Fund allocations loaded for {self.subaccount_id}")

            # 4. Load historical positions
            await self.trading_state.load_positions()
            self.positions = self.trading_state.get_all_positions()
            logger.info(f"Positions loaded for {self.subaccount_id}")

            # 5. Update current balance
            await self.update_balance()
            logger.info(f"Balance updated for {self.subaccount_id}")
            logger.info(f"FundsManager for {self.subaccount_id} fully initialized with all validations complete.")
        except Exception as e:
            logger.error(f"Failed to initialize FundsManager for {self.subaccount_id}: {str(e)}")
            raise

    async def _load_credentials(self):
        """Load credentials configuration."""
        try:
            if not self.config_manager:
                raise ValueError("ConfigManager is required for loading credentials")
            self.credentials = self.config_manager.get_config("client_credentials_encrypted")
            if not self.credentials:
                raise ValueError("Failed to load client_credentials_encrypted.yaml")
            logger.debug(f"Credentials loaded for {self.subaccount_id}")
        except Exception as err:
            logger.error(f"Failed to load credentials for {self.subaccount_id}: {str(err)}")
            raise

    async def _validate_subaccount_config(self):
        """Validate subaccount configuration."""
        if not self.credentials or not self.subaccount_id:
            raise ValueError("Credentials and subaccount_id are required for validation")
        subaccount_config = self._find_subaccount(self.credentials.get('subaccounts', {}), self.subaccount_id)
        if not subaccount_config:
            raise ValueError(f"Subaccount {self.subaccount_id} not found in client_credentials_encrypted.yaml")

        # Validate account type
        self.account_type = subaccount_config.get('account_type')
        if not self.account_type:
            raise ValueError(f"Missing account_type for subaccount {self.subaccount_id}")

        if self.account_type not in ['self', 'managed']:
            raise ValueError(f"Invalid account_type for {self.subaccount_id}: {self.account_type}, must be 'self' or 'managed'")

        # Validate API credentials
        self.api_key = subaccount_config.get("api_key", "")
        self.api_secret = subaccount_config.get("api_secret", "")
        if not self.api_key or not self.api_secret:
            raise ValueError(f"Incomplete API credentials for subaccount {self.subaccount_id}")

        # Validate trading configuration
        self.trading_config = subaccount_config.get("trading_config", {}).get("symbols", {})
        if not self.trading_config:
            raise ValueError(f"No trading_config defined for subaccount {self.subaccount_id}")

        logger.debug(f"Subaccount config validated for {self.subaccount_id}")

    async def load_fund_allocations(self):
        """Load fund allocation configuration."""
        if not self.credentials or not self.subaccount_id or not self.db or not self.funds_allocation_manager:
            raise ValueError("Required components not initialized for loading fund allocations")
            
        subaccount_config = self._find_subaccount(self.credentials.get('subaccounts', {}), self.subaccount_id)
        
        try:
            # Load from database first, fallback to config
            allocation_doc = await self.db.find_one("funds_allocations", {"subaccount_id": self.subaccount_id})
            
            if allocation_doc:
                logger.info(f'Loading allocations from database for {self.subaccount_id}')
                self.fund_allocations = allocation_doc.get("allocations", {})
                self.symbol_allocations = allocation_doc.get("symbol_allocations", {})
            else:
                logger.info(f'Loading allocations from config for {self.subaccount_id}')
                self.fund_allocations = subaccount_config.get("fund_allocations", {})
                self.symbol_allocations = subaccount_config.get("symbol_allocations", {})
                
                # Save to database
                await self.funds_allocation_manager.save_allocation(
                    subaccount_id=self.subaccount_id,
                    allocation_config=self.fund_allocations,
                    symbol_allocations=self.symbol_allocations
                )
            
            # Validate configuration
            if not self.fund_allocations:
                raise ValueError(f"No fund_allocations defined for subaccount {self.subaccount_id}")
            if not self.symbol_allocations:
                raise ValueError(f"No symbol_allocations defined for subaccount {self.subaccount_id}")
            
            # Validate configuration format
            AllocationConfig(**self.fund_allocations, subaccount_id=self.subaccount_id)

            if not self.config_manager:
                raise ValueError("ConfigManager is required for market validation")
            markets_config = self.config_manager.get_config('markets_encrypted')
            exchange_name = self.subaccount_id.split('.')[0]
            valid_symbols = {
                'spot': markets_config.get('exchanges', {}).get(exchange_name, {}).get('markets', {}).get('crypto', {}).get('spot', {}).get('symbols', []),
                'perpetual': markets_config.get('exchanges', {}).get(exchange_name, {}).get('markets', {}).get('crypto', {}).get('perpetual', {}).get('symbols', [])
            }
            SymbolAllocationConfig(**self.symbol_allocations, market_conf_valid_symbols=valid_symbols, subaccount_id=self.subaccount_id)
            logger.debug(f"Fund allocations loaded for {self.subaccount_id}")
            
        except Exception as err:
            logger.error(f"Failed to load fund allocations for {self.subaccount_id}: {str(err)}")
            raise

    def _find_subaccount(self, subaccounts: Dict, subaccount_id: str) -> Optional[Dict]:
        """
        Find subaccount in configuration.

        Args:
            subaccounts (Dict): Subaccount dictionary
            subaccount_id (str): Subaccount identifier (e.g., okx.xWings, okx.MomSayAI)
            
        Returns:
            Optional[Dict]: Found subaccount configuration or None
        """
        return self.account_config

    def _find_subaccount_recursive(self, subaccounts: Dict, remaining_parts: List[str], parent_name: str) -> Optional[Dict]:
        """
        Recursively find nested subaccount.

        Args:
            subaccounts (Dict): Current level subaccount dictionary
            remaining_parts (List[str]): Remaining parts of subaccount path
            parent_name (str): Parent account name
            
        Returns:
            Optional[Dict]: Found subaccount configuration or None
        """
        if not remaining_parts:
            return None
        
        current_name = remaining_parts[0]
        
        if current_name in subaccounts:
            if len(remaining_parts) == 1:
                return subaccounts[current_name]
            else:
                return self._find_subaccount_recursive(subaccounts[current_name].get('subaccounts', {}), remaining_parts[1:], current_name)
        
        return None

    async def _update_allocation_history(self):
        """Update allocation history."""
        try:
            async with self.lock:
                current_time = datetime.now(timezone(timedelta(hours=8)))
                self.allocation_history[current_time.isoformat()] = {
                    'fund_allocations': self.fund_allocations.copy(),
                    'symbol_allocations': self.symbol_allocations.copy(),
                    'total_balance': self.total_balance
                }
                logger.debug(f"Updated allocation history for {self.subaccount_id}")
        except Exception as err:
            logger.error(f"Failed to update allocation history for {self.subaccount_id}: {str(err)}")

    def get_current_account_id(self) -> str:
        """Get current account ID."""
        return self.subaccount_id

    def set_exchange(self, exchange: Any) -> None:
        """
        Set exchange instance.

        Args:
            exchange (ExchangeAPI): Exchange API instance
        """
        self.exchange = exchange
        logger.debug(f"Exchange set for {self.subaccount_id}")

    async def update_balance(self, asset: str = "USDT") -> None:
        """
        Update account balance.

        Args:
            asset (str): Asset type, defaults to USDT
        """
        try:
            if not self.exchange:
                logger.warning(f"No exchange set for {self.subaccount_id}, cannot update balance")
                return
            
            async with self.lock:
                balance_data = await self.exchange.fetch_balance(self.subaccount_id, asset)
                print(f"balance_data:{balance_data}")
                # Compatible with okx.py return {'total': {...}}, prioritize asset-specific balance
                if 'total' in balance_data and isinstance(balance_data['total'], dict):
                    self.total_balance = float(balance_data['total'].get(asset, 0))
                elif 'free' in balance_data:
                    self.total_balance = float(balance_data.get('free', 0))
                else:
                    self.total_balance = float(balance_data.get(asset, 0))
                #print(f"Account balance：{self.total_balance}")
                # await self._reallocate_funds()

                logger.debug(f"Updated balance for {self.subaccount_id}: {self.total_balance} {asset}")
        except Exception as err:
            logger.error(f"Failed to update balance for {self.subaccount_id}: {str(err)}")

    async def _reallocate_funds(self) -> None:
        """Reallocate funds."""
        try:
            async with self.lock:
                # Recalculate allocations based on current balance
                total_available = self.total_balance
                for allocation_key, allocation_value in self.fund_allocations.items():
                    if isinstance(allocation_value, dict) and 'percentage' in allocation_value:
                        percentage = allocation_value['percentage']
                        allocated_amount = total_available * (percentage / 100)
                        self.fund_allocations[allocation_key]['allocated'] = allocated_amount
                await self._update_allocation_history()
                logger.debug(f"Reallocated funds for {self.subaccount_id}")
        except Exception as e:
            logger.error(f"Failed to reallocate funds for {self.subaccount_id}: {str(e)}")

    async def get_available_funds(self, market_type: str, symbol: str = None) -> float:
        """
        Get available funds.

        Args:
            market_type (str): Market type (spot/perpetual)
            symbol (str, optional): Trading pair symbol
        Returns:
            float: Available funds amount
        """
        
        try:
            logger.info(f"ligongxiang {market_type} ")
            async with self.lock:
                if market_type == 'perpetual':
                    default_allocation = self.fund_allocations.get('futures_trading', 0.1)
                elif market_type == 'spot':
                    default_allocation = self.fund_allocations.get('spot_trading', 0.0)
                if symbol and symbol in self.symbol_allocations.get(market_type, {}):
                    logger.info(f"ligongxiang veryGood")
                    max_allocation = self.symbol_allocations[market_type][symbol]
                    logger.info(f"max_allocation {max_allocation} {default_allocation} {max_allocation}")
                    return self.total_balance * default_allocation * max_allocation
                else:
                    return 0.0
                    # Use default allocation
                    # default_allocation = self.fund_allocations.get('default', {}).get('percentage', 10)
                    # return self.total_balance * (default_allocation / 100)
        except Exception as e:
            logger.error(f"Failed to get available funds for {self.subaccount_id}: {str(e)}")
            return 0.0

    async def allocate_funds(self, subaccount_id: str, symbol: str, signal: str, price: float, 
                           market_type: str, max_allocation: float = 0.1, leverage: float = None) -> float:
        """
        Allocate funds for trading.

        Args:
            subaccount_id (str): Subaccount ID
            symbol (str): Trading pair symbol
            signal (str): Trading signal (buy/sell)
            price (float): Trading price
            market_type (str): Market type (spot/perpetual)
        Returns:
            float: Allocated amount
        """
        try:
           await self.update_balance()
           # Get available funds for the specified symbol and market type, total balance * allocation ratio
           available_funds = await self.get_available_funds(market_type, symbol)
           print(f"Subaccount {subaccount_id}-{symbol}-{market_type} available funds: {available_funds}")
           logger.info(f"Good {subaccount_id}-{symbol}-{market_type} available funds: {available_funds}")
           # Consider leverage
           print(f"Retrieving trading leverage: {self.trading_config.get(symbol)}")
           if symbol not in self.trading_config.keys():
               print(f"Subaccount not configured for this symbol")
               leverage = 0
           else:
               leverage = float(self.trading_config.get(symbol)['leverage']) if market_type == 'perpetual' else 1.0
           print(f"Symbol leverage ratio {leverage}, adjusted amount: {available_funds * leverage}")
           quantity = (float(available_funds) * int(leverage)) / price if price > 0 else 0
           #quantity = 10
           print(f"Final {symbol} tradable quantity: {quantity}")
           logger.info(f"Allocated {available_funds} for {subaccount_id}/{symbol}/{market_type} with leverage: {leverage} (quantity: {quantity})")
           return quantity
        except Exception as err:
            logger.error(f"Failed to allocate funds for {subaccount_id}: {str(err)}")
            return 0.0

    async def get_position(self, subaccount_id: str, symbol: str, market_type: str) -> Dict:
        """
        Get current position.

        Args:
            subaccount_id (str): Subaccount ID
            symbol (str): Trading pair symbol
            market_type (str): Market type
            
        Returns:
            Dict: Position information
        """
        try:
            if not self.exchange:
                logger.warning(f"No exchange set for {self.subaccount_id}, cannot get position")
                return {}
            
            position_data = await self.exchange.fetch_position(subaccount_id, symbol, market_type)
            logger.debug(f"Retrieved position for {subaccount_id}/{symbol}/{market_type}: {position_data}")
            return position_data
        except Exception as e:
            logger.error(f"Failed to get position for {subaccount_id}/{symbol}/{market_type}: {str(e)}")
            return {}

    async def rollback_position(self, subaccount_id: str, symbol: str, market_type: str, previous_state: Dict) -> None:
        """
        Rollback position to previous state.

        Args:
            subaccount_id (str): Subaccount ID
            symbol (str): Trading pair symbol
            market_type (str): Market type
            previous_state (Dict): Previous state
        """
        try:
            async with self.lock:
                # Implement position rollback logic here
                logger.info(f"Rolling back position for {subaccount_id}/{symbol}/{market_type} to previous state")
                # Specific implementation depends on business requirements
        except Exception as e:
            logger.error(f"Failed to rollback position for {subaccount_id}/{symbol}/{market_type}: {str(e)}")

    def get_allocation_summary(self) -> Dict[str, float]:
        """
        Get allocation summary.

        Returns:
            Dict[str, float]: Allocation summary
        """
        try:
            summary = {
                'total_balance': self.total_balance,
                'fund_allocations': self.fund_allocations.copy(),
                'symbol_allocations': self.symbol_allocations.copy()
            }
            return summary
        except Exception as e:
            logger.error(f"Failed to get allocation summary for {self.subaccount_id}: {str(e)}")
            return {}

    def is_initialized(self) -> bool:
        """Check if initialized."""
        return self._initialized

    async def close(self):
        """Close resources."""
        try:
            if self.db:
                self.db.close()
            if self.funds_allocation_manager:
                self.funds_allocation_manager.close()
            logger.info(f"FundsManager closed for {self.subaccount_id}")
        except Exception as e:
            logger.error(f"Failed to close FundsManager for {self.subaccount_id}: {str(e)}")