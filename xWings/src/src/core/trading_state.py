# File: src/core/trading_state.py
# Purpose: Manage trading account position state
# Dependencies: core.database, core.config_manager
# Notes: All timestamps use UTC+8
# Updated: 2025-08-05 20:29 PDT

import sys
import os
import logging
import asyncio
import threading
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime, timezone, timedelta

# Set project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.extend([str(path) for path in [PROJECT_ROOT, PROJECT_ROOT / 'src', PROJECT_ROOT / 'src/core'] if str(path) not in sys.path])

from src.core.database import Database
from src.core.config_manager import ConfigManager
from src.core.utils import PathManager

# Initialize logging
path_manager = PathManager(PROJECT_ROOT)
logger = logging.getLogger(__name__)

class TradingState:
    """
    Manage position state for a single trading account.
    Responsibilities:
    1. Load and save position information from/to database
    2. Maintain real-time position state
    3. Provide interfaces for position query and update
    """
    
    def __init__(self, account_id: str, database: Optional[Database] = None, 
                 exchange: Any = None, config_manager: Optional[ConfigManager] = None):
        """
        Initialize trading state manager.

        Args:
            account_id: Account ID
            database: Database instance for persistence
            exchange: Exchange instance for real-time data
            config_manager: Configuration manager instance
        """
        self.account_id = account_id
        self.db = database
        self.exchange = exchange
        self.config_manager = config_manager
        
        # Internal state
        self.positions: Dict[str, Dict[str, Any]] = {}
        self.lock = asyncio.Lock()
        
        logger.info(f"TradingState initialized for account {account_id}")

    def _get_position_key(self, symbol: str, market_type: str) -> str:
        """Generate unique identifier for position."""
        return f"{symbol}:{market_type}"

    async def load_positions(self) -> None:
        """Load account position information from database."""
        if not self.db:
            logger.warning(f"No database configured for account {self.account_id}")
            return

        try:
            # Use find method to retrieve all positions for the account
            query = {"subaccount_id": self.account_id}
            positions = await self.db.find("positions", query)
            
            async with self.lock:
                self.positions.clear()
                for pos in positions:
                    key = self._get_position_key(pos['symbol'], pos['market_type'])
                    self.positions[key] = {
                        'position': pos.get('position'),
                        'entry_price': pos.get('entry_price', 0.0),
                        'quantity': pos.get('quantity', 0.0),
                        'leverage': pos.get('leverage', 1.0),
                        'timeframes': pos.get('timeframes', []),
                        'last_update': pos.get('last_update', datetime.now(timezone(timedelta(hours=8))))
                    }
            
            logger.info(f"Loaded {len(self.positions)} positions for account {self.account_id}")
        except Exception as e:
            logger.error(f"Failed to load positions for account {self.account_id}: {str(e)}")
            raise

    async def get_position(self, symbol: str, market_type: str) -> Dict[str, Any]:
        """
        Retrieve position information for a specific trading pair.

        Args:
            symbol: Trading pair
            market_type: Market type (spot/perpetual)
        Returns:
            Dictionary containing position information
        """
        key = self._get_position_key(symbol, market_type)
        async with self.lock:
            return self.positions.get(key, {
                'position': None,
                'entry_price': 0.0,
                'quantity': 0.0,
                'leverage': 1.0,
                'timeframes': [],
                'last_update': datetime.now(timezone(timedelta(hours=8)))
            })

    async def update_position(self, symbol: str, market_type: str, 
                            position: Optional[str], price: float, 
                            quantity: float, leverage: float = 1.0) -> None:
        """
        Update position information.

        Args:
            symbol: Trading pair
            market_type: Market type
            position: Position direction ('long'/'short'/None)
            price: Entry price
            quantity: Position quantity
            leverage: Leverage multiplier
        """
        if price <= 0 or quantity < 0:
            raise ValueError(f"Invalid price ({price}) or quantity ({quantity})")

        key = self._get_position_key(symbol, market_type)
        now = datetime.now(timezone(timedelta(hours=8)))
        
        position_data = {
            'position': position,
            'entry_price': price,
            'quantity': quantity,
            'leverage': leverage,
            'timeframes': self.positions.get(key, {}).get('timeframes', []),
            'last_update': now
        }

        try:
            # Update in-memory state
            async with self.lock:
                self.positions[key] = position_data.copy()

            # Persist to database
            if self.db:
                await self.db.update_one(
                    "positions",
                    {
                        "subaccount_id": self.account_id,
                        "symbol": symbol,
                        "market_type": market_type
                    },
                    {"$set": {
                        **position_data,
                        "subaccount_id": self.account_id,
                        "symbol": symbol,
                        "market_type": market_type
                    }},
                    upsert=True
                )
                
            logger.info(f"Updated position for {self.account_id}/{symbol}/{market_type}: "
                       f"{position} @ {price} x{quantity} ({leverage}x)")
                       
        except Exception as e:
            # Rollback in-memory state on error
            async with self.lock:
                if key in self.positions:
                    del self.positions[key]
            logger.error(f"Failed to update position for {self.account_id}/{symbol}/{market_type}: {str(e)}")
            raise

    def get_all_positions(self) -> Dict[str, Dict[str, Any]]:
        """Get a snapshot of all positions."""
        return self.positions.copy()

    async def close(self) -> None:
        """Close and clean up resources."""
        async with self.lock:
            self.positions.clear()
        logger.info(f"TradingState closed for account {self.account_id}")