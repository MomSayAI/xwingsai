# File: src/core/position_sync_manager.py
# Purpose: Manage periodic position synchronization between exchanges, database, and memory
# Dependencies: asyncio, logging, datetime, typing
# Notes: Supports configurable sync intervals and error handling
# Updated: 2025-08-05

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional
from pathlib import Path
import sys

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.core.database import Database
from src.core.trading_state import TradingState
from src.core.exchange import ExchangeAPI
from src.core.config_manager import ConfigManager
from src.core.utils import PathManager

logger = logging.getLogger(__name__)

class PositionSyncManager:
    """Position synchronization manager for periodic syncing of position data between exchanges, database, and memory"""
    
    def __init__(self, database: Database, config_manager: ConfigManager, 
                 sync_interval: int = 300, max_retries: int = 3):
        """
        Initialize the position synchronization manager

        Args:
            database (Database): Database instance
            config_manager (ConfigManager): Configuration manager
            sync_interval (int): Synchronization interval in seconds (default: 5 minutes)
            max_retries (int): Maximum number of retry attempts
        """
        self.database = database
        self.config_manager = config_manager
        self.sync_interval = sync_interval
        self.max_retries = max_retries
        self.running = False
        self.sync_task: Optional[asyncio.Task] = None
        self.trading_states: Dict[str, TradingState] = {}
        self.exchanges: Dict[str, ExchangeAPI] = {}
        self.last_sync_time: Dict[str, datetime] = {}
        self.sync_stats = {
            'total_syncs': 0,
            'successful_syncs': 0,
            'failed_syncs': 0,
            'last_sync': None
        }
        
        logger.info(f"PositionSyncManager initialized with {sync_interval}s interval")
    
    def register_trading_state(self, subaccount_id: str, trading_state: TradingState):
        """Register a trading state manager"""
        self.trading_states[subaccount_id] = trading_state
        logger.info(f"Registered trading state for {subaccount_id}")
    
    def register_exchange(self, subaccount_id: str, exchange: ExchangeAPI):
        """Register an exchange instance"""
        self.exchanges[subaccount_id] = exchange
        logger.info(f"Registered exchange for {subaccount_id}")
    
    async def start_sync_loop(self):
        """Start the periodic synchronization loop"""
        if self.running:
            logger.warning("Position sync loop already running")
            return
        
        self.running = True
        logger.info(f"Starting position sync loop with {self.sync_interval}s interval")
        
        try:
            while self.running:
                await self._perform_sync()
                await asyncio.sleep(self.sync_interval)
        except asyncio.CancelledError:
            logger.info("Position sync loop cancelled")
        except Exception as e:
            logger.error(f"Position sync loop error: {str(e)}")
        finally:
            self.running = False
    
    async def stop_sync_loop(self):
        """Stop the periodic synchronization loop"""
        self.running = False
        if self.sync_task and not self.sync_task.done():
            self.sync_task.cancel()
            try:
                await self.sync_task
            except asyncio.CancelledError:
                pass
        logger.info("Position sync loop stopped")
    
    async def _perform_sync(self):
        """Perform the synchronization operation"""
        sync_start = datetime.now(timezone(timedelta(hours=8)))
        logger.debug(f"Starting position sync at {sync_start}")
        
        sync_tasks = []
        for subaccount_id in self.exchanges.keys():
            if subaccount_id in self.trading_states:
                task = self._sync_subaccount_positions(subaccount_id)
                sync_tasks.append(task)
        
        if sync_tasks:
            results = await asyncio.gather(*sync_tasks, return_exceptions=True)
            
            # Count synchronization results
            successful = sum(1 for r in results if not isinstance(r, Exception))
            failed = len(results) - successful
            
            self.sync_stats['total_syncs'] += len(results)
            self.sync_stats['successful_syncs'] += successful
            self.sync_stats['failed_syncs'] += failed
            self.sync_stats['last_sync'] = sync_start
            
            sync_duration = (datetime.now(timezone(timedelta(hours=8))) - sync_start).total_seconds()
            logger.info(f"Position sync completed: {successful} successful, {failed} failed in {sync_duration:.2f}s")
        else:
            logger.warning("No subaccounts registered for position sync")
    
    async def _sync_subaccount_positions(self, subaccount_id: str):
        """Synchronize positions for a single subaccount"""
        for attempt in range(self.max_retries):
            try:
                exchange = self.exchanges[subaccount_id]
                trading_state = self.trading_states[subaccount_id]
                
                # Get subaccount configuration
                subaccount_config = self.config_manager.get_subaccount_config(subaccount_id, exchange.exchange_name)
                trading_config = subaccount_config.get('trading_config', {}).get('symbols', {})
                
                sync_count = 0
                for symbol, config in trading_config.items():
                    for market_type in ['spot', 'perpetual']:
                        try:
                            # Fetch latest position from exchange
                            position_data = await exchange.fetch_position(subaccount_id, symbol, market_type)
                            
                            # Update database
                            await self.database.save_position(
                                subaccount_id=subaccount_id,
                                symbol=symbol,
                                market_type=market_type,
                                position=position_data.get('side'),
                                entry_price=position_data.get('avg_price', 0.0),
                                quantity=position_data.get('quantity', 0.0),
                                leverage=config.get('leverage', 1.0)
                            )
                            
                            # Update in-memory state (if position exists)
                            if position_data.get('quantity', 0) > 0:
                                await trading_state.update_position(
                                    subaccount_id=subaccount_id,
                                    symbol=symbol,
                                    market_type=market_type,
                                    signal=f"{position_data.get('side', 'long').title()}",
                                    price=position_data.get('avg_price', 0.0),
                                    quantity=position_data.get('quantity', 0.0),
                                    leverage=config.get('leverage', 1.0)
                                )
                                sync_count += 1
                            
                        except Exception as e:
                            logger.warning(f"Failed to sync {subaccount_id}/{symbol}/{market_type}: {str(e)}")
                            continue
                
                self.last_sync_time[subaccount_id] = datetime.now(timezone(timedelta(hours=8)))
                logger.debug(f"Synced {sync_count} positions for {subaccount_id}")
                return True
                
            except Exception as e:
                logger.error(f"Position sync attempt {attempt + 1} failed for {subaccount_id}: {str(e)}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
                else:
                    raise
    
    async def force_sync(self, subaccount_id: Optional[str] = None):
        """Force synchronize positions for a specific subaccount or all subaccounts"""
        if subaccount_id:
            if subaccount_id in self.exchanges and subaccount_id in self.trading_states:
                await self._sync_subaccount_positions(subaccount_id)
                logger.info(f"Force synced positions for {subaccount_id}")
            else:
                logger.error(f"Subaccount {subaccount_id} not registered for sync")
        else:
            await self._perform_sync()
            logger.info("Force synced all positions")
    
    def get_sync_stats(self) -> Dict:
        """Retrieve synchronization statistics"""
        return {
            **self.sync_stats,
            'last_sync_times': {k: v.isoformat() for k, v in self.last_sync_time.items()},
            'registered_subaccounts': list(self.exchanges.keys()),
            'sync_interval': self.sync_interval,
            'running': self.running
        }
    
    async def health_check(self) -> Dict:
        """Perform health check"""
        health = {
            'status': 'healthy' if self.running else 'stopped',
            'registered_count': len(self.exchanges),
            'last_sync': self.sync_stats['last_sync'].isoformat() if self.sync_stats['last_sync'] else None,
            'sync_success_rate': 0.0
        }
        
        if self.sync_stats['total_syncs'] > 0:
            health['sync_success_rate'] = self.sync_stats['successful_syncs'] / self.sync_stats['total_syncs'] 
        
        return health