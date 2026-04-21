# File: src/core/executor.py
# Purpose: Execute trading orders based on signals
# Dependencies: core.client_manager, core.exchange, core.database, core.trading_state, core.config_manager, core.utils
# Notes: Timestamps in UTC+8 via database.py. Supports nested subaccounts.
#        Compatible with Binance API changes as of 2025-06-04.
#        Enhanced with thread safety, async optimization, and detailed error handling.
#        Uses markets_encrypted.yaml for symbols and market types.
#        Adapted to new signal format from signal_generator.py.
# Updated: 2025-08-05 20:16 PDT

import sys
from pathlib import Path
import logging
from typing import Dict, Any, Optional, cast, Literal
from time import time
from datetime import datetime, timezone, timedelta
import asyncio

# Set project root and ensure paths
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.extend([str(path) for path in [PROJECT_ROOT, PROJECT_ROOT / 'src', PROJECT_ROOT / 'src/core'] if str(path) not in sys.path])

from src.core.database import Database
from src.core.client_manager import ClientManager
from src.core.trading_state import TradingState
from src.core.exchange import ExchangeAPI, ExchangeError
from src.core.config_manager import ConfigManager
from src.core.utils import PathManager
from src.core.funds_manager import FundsManager

# Initialize logging
path_manager = PathManager(PROJECT_ROOT)
logger = logging.getLogger(__name__)

class Executor:
    def __init__(self, subaccount_id: str, exchange_name: str, symbol: str, market_type: str,
                 client_manager: ClientManager, trading_state: TradingState, database: Database,
                 configs=None):
        """
        Initialize Executor for trading orders.

        Args:
            subaccount_id (str): Subaccount identifier
            exchange_name (str): Exchange name (e.g., binance, okx)
            symbol (str): Trading pair (e.g., BTC/USDT)
            market_type (str): Market type (spot, perpetual)
            client_manager (ClientManager): Client manager instance
            trading_state (TradingState): Trading state manager instance
            database (Database): Database instance
        """
        self.lock = asyncio.Lock()
        self.config_manager = configs
        self.markets = self.config_manager.get_config('markets_encrypted')
        self.credentials = self.config_manager.get_config('client_credentials_encrypted')
        self.subaccount_id = subaccount_id
        self.exchange_name = exchange_name.lower()
        self.symbol = symbol
        self.market_type = market_type
        self.client_manager = client_manager
        self.trading_state = trading_state
        self.database = database

        # Retrieve timeframes from subaccount config, fallback to system defaults
        subaccount_config = self.config_manager.get_subaccount_config(subaccount_id, exchange_name)

        trading_config = subaccount_config.get('trading_config', {}).get('symbols', {}).get(symbol, {})
        self.timeframes = trading_config.get('timeframes', self.markets.get('exchanges', {}).get(exchange_name, {}).get('timeframes', ['3m', '15m', '30m', '1h']))
        # self.config_manager.validate_subaccount(subaccount_id, symbol, self.timeframes)   # Redundant code, commented out
        subaccount_config = client_manager.subaccounts[subaccount_id]
        trading_config = subaccount_config.get('trading_config', {}).get('symbols', {}).get(symbol, {})

        self.min_quantity = self.markets.get('exchanges', {}).get(exchange_name, {}).get('markets', {}).get('crypto', {}).get(market_type, {}).get('min_quantity', {}).get(symbol, 0.001)
        self.exchange = self.client_manager.exchanges.get(subaccount_id)

        self.leverage = float(trading_config.get('leverage', 1.0)) if market_type == 'perpetual' else 1.0
        self.performance_metrics = {"order_execution_duration": []}
        logger.info(f"Executor initialized: subaccount={subaccount_id}, symbol={symbol}, market_type={market_type}, leverage={self.leverage}")

    async def execute_order(self, signal_data: Dict[str, Any], quantity: float, order_type: str = 'limit') -> Dict[str, Any]:
        """
        Execute order, supporting market order fallback to ensure complete position closure.

        Args:
            signal_data: Signal data
            quantity: Order quantity
            order_type: 'limit' or 'market'

        Returns:
            Dict: Order result
        """
        async with self.lock:
            try:
                start_time = time()
                signal = signal_data.get('signal')
                price = signal_data.get('price')
                timestamp = signal_data.get('timestamp')

                if not signal or not isinstance(signal, str):
                    logger.error(f"Invalid signal: {signal}")
                    raise ValueError(f"Invalid signal: {signal}")
                if quantity < self.min_quantity:
                    logger.error(f"Quantity {quantity} below minimum {self.min_quantity} for {self.symbol}")
                    raise ValueError(f"Quantity {quantity} below minimum {self.min_quantity}")
                if price is not None and price <= 0:
                    logger.error(f"Invalid price: {price}")
                    raise ValueError(f"Invalid price: {price}")
                if timestamp and not isinstance(timestamp, datetime):
                    logger.error(f"Invalid timestamp: {timestamp}")
                    raise ValueError(f"Invalid timestamp: {timestamp}")

                signal_type = signal.split(' at ')[0].capitalize() if ' at ' in signal else signal.capitalize()
                if signal_type not in ['Buy', 'Sell']:
                    logger.error(f"Invalid signal type: {signal_type}")
                    raise ValueError(f"Invalid signal type: {signal_type}")

                # Check current position
                current_position = await self.trading_state.get_position(self.subaccount_id, self.symbol, self.market_type)
                current_side = current_position.get('side')
                current_quantity = current_position.get('quantity', 0.0)

                # Determine if we need to close an existing position
                close_position = False
                close_quantity = 0.0
                close_side = None
                if signal_type == 'Buy' and current_side == 'short' and current_quantity > 0:
                    close_position = True
                    close_quantity = current_quantity
                    close_side = 'buy'
                elif signal_type == 'Sell' and current_side == 'long' and current_quantity > 0:
                    close_position = True
                    close_quantity = current_quantity
                    close_side = 'sell'

                # 1. Close existing position
                if close_position and close_quantity > 0:
                    logger.info(f"[Close Position] {self.subaccount_id} {self.symbol} {self.market_type} {current_side} quantity: {close_quantity}")
                    # Attempt limit order to close position
                    close_order = await self.exchange.place_order(
                        subaccount_id=self.subaccount_id,
                        symbol=self.symbol,
                        market_type=self.market_type,
                        side=close_side,
                        amount=close_quantity,
                        price=price if order_type == 'limit' else None,
                        client_order_id=None
                    )
                    executed_qty = float(close_order.get('filled', close_order.get('amount', 0)))
                    order_status = close_order.get('status', '')
                    # If partially filled, use market order to complete
                    if executed_qty < close_quantity:
                        remain_qty = close_quantity - executed_qty
                        logger.warning(f"[Partial Close] Remaining {remain_qty}, using market order to complete")
                        market_order = await self.exchange.place_order(
                            subaccount_id=self.subaccount_id,
                            symbol=self.symbol,
                            market_type=self.market_type,
                            side=close_side,
                            amount=remain_qty,
                            price=None,
                            client_order_id=None
                        )
                        executed_qty += float(market_order.get('filled', market_order.get('amount', 0)))
                    # Confirm complete closure
                    if executed_qty >= close_quantity:
                        logger.info(f"[Close Complete] {self.subaccount_id} {self.symbol} {self.market_type} fully closed")
                        # Update position
                        await self.trading_state.update_position(
                            symbol=self.symbol,
                            market_type=self.market_type,
                            position=None,
                            price=0.0,
                            quantity=0.0,
                            leverage=self.leverage
                        )
                    else:
                        logger.error(f"[Close Failed] {self.subaccount_id} {self.symbol} {self.market_type} not fully closed")
                        raise Exception("Failed to fully close position")

                # 2. Open new position
                # Determine order side
                side = 'buy' if signal_type == 'Buy' else 'sell'
                order = await self.exchange.place_order(
                    subaccount_id=self.subaccount_id,
                    symbol=self.symbol,
                    market_type=self.market_type,
                    side=side,
                    amount=quantity,
                    price=price if order_type == 'limit' else None,
                    client_order_id=None
                )
                order_id = order.get('orderId')
                order_status = order.get('status')
                executed_price = float(order.get('price', price or await self._fetch_current_price()))
                executed_quantity = float(order.get('amount', quantity))
                # Update trading state
                new_side = 'long' if signal_type == 'Buy' else 'short'
                await self.trading_state.update_position(
                    symbol=self.symbol,
                    market_type=self.market_type,
                    position=new_side,  # 'long' or 'short'
                    price=executed_price,
                    quantity=executed_quantity,
                    leverage=self.leverage
                )
                # Save order to database
                order_data = {
                    'subaccount_id': self.subaccount_id,
                    'symbol': self.symbol,
                    'market_type': self.market_type,
                    'order_id': order_id,
                    'side': side,
                    'type': order_type,
                    'quantity': executed_quantity,
                    'price': executed_price,
                    'status': order_status,
                    'timestamp': timestamp or datetime.now(timezone(timedelta(hours=8))),
                    'leverage': self.leverage,
                    'signal': signal
                }
                await self.database.save_trade(
                    subaccount_id=self.subaccount_id,
                    exchange_name=self.exchange_name,
                    symbol=self.symbol,
                    market_type=self.market_type,
                    signal=order_data['signal'],
                    price=order_data['price'],
                    quantity=order_data['quantity'],
                    fee=order_data.get('fee', 0.0),
                    timestamp=(timestamp.timestamp() * 1000) if isinstance(timestamp, datetime) else timestamp,
                    order_id=order_id,
                    leverage=self.leverage,
                    timeframe=None
                )
                self.performance_metrics["order_execution_duration"].append(time() - start_time)
                logger.info(f"Order executed: subaccount={self.subaccount_id}, symbol={self.symbol}, market_type={self.market_type}, signal={signal}, quantity={executed_quantity}, price={executed_price}, order_id={order_id}")
                return order_data

            except Exception as e:
                logger.error(f"Execute order error for {self.subaccount_id}/{self.symbol}/{self.market_type}: {str(e)}")
                raise

    async def _fetch_current_price(self) -> float:
        """
        Fetch the current market price for the symbol.

        Returns:
            float: Current price
        """
        try:
            ticker = await self.exchange.fetch_ticker(self.symbol, params={'market_type': self.market_type})
            return float(ticker.get('last', 0.0))
        except Exception as e:
            logger.error(f"Fetch current price error for {self.symbol}: {str(e)}")
            raise