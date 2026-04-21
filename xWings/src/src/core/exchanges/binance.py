# File: src/core/exchanges/binance.py
# Purpose: Binance exchange plugin for xWings trading system
# Notes: Supports batch orders and protection orders via Binance API.
#        Compatible with Binance API changes as of 2025-06-04.
#        Timestamps in UTC+8 for consistency with database.py and signal_generator.py.
#        Uses global logging configuration.
#        Added fetch_position and cancel_open_orders for FundsManager integration.
# Updated: 2025-07-06

import sys
import os
import logging
from time import time
import asyncio
import ccxt.async_support as ccxt
import websockets
import json
from typing import Dict, List, Callable, Optional
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Set working directory to project root and ensure paths
PROJECT_ROOT = Path(__file__).resolve().parents[3]
os.chdir(PROJECT_ROOT)
SRC_PATH = PROJECT_ROOT / 'src'
CORE_PATH = SRC_PATH / 'core'
for path in [PROJECT_ROOT, SRC_PATH, CORE_PATH]:
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)
        logging.debug(f"Added {path} to sys.path")

try:
    from core.utils import PathManager
    from core.exchange import ExchangeAPI, RateLimiter, ExchangeError
    from core.config_manager import ConfigManager
except ModuleNotFoundError as e:
    print(f"Module import error: {e}. Ensure src/core/ contains __init__.py and all required modules.")
    raise

# Initialize logger
logger = logging.getLogger(__name__)
logger.debug(f"Binance Exchange logging initialized with project_root: {PROJECT_ROOT}")

class BinanceExchange(ExchangeAPI):
    def __init__(self, subaccount_id: str = None, public_only: bool = False):
        super().__init__(None, None, None)
        self.exchange_name = "binance"
        self.config_manager = ConfigManager(key_file=str(PathManager(PROJECT_ROOT).get_config_path('encryption.key')))
        self.params = self.config_manager.get_config('parameters_encrypted')
        self.credentials = self.config_manager.get_config('client_credentials_encrypted')
        self.subaccount_id = subaccount_id
        if public_only or subaccount_id is None:
            # 公共行情实例，不需要API key/secret
            self.client = ccxt.binance({"enableRateLimit": True})
        else:
            subaccount_config = self._find_subaccount(self.credentials.get('subaccounts', {}), subaccount_id)
            if not subaccount_config:
                raise ExchangeError("INVALID_SUBACCOUNT", f"Subaccount {subaccount_id} not found")
            self.client = ccxt.binance({
                "apiKey": subaccount_config.get('api_key'),
                "secret": subaccount_config.get('api_secret'),
                "enableRateLimit": True
            })
        self.rate_limiter = RateLimiter(calls=20, period=1.0)
        self.ws_url = "wss://stream.binance.com:9443/ws"
        self.ws_subscriptions = {}
        self.ws_running = False
        self.kline_callbacks = {}
        logger.info(f"BinanceExchange initialized for subaccount {subaccount_id}")

    async def fetch_ohlcv(self, symbol: str, timeframe: str, market_type: str, since: int = None, limit: int = None) -> List[List[float]]:
        """
        Fetch OHLCV data from Binance.

        Args:
            symbol (str): Trading pair (e.g., BTC/USDT)
            timeframe (str): Timeframe (e.g., '3m', '15m')
            market_type (str): Market type (spot, perpetual)
            since (int, optional): Start time in milliseconds
            limit (int, optional): Number of candles

        Returns:
            List[List[float]]: OHLCV data [timestamp_ms, open, high, low, close, volume]
        """
        try:
            inst_id = symbol.replace("/", "")
            params = {'recvWindow': 5000}
            if market_type == 'perpetual':
                params['contractType'] = 'PERPETUAL'
            if since:
                params['startTime'] = since
            if limit:
                params['limit'] = min(limit, 1000)  # Binance max limit
            ohlcv = await self.client.fetch_ohlcv(inst_id, timeframe, params=params)
            logger.debug(f"Fetched OHLCV for {symbol}/{timeframe}/{market_type}: {len(ohlcv)} candles")
            return ohlcv
        except ccxt.AuthenticationError as e:
            raise ExchangeError("AUTHENTICATION_ERROR", str(e))
        except ccxt.NetworkError as e:
            logger.error(f"Network error fetching OHLCV for {symbol}: {str(e)}")
            raise ExchangeError("NETWORK_ERROR", str(e))
        except Exception as e:
            if '429' in str(e) or '-1003' in str(e):
                raise ExchangeError("RATE_LIMIT_EXCEEDED", str(e))
            if '-2038' in str(e) or '-1034' in str(e):
                raise ExchangeError(str(e).split()[0], str(e))
            raise ExchangeError("FETCH_OHLCV_FAILED", str(e))

    async def place_order(self, subaccount_id: str, symbol: str, market_type: str, side: str, amount: float, price: Optional[float], client_order_id: str = None) -> Dict:
        """
        Place an order on Binance.

        Args:
            subaccount_id (str): Subaccount identifier
            symbol (str): Trading pair
            market_type (str): Market type
            side (str): Order side (buy/sell)
            amount (float): Order quantity
            price (Optional[float]): Order price (None for market order)
            client_order_id (str, optional): Client order ID

        Returns:
            Dict: Order details
        """
        try:
            if subaccount_id != self.subaccount_id:
                raise ExchangeError("SUBACCOUNT_MISMATCH", f"Subaccount {subaccount_id} does not match {self.subaccount_id}")
            if symbol not in self.params.get('trading', {}).get('allowed_symbols', []):
                raise ExchangeError("INVALID_SYMBOL", f"Symbol {symbol} not allowed")
            params = {
                'recvWindow': 5000,
                'newClientOrderId': client_order_id or f"xwings-{subaccount_id}-{int(time())}"
            }
            if market_type == 'perpetual':
                params['contractType'] = 'PERPETUAL'
            order = await self.client.create_order(symbol, 'limit', side, amount, price, params=params)
            logger.info(f"Placed order {order['id']} for {symbol}: {side} {amount} @ {price}")
            return {'code': '0', 'orderId': order['id'], 'fee': order.get('fee', 0.0), 'status': order['status']}
        except ccxt.AuthenticationError as e:
            raise ExchangeError("AUTHENTICATION_ERROR", str(e))
        except ccxt.NetworkError as e:
            raise ExchangeError("NETWORK_ERROR", str(e))
        except Exception as e:
            if '429' in str(e) or '-1003' in str(e):
                raise ExchangeError("RATE_LIMIT_EXCEEDED", str(e))
            if '-2038' in str(e) or '-1034' in str(e):
                raise ExchangeError(str(e).split()[0], str(e))
            raise ExchangeError("ORDER_FAILED", str(e))

    async def place_batch_orders(self, subaccount_id: str, orders: List[Dict], is_protection: bool = False) -> List[Dict]:
        """
        Place batch orders on Binance.

        Args:
            subaccount_id (str): Subaccount identifier
            orders (List[Dict]): List of orders
            is_protection (bool): Protection order flag

        Returns:
            List[Dict]: Order results
        """
        try:
            if not orders:
                return []
            if subaccount_id != self.subaccount_id:
                raise ExchangeError("SUBACCOUNT_MISMATCH", f"Subaccount {subaccount_id} does not match {self.subaccount_id}")
            binance_orders = [
                {
                    'symbol': order['symbol'].replace("/", ""),
                    'side': order['side'].upper(),
                    'type': 'STOP_MARKET' if is_protection else 'LIMIT',
                    'quantity': str(order['quantity']),
                    'price': str(order['price'] if not is_protection else order.get('stop_price', order['price'])),
                    'timeInForce': 'GTC',
                    'recvWindow': 5000,
                    'newClientOrderId': order['client_order_id'],
                    **({'contractType': 'PERPETUAL'} if order['market_type'] == 'perpetual' else {})
                } for order in orders
            ]
            results = await self.client.create_orders(binance_orders)
            formatted_results = [{'code': '0', 'orderId': r['id'], 'fee': r.get('fee', 0.0), 'msg': r.get('msg', ''), 'status': r['status']} for r in results]
            logger.info(f"Placed batch orders for {subaccount_id}: {len(results)} orders")
            return formatted_results
        except ccxt.AuthenticationError as e:
            raise ExchangeError("AUTHENTICATION_ERROR", str(e))
        except ccxt.NetworkError as e:
            raise ExchangeError("NETWORK_ERROR", str(e))
        except Exception as e:
            if '429' in str(e) or '-1003' in str(e):
                raise ExchangeError("RATE_LIMIT_EXCEEDED", str(e))
            if '-2038' in str(e) or '-1034' in str(e):
                raise ExchangeError(str(e).split()[0], str(e))
            raise ExchangeError("BATCH_ORDER_FAILED", str(e))

    async def place_protection_order(self, subaccount_id: str, symbol: str, market_type: str, side: str, quantity: float, stop_price: float, client_order_id: str) -> Dict:
        """
        Place a protection order (stop-loss) on Binance.

        Args:
            subaccount_id (str): Subaccount identifier
            symbol (str): Trading pair
            market_type (str): Market type
            side (str): Order side
            quantity (float): Order quantity
            stop_price (float): Stop price
            client_order_id (str): Client order ID

        Returns:
            Dict: Order details
        """
        try:
            if subaccount_id != self.subaccount_id:
                raise ExchangeError("SUBACCOUNT_MISMATCH", f"Subaccount {subaccount_id} does not match {self.subaccount_id}")
            params = {
                'symbol': symbol.replace("/", ""),
                'side': side.upper(),
                'type': 'STOP_MARKET',
                'quantity': str(quantity),
                'stopPrice': str(stop_price),
                'timeInForce': 'GTC',
                'recvWindow': 5000,
                'newClientOrderId': client_order_id
            }
            if market_type == 'perpetual':
                params['contractType'] = 'PERPETUAL'
            result = await self.client.create_order(**params)
            logger.info(f"Placed protection order {result['id']} for {symbol}: {side} {quantity} @ {stop_price}")
            return {'code': '0', 'orderId': result['id'], 'fee': 0.0, 'status': 'open'}
        except ccxt.AuthenticationError as e:
            raise ExchangeError("AUTHENTICATION_ERROR", str(e))
        except ccxt.NetworkError as e:
            raise ExchangeError("NETWORK_ERROR", str(e))
        except Exception as e:
            if '-2038' in str(e) or '-1034' in str(e):
                raise ExchangeError(str(e).split()[0], str(e))
            raise ExchangeError("PROTECTION_ORDER_FAILED", str(e))

    async def set_leverage(self, symbol: str, leverage: int, margin_mode: str = 'isolated') -> None:
        """
        Set leverage for a symbol on Binance.

        Args:
            symbol (str): Trading pair
            leverage (int): Leverage multiplier
            margin_mode (str): Margin mode (isolated/cross)
        """
        try:
            params = {
                'symbol': symbol.replace("/", ""),
                'marginMode': margin_mode,
                'recvWindow': 5000
            }
            await self.client.set_leverage(leverage, symbol, params=params)
            logger.info(f"Set leverage {leverage}x for {symbol} in {margin_mode} mode")
        except Exception as e:
            raise ExchangeError("SET_LEVERAGE_FAILED", str(e))

    async def fetch_ticker(self, symbol: str, params: Dict = {}) -> Dict:
        """
        Fetch ticker data from Binance.

        Args:
            symbol (str): Trading pair
            params (Dict): Additional parameters

        Returns:
            Dict: Ticker data
        """
        try:
            inst_id = symbol.replace("/", "")
            params = {**params, 'recvWindow': 5000}
            if params.get('market_type') == 'perpetual':
                params['contractType'] = 'PERPETUAL'
            ticker = await self.client.fetch_ticker(inst_id, params=params)
            logger.debug(f"Fetched ticker for {symbol}: {ticker['last']}")
            return ticker
        except Exception as e:
            if '-2038' in str(e) or '-1034' in str(e):
                raise ExchangeError(str(e).split()[0], str(e))
            raise ExchangeError("FETCH_TICKER_FAILED", str(e))

    async def get_instrument_info(self, symbol: str, market_type: str) -> Dict:
        """
        Get instrument information from Binance.

        Args:
            symbol (str): Trading pair
            market_type (str): Market type

        Returns:
            Dict: Instrument details
        """
        try:
            inst_id = symbol.replace("/", "")
            params = {'recvWindow': 5000}
            if market_type == 'perpetual':
                params['contractType'] = 'PERPETUAL'
            result = await self.client.fetch_markets(params={'symbol': inst_id})
            instrument = result[0]
            info = {
                'minQty': float(instrument['limits']['amount']['min']),
                'lotSize': float(instrument['precision']['amount']),
                'tickSize': float(instrument['precision']['price'])
            }
            logger.debug(f"Fetched instrument info for {symbol}/{market_type}: {info}")
            return info
        except Exception as e:
            raise ExchangeError("INSTRUMENT_INFO_FAILED", str(e))

    async def fetch_balance(self, subaccount_id: str, asset: str = 'USDT') -> Dict:
        """
        Fetch balance for a subaccount.

        Args:
            subaccount_id (str): Subaccount identifier
            asset (str): Asset to fetch balance for (default: USDT)

        Returns:
            Dict: Balance details
        """
        try:
            if subaccount_id != self.subaccount_id:
                raise ExchangeError("SUBACCOUNT_MISMATCH", f"Subaccount {subaccount_id} does not match {self.subaccount_id}")
            balance = await self.client.fetch_balance(params={'recvWindow': 5000})
            total = float(balance.get('total', {}).get(asset, 0.0))
            logger.debug(f"Fetched balance for {subaccount_id}/{asset}: {total}")
            return {'total': {asset: total}}
        except Exception as e:
            raise ExchangeError("FETCH_BALANCE_FAILED", str(e))

    async def fetch_trading_fees(self, symbol: str, params: Dict = {}) -> Dict:
        """
        Fetch trading fees for a symbol.

        Args:
            symbol (str): Trading pair
            params (Dict): Additional parameters

        Returns:
            Dict: Fee details
        """
        try:
            inst_id = symbol.replace("/", "")
            params = {**params, 'recvWindow': 5000}
            if params.get('market_type') == 'perpetual':
                params['contractType'] = 'PERPETUAL'
            fees = await self.client.fetch_trading_fees(params={'symbol': inst_id})
            logger.debug(f"Fetched trading fees for {symbol}: {fees}")
            return fees
        except Exception as e:
            raise ExchangeError("FETCH_FEES_FAILED", str(e))

    async def fetch_asset_valuation(self, subaccount_id: str) -> Dict:
        """
        Fetch asset valuation for a subaccount.

        Args:
            subaccount_id (str): Subaccount identifier

        Returns:
            Dict: Valuation details
        """
        try:
            if subaccount_id != self.subaccount_id:
                raise ExchangeError("SUBACCOUNT_MISMATCH", f"Subaccount {subaccount_id} does not match {self.subaccount_id}")
            balance = await self.client.fetch_balance(params={'recvWindow': 5000})
            valuation = {asset: float(details['total']) for asset, details in balance.get('total', {}).items()}
            logger.debug(f"Fetched asset valuation for {subaccount_id}: {valuation}")
            return valuation
        except Exception as e:
            raise ExchangeError("FETCH_VALUATION_FAILED", str(e))

    async def fetch_position(self, subaccount_id: str, symbol: str, market_type: str) -> Dict:
        """
        Fetch current position for a subaccount, symbol, and market type.

        Args:
            subaccount_id (str): Subaccount identifier
            symbol (str): Trading pair
            market_type (str): Market type (spot, perpetual)

        Returns:
            Dict: Position details (quantity, side, avg_price)
        """
        try:
            if subaccount_id != self.subaccount_id:
                raise ExchangeError("SUBACCOUNT_MISMATCH", f"Subaccount {subaccount_id} does not match {self.subaccount_id}")
            inst_id = symbol.replace("/", "")
            params = {'recvWindow': 5000}
            if market_type == 'perpetual':
                params['contractType'] = 'PERPETUAL'
            positions = await self.client.fetch_position(inst_id, params=params)
            position = positions[0] if positions else {}
            result = {
                'quantity': float(position.get('contracts', 0.0)),
                'side': position.get('side', None),  # 'long', 'short', or None
                'avg_price': float(position.get('entryPrice', 0.0))
            }
            logger.debug(f"Fetched position for {subaccount_id}/{symbol}/{market_type}: {result}")
            return result
        except ccxt.AuthenticationError as e:
            raise ExchangeError("AUTHENTICATION_ERROR", str(e))
        except ccxt.NetworkError as e:
            raise ExchangeError("NETWORK_ERROR", str(e))
        except Exception as e:
            if '429' in str(e) or '-1003' in str(e):
                raise ExchangeError("RATE_LIMIT_EXCEEDED", str(e))
            raise ExchangeError("FETCH_POSITION_FAILED", str(e))

    async def cancel_open_orders(self, subaccount_id: str, symbol: str, market_type: str) -> None:
        """
        Cancel all open orders for a subaccount, symbol, and market type.

        Args:
            subaccount_id (str): Subaccount identifier
            symbol (str): Trading pair
            market_type (str): Market type (spot, perpetual)
        """
        try:
            if subaccount_id != self.subaccount_id:
                raise ExchangeError("SUBACCOUNT_MISMATCH", f"Subaccount {subaccount_id} does not match {self.subaccount_id}")
            inst_id = symbol.replace("/", "")
            params = {'recvWindow': 5000}
            if market_type == 'perpetual':
                params['contractType'] = 'PERPETUAL'
            await self.client.cancel_all_orders(inst_id, params=params)
            logger.info(f"Cancelled open orders for {subaccount_id}/{symbol}/{market_type}")
        except ccxt.AuthenticationError as e:
            raise ExchangeError("AUTHENTICATION_ERROR", str(e))
        except ccxt.NetworkError as e:
            raise ExchangeError("NETWORK_ERROR", str(e))
        except Exception as e:
            if '429' in str(e) or '-1003' in str(e):
                raise ExchangeError("RATE_LIMIT_EXCEEDED", str(e))
            raise ExchangeError("CANCEL_ORDERS_FAILED", str(e))

    async def get_server_time(self) -> int:
        """
        Get Binance server time in milliseconds.

        Returns:
            int: Server time in milliseconds
        """
        try:
            result = await self.client.publicGetTime()
            logger.debug(f"Fetched server time: {result['serverTime']}")
            return result['serverTime']
        except Exception as e:
            raise ExchangeError("FETCH_SERVER_TIME_FAILED", str(e))

    async def start_websocket(self):
        """
        Start WebSocket for real-time data with reconnection logic.
        Uses Binance WebSocket URL: wss://stream.binance.com:9443/ws
        """
        self.ws_running = True
        reconnect_delay = 5
        while self.ws_running:
            try:
                async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=10) as ws:
                    logger.info("Binance WebSocket connected")
                    for channel, params in self.ws_subscriptions.items():
                        subscription = {
                            "method": "SUBSCRIBE",
                            "params": [f"{params['symbol'].lower()}@kline_{params['timeframe']}"],
                            "id": int(time())
                        }
                        await ws.send(json.dumps(subscription))
                        logger.debug(f"Subscribed to {channel}")
                    while self.ws_running:
                        try:
                            message = await asyncio.wait_for(ws.recv(), timeout=30)
                            data = json.loads(message)
                            if 'e' in data and data['e'] == 'kline' and 'k' in data:
                                kline_data = data['k']
                                if kline_data['x']:  # Only process closed candles
                                    kline = [
                                        kline_data['t'],  # timestamp_ms
                                        float(kline_data['o']),  # open
                                        float(kline_data['h']),  # high
                                        float(kline_data['l']),  # low
                                        float(kline_data['c']),  # close
                                        float(kline_data['v'])   # volume
                                    ]
                                    channel = f"{data['s'].lower()}@kline_{kline_data['i']}"
                                    if channel in self.kline_callbacks:
                                        self.kline_callbacks[channel](kline)
                        except json.JSONDecodeError:
                            logger.error(f"Invalid JSON from Binance WebSocket: {message}")
                            raise ExchangeError("INVALID_JSON", "Invalid JSON from WebSocket")
                        except asyncio.TimeoutError:
                            logger.warning("WebSocket timeout, sending ping")
                            await ws.ping()
            except (websockets.ConnectionClosed, asyncio.TimeoutError) as e:
                logger.error(f"Binance WebSocket disconnected: {str(e)}, reconnecting in {reconnect_delay}s")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 60)  # Exponential backoff

    async def stop_ws(self):
        """Stop WebSocket connections."""
        self.ws_running = False
        self.ws_subscriptions.clear()
        logger.info("Binance WebSocket stopped")

    async def subscribe_kline(self, symbol: str, timeframe: str, market_type: str, callback: Callable):
        """
        Subscribe to Kline data via WebSocket.

        Args:
            symbol (str): Trading pair
            timeframe (str): Timeframe
            market_type (str): Market type
            callback (Callable): Callback function for kline data
        """
        channel = f"{symbol.replace('/', '').lower()}@kline_{timeframe}"
        self.ws_subscriptions[channel] = {"symbol": symbol.replace('/', ''), "timeframe": timeframe}
        self.kline_callbacks[channel] = callback
        logger.info(f"Subscribed to Binance Kline: {channel}")