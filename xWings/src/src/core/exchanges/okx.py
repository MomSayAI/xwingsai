# File: src/core/exchanges/okx.py
# Purpose: OKX exchange plugin for xWings trading system
# Notes: Supports batch orders and protection orders via OKX API v5.
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
import inspect

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
    from src.core.utils import PathManager
    from src.core.exchange import ExchangeAPI, RateLimiter, ExchangeError
    from src.core.config_manager import ConfigManager
except ModuleNotFoundError as e:
    print(f"Module import error: {e}. Ensure src/core/ contains __init__.py and all required modules.")
    raise

# Initialize logger
logger = logging.getLogger(__name__)
logger.debug(f"OKX Exchange logging initialized with project_root: {PROJECT_ROOT}")


class OkxExchange(ExchangeAPI):
    def __init__(self, subaccount_id: str = None, config: dict = None, public_only: bool = False):
        super().__init__(None, None, None)
        self.exchange_name = "okx"
        self.subaccount_id = subaccount_id
        if config and not public_only:
            self.client = ccxt.okx({
                "apiKey": config.get('api_key'),
                "secret": config.get('api_secret'),
                "password": config.get('passphrase'),
                "enableRateLimit": True
            })
        elif public_only or subaccount_id is None:
            self.client = ccxt.okx({"enableRateLimit": True})

        #self.client.httpProxy = 'http://127.0.0.1:10809'
        self.rate_limiter = RateLimiter(calls=20, period=1.0)
        self.ws_url = "wss://ws.okx.com:8443/ws/v5/public"
        
        # --- WebSocket V2: Connection Multiplexing ---
        self.ws = None
        self._ws_lock = asyncio.Lock()
        self._ws_subscriptions = {} # key: channel_instId, value: callback
        self._ws_handler_task = None
        self._is_ws_running = False
        # --- End WebSocket V2 ---

        logger.info(f"OKX Exchange initialized for subaccount {subaccount_id}")

    def get_current_account_name(self) -> str:
        return self.subaccount_id

    async def fetch_ohlcv(self, symbol: str, timeframe: str, market_type: str, since: int = None, limit: int = None) -> \
    List[List[float]]:
        """
        Fetch OHLCV data from OKX.

        Args:
            symbol (str): Trading pair (e.g., BTC/USDT)
            timeframe (str): Timeframe (e.g., '3m', '15m', '60m')
            market_type (str): Market type (spot, perpetual)
            since (int, optional): Start time in milliseconds
            limit (int, optional): Number of candles

        Returns:
            List[List[float]]: OHLCV data [timestamp_ms, open, high, low, close, volume]
        """
        try:
            # 验证输入参数
            if not symbol or not timeframe or not market_type:
                raise ExchangeError("INVALID_PARAMETERS", "Symbol, timeframe, and market_type are required")
            
            # 验证 market_type
            if market_type not in ['spot', 'perpetual']:
                raise ExchangeError("INVALID_MARKET_TYPE", f"Market type must be 'spot' or 'perpetual', got: {market_type}")
            
            # 验证 timeframe
            valid_timeframes = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '12h', '1d', '1w', '1M']
            if timeframe not in valid_timeframes:
                raise ExchangeError("INVALID_TIMEFRAME", f"Timeframe {timeframe} not supported. Valid: {valid_timeframes}")
            
            # 只进行必要的转换：60m -> 1h，让CCXT自动转换为1H
            # 其他timeframe直接使用
            if timeframe == '60m':
                okx_timeframe = '1h'
                logger.debug(f"Converted 60m to 1h for OKX API (will be auto-converted to 1H by CCXT)")
            else:
                okx_timeframe = timeframe
                logger.debug(f"Using timeframe directly: {okx_timeframe} for OKX API")

            # 构建 inst_id
            inst_id = symbol.replace("/", "-") + ("-SWAP" if market_type == 'perpetual' else "")
            
            # 构建参数
            params = {}
            if market_type == 'perpetual':
                params['instType'] = 'SWAP'
            else:
                params['instType'] = 'SPOT'
                
            if since:
                params['after'] = since
            if limit:
                params['limit'] = min(limit, 1000)  # OKX max limit

            logger.debug(f"Fetching OHLCV from OKX: inst_id={inst_id}, timeframe={okx_timeframe}, params={params}")
            
            # 添加重试机制
            max_retries = 3
            retry_delay = 1
            
            for attempt in range(max_retries):
                try:
                    ohlcv = await self.client.fetch_ohlcv(inst_id, okx_timeframe, params=params)
                    logger.debug(f"Fetched OHLCV for {symbol}/{okx_timeframe}/{market_type}: {len(ohlcv)} candles")
                    return ohlcv
                except Exception as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"Attempt {attempt + 1} failed for {symbol}/{timeframe}/{market_type}: {str(e)}")
                        await asyncio.sleep(retry_delay)
                        retry_delay *= 2  # 指数退避
                    else:
                        raise
        except ccxt.AuthenticationError as e:
            raise ExchangeError("AUTHENTICATION_ERROR", str(e))
        except ccxt.NetworkError as e:
            raise ExchangeError("NETWORK_ERROR", str(e))
        except Exception as e:
            if '429' in str(e) or '51023' in str(e):
                raise ExchangeError("RATE_LIMIT_EXCEEDED", str(e))
            if '51000' in str(e):
                logger.error(f"OKX timeframe error: {timeframe}, error: {str(e)}")
                logger.error(f"Supported timeframes: {timeframe}")
                logger.error(f"OKX supported timeframes: {timeframe}")
                raise ExchangeError("INVALID_TIMEFRAME", f"Timeframe {timeframe} not supported by OKX")
            raise ExchangeError("FETCH_OHLCV_FAILED", str(e))

    async def place_order(self, subaccount_id: str, symbol: str, market_type: str, side: str, amount: float,
                          price: Optional[float], client_order_id: str = None) -> Dict:
        """
        Place an order on OKX.

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
            logger.info("调用执行")
            params = {
                'instType': 'SWAP' if market_type == 'perpetual' else 'SPOT',
                'tdMode': 'isolated' if market_type == 'perpetual' else 'cash',
                # 'posSide': 'long' if side == 'buy' else 'short' if market_type == 'perpetual' else None,
                'instId': symbol.replace("/", "-") + (
                    "-SWAP" if market_type == 'perpetual' else ""),
                'clOrdId': client_order_id or f"{subaccount_id}{int(time())}".replace(".", "")
            }
            logger.info(f"调用执行 {params}")
            params = {k: v for k, v in params.items() if v is not None}
            # --- 兼容市价单 ---
            order_type = 'market' if price is None else 'limit'
            if order_type == 'market':
                order = await self.client.create_order(symbol, 'market', side, amount, None, params=params)
            else:
                order = await self.client.create_order(symbol, 'limit', side, amount, price, params=params)
            logger.info(f"Placed order {order['id']} for {symbol}: {side} {amount} @ {price}")
            return {'code': '0', 'orderId': order['id'], 'fee': order.get('fee', 0.0), 'status': order['status'], 'price': order.get('price'), 'amount': order.get('amount')}
        except ccxt.AuthenticationError as e:
            raise ExchangeError("AUTHENTICATION_ERROR", str(e))
        except ccxt.NetworkError as e:
            raise ExchangeError("NETWORK_ERROR", str(e))
        except Exception as e:
            if '429' in str(e) or '51023' in str(e):
                raise ExchangeError("RATE_LIMIT_EXCEEDED", str(e))
            raise ExchangeError("ORDER_FAILED", str(e))

    async def place_batch_orders(self, subaccount_id: str, orders: List[Dict], is_protection: bool = False) -> List[
        Dict]:
        """
        Place batch orders on OKX.

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
            logger.info("调用批量执行")
            okx_orders = [
                {
                    'instId': order['symbol'].replace("/", "-") + (
                        "-SWAP" if order['market_type'] == 'perpetual' else ""),
                    'tdMode': 'isolated' if order['market_type'] == 'perpetual' else 'cash',
                    'side': order['side'],
                    'ordType': 'limit' if not is_protection else 'conditional',
                    'sz': str(order['quantity']),
                    'px': str(order['price'] if not is_protection else order.get('stop_price', order['price'])),
                    'clOrdId': order['client_order_id']
                    # 'posSide': 'long' if order['side'] == 'buy' else 'short' if order[
                    #                                                                 'market_type'] == 'perpetual' else None
                } for order in orders
            ]
            logger.info(f"调用批量执行 {okx_orders}")
            okx_orders = [{k: v for k, v in o.items() if v is not None} for o in okx_orders]
            results = await self.client.private_post_trade_batch_orders({'body': okx_orders})
            formatted_results = [
                {'code': r['code'], 'orderId': r['ordId'], 'fee': r.get('fee', 0.0), 'msg': r.get('msg', ''),
                 'status': 'open'} for r in results['data']]
            logger.info(f"Placed batch orders for {subaccount_id}: {len(results['data'])} orders")
            return formatted_results
        except ccxt.AuthenticationError as e:
            raise ExchangeError("AUTHENTICATION_ERROR", str(e))
        except ccxt.NetworkError as e:
            raise ExchangeError("NETWORK_ERROR", str(e))
        except Exception as e:
            if '429' in str(e) or '51023' in str(e):
                raise ExchangeError("RATE_LIMIT_EXCEEDED", str(e))
            raise ExchangeError("BATCH_ORDER_FAILED", str(e))

    async def place_protection_order(self, subaccount_id: str, symbol: str, market_type: str, side: str,
                                     quantity: float, stop_price: float, client_order_id: str) -> Dict:
        """
        Place a protection order (stop-loss) on OKX.

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

            params = {
                'instId': symbol.replace("/", "-") + ("-SWAP" if market_type == 'perpetual' else ""),
                'tdMode': 'isolated',
                'side': side,
                'ordType': 'conditional',
                'sz': str(quantity),
                'slTriggerPx': str(stop_price),
                'slOrdPx': '-1',  # Market order on trigger
                'clOrdId': client_order_id
            }
            result = await self.client.private_post_trade_order_algo(params)
            logger.info(f"Placed protection order {result['algoId']} for {symbol}: {side} {quantity} @ {stop_price}")
            return {'code': '0', 'orderId': result['algoId'], 'fee': 0.0, 'status': 'open'}
        except ccxt.AuthenticationError as e:
            raise ExchangeError("AUTHENTICATION_ERROR", str(e))
        except ccxt.NetworkError as e:
            raise ExchangeError("NETWORK_ERROR", str(e))
        except Exception as e:
            raise ExchangeError("PROTECTION_ORDER_FAILED", str(e))

    async def set_leverage(self, symbol: str, leverage: int, margin_mode: str = 'isolated') -> None:
        """
        Set leverage for a symbol on OKX.

        Args:
            symbol (str): Trading pair
            leverage (int): Leverage multiplier
            margin_mode (str): Margin mode (isolated/cross)
        """
        try:
            params = {
                'instId': symbol.replace("/", "-") + "-SWAP",
                'lever': str(leverage),
                'mgnMode': margin_mode
            }
            await self.client.set_leverage(leverage, symbol, params=params)
            logger.info(f"Set leverage {leverage}x for {symbol} in {margin_mode} mode")
        except Exception as e:
            raise ExchangeError("SET_LEVERAGE_FAILED", str(e))

    async def fetch_ticker(self, symbol: str, params: Dict = {}) -> Dict:
        """
        Fetch ticker data from OKX.

        Args:
            symbol (str): Trading pair
            params (Dict): Additional parameters

        Returns:
            Dict: Ticker data
        """
        try:
            inst_id = symbol.replace("/", "-") + ("-SWAP" if params.get('market_type') == 'perpetual' else "")
            ticker = await self.client.fetch_ticker(inst_id, params)
            logger.debug(f"Fetched ticker for {symbol}: {ticker['last']}")
            return ticker
        except Exception as e:
            raise ExchangeError("FETCH_TICKER_FAILED", str(e))

    async def get_instrument_info(self, symbol: str, market_type: str) -> Dict:
        """
        Get instrument information from OKX.

        Args:
            symbol (str): Trading pair
            market_type (str): Market type

        Returns:
            Dict: Instrument details
        """
        try:
            inst_id = symbol.replace("/", "-") + ("-SWAP" if market_type == 'perpetual' else "")
            result = await self.client.public_get_public_instruments(
                {'instType': 'SWAP' if market_type == 'perpetual' else 'SPOT', 'instId': inst_id})
            instrument = result['data'][0]
            info = {
                'minQty': float(instrument['minSz']),
                'lotSize': float(instrument['lotSz']),
                'tickSize': float(instrument['tickSz'])
            }
            logger.debug(f"Fetched instrument info for {symbol}/{market_type}: {info}")
            return info
        except Exception as e:
            raise ExchangeError("INSTRUMENT_INFO_FAILED", str(e))

    async def fetch_balance(self, subaccount_id: str, asset: str = 'USDT') -> Dict:
        """
        Fetch balance for a subaccount. 返回所有币种所有账户类型余额。
        """
        try:
            # 查资金账户所有币种
            balance = await self.client.fetch_balance()
            total = balance.get('total', {})
            print(f"账户余额：{total}")
            # 查合约账户所有币种
            contract_balance = await self.client.fetch_balance({'type': 'swap'})
            contract_total = contract_balance.get('total', {})
            # 合并资金账户和合约账户余额
            for k, v in contract_total.items():
                if v and (k not in total or float(total.get(k, 0.0)) == 0):
                    total[k] = v
            # 只返回非零余额
            nonzero_total = {k: v for k, v in total.items() if v and abs(float(v)) > 1e-8}
            logger.debug(f"Fetched balance for {subaccount_id}: {nonzero_total}")
            return {'total': nonzero_total}
        except Exception as err:
            raise ExchangeError("FETCH_BALANCE_FAILED", str(err))

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
            inst_id = symbol.replace("/", "-") + ("-SWAP" if params.get('market_type') == 'perpetual' else "")
            fees = await self.client.fetch_trading_fees(params={'instId': inst_id})
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

            balance = await self.client.fetch_balance()
            valuation = {asset: float(details['total']) for asset, details in balance.get('total', {}).items()}
            logger.debug(f"Fetched asset valuation for {subaccount_id}: {valuation}")
            return valuation
        except Exception as e:
            raise ExchangeError("FETCH_VALUATION_FAILED", str(e))

    # 处理 fetch_position 返回结果的逻辑
    async def process_position_response(self, positions):
        """
        处理 OKX API fetch_position 返回结果
        如果返回是数组则获取 positions，如果是对象则直接使用
        """
        # 检查返回结果是否为数组
        if isinstance(positions, list) and len(positions) > 0:
            # 如果是数组且有数据，取第一个元素
            position = positions
        elif isinstance(positions, dict):
            # 如果是字典对象，直接使用
            position = positions
        else:
            # 如果返回为空或无效数据，返回默认值
            position = None

        return position

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

            inst_id = symbol.replace("/", "-") + ("-SWAP" if market_type == 'perpetual' else "")
            '''
            {'instType': 'SWAP'}：只获取永续合约仓位。
            {'instType': 'FUTURES'}：只获取交割合约仓位。
            {'instType': 'MARGIN'}：只获取杠杆现货仓位
            '''
            params = {'instType': 'SWAP' if market_type.lower() == 'perpetual' else 'MARGIN'}
            logger.info(f"Fetching position for {subaccount_id}/{symbol}/{market_type}")
            logger.info(f"InstId: {inst_id}, Params: {params}")
            positions = await self.client.fetch_position(inst_id, params=params)
            logger.info(f"positions: {positions}")
            processed_position = await self.process_position_response(positions)
            logger.info(f"position: {processed_position}")
            logger.info(f"I am  here")
            if processed_position:
                logger.info(f"I am  here2")
                # 解析持仓信息
                pos_side = processed_position.get('posSide', '')
                if pos_side == 'long' or float(processed_position.get('pos', 0)) > 0:
                    side = 'buy'
                elif pos_side == 'short' or float(processed_position.get('pos', 0)) < 0:
                    side = 'sell'
                else:
                    side = None
                result = {
                    'side': side,
                    'quantity': abs(float(processed_position.get('pos', 0))),
                    'avg_price': float(processed_position.get('avgPx', 0)),
                    'leverage': float(processed_position.get('lever', 1)),
                    'margin_mode': processed_position.get('mgnMode', ''),
                    'raw_data': processed_position
                }
                logger.info(f"I am  here3")
                logger.debug(f"Fetched position for {subaccount_id}/{symbol}/{market_type}: {result}")
                return result
            else:
                # 没有持仓数据
                logger.info(f"I am  here3")
                logger.debug(f"Fetched position for {subaccount_id}/{symbol}/{market_type}")
                return {'side': None, 'quantity': 0}
        except ccxt.AuthenticationError as e:
            raise ExchangeError("AUTHENTICATION_ERROR", str(e))
        except ccxt.NetworkError as e:
            raise ExchangeError("NETWORK_ERROR", str(e))
        except Exception as e:
            logger.info(f"I am  here4")
            if '429' in str(e) or '51023' in str(e):
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

            inst_id = symbol.replace("/", "-") + ("-SWAP" if market_type == 'perpetual' else "")
            params = {'instType': 'SWAP' if market_type == 'perpetual' else 'SPOT'}
            await self.client.cancel_all_orders(inst_id, params=params)
            logger.info(f"Cancelled open orders for {subaccount_id}/{symbol}/{market_type}")
        except ccxt.AuthenticationError as e:
            raise ExchangeError("AUTHENTICATION_ERROR", str(e))
        except ccxt.NetworkError as e:
            raise ExchangeError("NETWORK_ERROR", str(e))
        except Exception as e:
            if '429' in str(e) or '51023' in str(e):
                raise ExchangeError("RATE_LIMIT_EXCEEDED", str(e))
            raise ExchangeError("CANCEL_ORDERS_FAILED", str(e))

    async def _websocket_handler(self):
        """
        Handles a single, multiplexed WebSocket connection for all public subscriptions.
        Manages connection, reconnection, subscriptions, and message distribution.
        """
        reconnect_delay = 5
        max_reconnect_delay = 60
        
        while self._is_ws_running:
            try:
                logger.info("Connecting to OKX WebSocket...")
                self.ws = await websockets.connect(
                    self.ws_url,
                    ping_interval=None,  # We will handle ping/pong manually
                    ping_timeout=None,
                    close_timeout=10
                )
                logger.info("OKX WebSocket connected successfully.")
                reconnect_delay = 5 # Reset reconnect delay on successful connection

                # Subscribe to all pending channels
                sub_args = []
                for channel_key in self._ws_subscriptions.keys():
                    channel, instId = channel_key.split(':')
                    sub_args.append({"channel": channel, "instId": instId})
                
                if sub_args:
                    sub_msg = {"op": "subscribe", "args": sub_args}
                    await self.ws.send(json.dumps(sub_msg))
                    logger.info(f"Subscribed to {len(sub_args)}.")

                # Start a task to send pings periodically
                ping_task = asyncio.create_task(self._send_pings())

                # Main message processing loop
                while self._is_ws_running:
                    try:
                        msg = await asyncio.wait_for(self.ws.recv(), timeout=30)
                        data = json.loads(msg)
                        if data.get('event') == 'subscribe':
                            logger.debug(f"Subscription confirmation: {data}")
                            continue

                        if "data" in data and "arg" in data:
                            channel = data["arg"]["channel"]
                            instId = data["arg"]["instId"]
                            channel_key = f"{channel}:{instId}"
                            callback = self._ws_subscriptions.get(channel_key)

                            if callback:
                                for item in data["data"]:
                                    try:
                                        # 构造包含symbol信息的完整消息
                                        full_message = {
                                            "arg": data["arg"],
                                            "data": [item]
                                        }
                                        if inspect.iscoroutinefunction(callback):
                                            await callback(full_message)
                                        else:
                                            callback(full_message)
                                    except Exception as e:
                                        logger.error(f"Error in WebSocket callback for {channel_key}: {e}")
                    
                    except asyncio.TimeoutError:
                        # No message received for 30 seconds, connection might be stale.
                        # The ping task is already running to keep it alive.
                        logger.debug("WebSocket recv timed out. Ping task should be keeping it alive.")
                        continue # Continue waiting for messages
                    except websockets.ConnectionClosed as e:
                        logger.warning(f"OKX WebSocket connection closed: {e}")
                        break # Break inner loop to reconnect
                    except Exception as e:
                        logger.error(f"Error in WebSocket message handler: {e}")
                        break # Break inner loop to reconnect

            except Exception as e:
                logger.error(f"Failed to connect to OKX WebSocket: {e}")
            finally:
                if 'ping_task' in locals() and not ping_task.done():
                    ping_task.cancel()
                if self.ws:
                    await self.ws.close()
                    self.ws = None
            
            if self._is_ws_running:
                logger.info(f"Reconnecting to OKX WebSocket in {reconnect_delay} seconds...")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 1.5, max_reconnect_delay)

    async def _send_pings(self):
        """Periodically sends pings to keep the connection alive."""
        while self._is_ws_running and self.ws and not self.ws.closed:
            try:
                await self.ws.send("ping")
                logger.debug("Sent WebSocket ping.")
                await asyncio.sleep(25) # OKX requires activity every 30s
            except websockets.ConnectionClosed:
                logger.warning("Could not send ping, connection is closed.")
                break
            except Exception as e:
                logger.error(f"Error sending ping: {e}")
                break

    async def subscribe_ticker(self, symbol: str, market_type: str, callback: Callable):
        """
        Subscribes to a ticker channel. Manages a single multiplexed connection.
        """
        async with self._ws_lock:
            # --- 新增：ticker强制用public ws ---
            self.ws_url = "wss://ws.okx.com:8443/ws/v5/public"
            instId = symbol.replace("/", "-") + ("-SWAP" if market_type == "perpetual" else "")
            channel_key = f"tickers:{instId}"
            self._ws_subscriptions[channel_key] = callback
            
            # If the handler is not running, start it.
            if not self._is_ws_running:
                self._is_ws_running = True
                self._ws_handler_task = asyncio.create_task(self._websocket_handler())
                logger.info("Started main WebSocket handler.")

            # If already connected, send a new subscription message
            elif self.ws and not self.ws.closed:
                try:
                    sub_msg = {"op": "subscribe", "args": [{"channel": "trades", "instId": instId}]}
                    await self.ws.send(json.dumps(sub_msg))
                    logger.info(f"Sent new subscription for {instId}")
                except Exception as err:
                    logger.error(f"Failed to send subscription for {instId}: {err}")

    async def subscribe_candle(self, symbol: str, market_type: str, channel: str, callback: Callable):
        """
        订阅channel K线频道，管理单一多路复用连接。
        """
        async with self._ws_lock:
            # --- 新增：candle强制用business ws ---
            self.ws_url = "wss://ws.okx.com:8443/ws/v5/business"
            instId = symbol.replace("/", "-") + ("-SWAP" if market_type == "perpetual" else "")
            channel_key = f"{channel}:{instId}"
            self._ws_subscriptions[channel_key] = callback
            # 如果handler未运行，启动之
            if not self._is_ws_running:
                self._is_ws_running = True
                self._ws_handler_task = asyncio.create_task(self._websocket_handler())
                logger.info(f"Started main WebSocket handler for {channel}.")
            # 已连接则直接发订阅消息
            elif self.ws and not self.ws.closed:
                try:
                    sub_msg = {"op": "subscribe", "args": [{"channel": channel, "instId": instId}]}
                    await self.ws.send(json.dumps(sub_msg))
                    logger.info(f"Sent new {channel} subscription for {instId}")
                except Exception as err:
                    logger.error(f"Failed to send {channel} subscription for {instId}: {err}")

    async def fetch_ohlcv_public(self, symbol: str, timeframe: str, market_type: str, since: int = None,
                                 limit: int = None) -> List[List[float]]:
        """
        Fetch OHLCV data from OKX public API (no authentication required).
        This method avoids IP whitelist restrictions.

        Args:
            symbol (str): Trading pair (e.g., BTC/USDT)
            timeframe (str): Timeframe (e.g., '3m', '15m', '60m')
            market_type (str): Market type (spot, perpetual)
            since (int, optional): Start time in milliseconds
            limit (int, optional): Number of candles

        Returns:
            List[List[float]]: OHLCV data [timestamp_ms, open, high, low, close, volume]
        """
        try:
            from src.core.exchanges.okx_public import fetch_ohlcv_from_public

            logger.debug(f"Fetching OHLCV from OKX public API: {symbol}/{timeframe}/{market_type}")
            ohlcv = await fetch_ohlcv_from_public(symbol, timeframe, market_type, since, limit, use_proxy=True)
            logger.debug(f"Fetched {len(ohlcv)} OHLCV candles from public API for {symbol}/{timeframe}/{market_type}")
            return ohlcv

        except Exception as e:
            logger.error(f"Failed to fetch OHLCV from public API: {str(e)}")
            raise ExchangeError("PUBLIC_API_ERROR", str(e))

    async def get_position_mode(self, subaccount_id: str) -> str:
        """获取当前持仓模式"""
        try:

            result = await self.client.private_get_account_config()
            pos_mode = result['data'][0]['posMode']  # 'long_short_mode' 或 'net_mode'
            logger.info(f"Current position mode for {subaccount_id}: {pos_mode}")
            return pos_mode
        except Exception as e:
            raise ExchangeError("GET_POSITION_MODE_FAILED", str(e))

    async def set_position_mode(self, subaccount_id: str, mode: str = 'net_mode') -> None:
        """
        设置持仓模式。
        推荐使用 'net_mode'（单向持仓），这样反向信号时直接下 buy/sell 就能自动平仓。
        """
        try:

            current_mode = await self.get_position_mode(subaccount_id)
            if current_mode == mode:
                logger.info(f"Position mode already {mode} for {subaccount_id}, no change needed.")
                return

            if mode not in ['net_mode', 'long_short_mode']:
                raise ExchangeError("INVALID_POSITION_MODE", f"Invalid mode {mode}")

            params = {"posMode": mode}
            result = await self.client.private_post_account_set_position_mode(params)
            if result['code'] != '0':
                raise ExchangeError("SET_POSITION_MODE_FAILED", f"OKX error: {result.get('msg', '')}")

            logger.info(f"Successfully set position mode to {mode} for {subaccount_id}")
        except Exception as e:
            raise ExchangeError("SET_POSITION_MODE_FAILED", str(e))
