# File: src/core/exchanges/okx_public.py
# Purpose: OKX public data API for xWings trading system (no authentication required)
# Notes: Provides public market data without API key authentication
#        Avoids IP whitelist restrictions for market data access
# Updated: 2025-08-05

import sys
import os
import logging
import asyncio
import ccxt.async_support as ccxt
import aiohttp
import json
from typing import Dict, List, Optional
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
    from src.core.exchange import ExchangeError
except ModuleNotFoundError as e:
    print(f"Module import error: {e}. Ensure src/core/ contains __init__.py and all required modules.")
    raise

# Initialize logger
logger = logging.getLogger(__name__)
logger.debug(f"OKX Public API logging initialized with project_root: {PROJECT_ROOT}")

class OkxPublicAPI:
    """OKX Public API for market data without authentication."""
    
    def __init__(self, use_proxy: bool = True):
        """
        Initialize OKX Public API client.

        Args:
            use_proxy (bool): Whether to use proxy (default: True)
        """
        self.base_url = "https://www.okx.com"
        self.api_url = "https://www.okx.com/api/v5"
        self.session = None
        # Rate limiting
        self.request_count = 0
        self.last_request_time = 0
        self.rate_limit = 20  # requests per second
        
        logger.info("OKX Public API initialized")

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session."""
        if self.session is None or self.session.closed:
            connector = aiohttp.TCPConnector(limit=100, limit_per_host=30)
            timeout = aiohttp.ClientTimeout(total=30, connect=10)
            
            if self.use_proxy:
                self.session = aiohttp.ClientSession(
                    connector=connector,
                    timeout=timeout,
                    headers={
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                    }
                )
            else:
                self.session = aiohttp.ClientSession(
                    connector=connector,
                    timeout=timeout,
                    headers={
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                    }
                )
        
        return self.session

    async def _rate_limit(self):
        """Apply rate limiting to API requests."""
        current_time = asyncio.get_event_loop().time()
        if current_time - self.last_request_time < 1.0 / self.rate_limit:
            await asyncio.sleep(1.0 / self.rate_limit - (current_time - self.last_request_time))
        self.last_request_time = asyncio.get_event_loop().time()

    async def _make_request(self, endpoint: str, params: Dict = None) -> Dict:
        """
        Make HTTP request to OKX API.

        Args:
            endpoint (str): API endpoint
            params (Dict): Query parameters

        Returns:
            Dict: API response
        """
        await self._rate_limit()
        
        session = await self._get_session()
        url = f"{self.api_url}/{endpoint}"
        
        try:
            if self.use_proxy:
                async with session.get(url, params=params, proxy=self.proxy_url) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get('code') == '0':
                            return data
                        else:
                            raise ExchangeError("API_ERROR", f"OKX API error: {data}")
                    else:
                        raise ExchangeError("HTTP_ERROR", f"HTTP {response.status}: {await response.text()}")
            else:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get('code') == '0':
                            return data
                        else:
                            raise ExchangeError("API_ERROR", f"OKX API error: {data}")
                    else:
                        raise ExchangeError("HTTP_ERROR", f"HTTP {response.status}: {await response.text()}")
                        
        except aiohttp.ClientError as e:
            raise ExchangeError("NETWORK_ERROR", f"Network error: {str(e)}")
        except json.JSONDecodeError as e:
            raise ExchangeError("JSON_ERROR", f"JSON decode error: {str(e)}")
        except Exception as e:
            raise ExchangeError("REQUEST_ERROR", f"Request error: {str(e)}")

    def _format_symbol(self, symbol: str, market_type: str) -> str:
        """
        Format symbol for OKX API.

        Args:
            symbol (str): Trading pair (e.g., BTC/USDT)
            market_type (str): Market type (spot, perpetual)

        Returns:
            str: Formatted symbol
        """
        formatted = symbol.replace("/", "-")
        if market_type == "perpetual":
            formatted += "-SWAP"
        return formatted

    def _convert_timeframe(self, timeframe: str) -> str:
        """
        Convert timeframe to OKX format.

        Args:
            timeframe (str): Timeframe (e.g., 3m, 15m, 1h)

        Returns:
            str: OKX timeframe format
        """
        timeframe_map = {
            '1m': '1m',
            '3m': '3m',
            '5m': '5m',
            '15m': '15m',
            '30m': '30m',
            '1h': '1H',
            '2h': '2H',
            '4h': '4H',
            '6h': '6H',
            '12h': '12H',
            '1d': '1D',
            '1w': '1W',
            '1M': '1M'
        }
        return timeframe_map.get(timeframe, timeframe)

    async def fetch_ohlcv_public(self, symbol: str, timeframe: str, market_type: str, 
                                since: Optional[int] = None, limit: Optional[int] = None) -> List[List[float]]:
        """
        Fetch OHLCV data from OKX public API (no authentication required).

        Args:
            symbol (str): Trading pair (e.g., BTC/USDT)
            timeframe (str): Timeframe (e.g., '3m', '15m', '60m')
            market_type (str): Market type (spot, perpetual)
            since (int, optional): Start time in milliseconds
            limit (int, optional): Number of candles (max 300)

        Returns:
            List[List[float]]: OHLCV data [timestamp_ms, open, high, low, close, volume]
        """
        try:
            inst_id = self._format_symbol(symbol, market_type)
            okx_timeframe = self._convert_timeframe(timeframe)
            
            params = {
                'instId': inst_id,
                'bar': okx_timeframe
            }
            
            if since:
                params['after'] = since
            if limit:
                params['limit'] = min(limit, 300)  # OKX max limit for public API
            
            logger.debug(f"Fetching OHLCV from OKX public API: inst_id={inst_id}, timeframe={okx_timeframe}, params={params}")
            
            response = await self._make_request('market/candles', params)
            
            if not response.get('data'):
                logger.warning(f"No OHLCV data returned for {symbol}/{timeframe}/{market_type}")
                return []
            
            # Convert OKX format to standard OHLCV format
            ohlcv_data = []
            for candle in response['data']:
                # OKX format: [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
                ohlcv_data.append([
                    int(candle[0]),  # timestamp
                    float(candle[1]), # open
                    float(candle[2]), # high
                    float(candle[3]), # low
                    float(candle[4]), # close
                    float(candle[5])  # volume
                ])
            
            logger.debug(f"Fetched {len(ohlcv_data)} OHLCV candles for {symbol}/{timeframe}/{market_type}")
            return ohlcv_data
            
        except Exception as e:
            logger.error(f"Failed to fetch OHLCV from public API: {str(e)}")
            raise

    async def fetch_ticker_public(self, symbol: str, market_type: str) -> Dict:
        """
        Fetch ticker data from OKX public API.

        Args:
            symbol (str): Trading pair
            market_type (str): Market type

        Returns:
            Dict: Ticker data
        """
        try:
            inst_id = self._format_symbol(symbol, market_type)
            
            response = await self._make_request('market/ticker', {'instId': inst_id})
            
            if not response.get('data'):
                raise ExchangeError("NO_DATA", f"No ticker data for {symbol}")
            
            ticker_data = response['data'][0]
            
            return {
                'symbol': symbol,
                'last': float(ticker_data['last']),
                'bid': float(ticker_data['bidPx']),
                'ask': float(ticker_data['askPx']),
                'high': float(ticker_data['high24h']),
                'low': float(ticker_data['low24h']),
                'volume': float(ticker_data['vol24h']),
                'timestamp': int(ticker_data['ts'])
            }
            
        except Exception as e:
            logger.error(f"Failed to fetch ticker from public API: {str(e)}")
            raise

    async def fetch_order_book_public(self, symbol: str, market_type: str, depth: int = 20) -> Dict:
        """
        Fetch order book from OKX public API.

        Args:
            symbol (str): Trading pair
            market_type (str): Market type
            depth (int): Order book depth (max 400)

        Returns:
            Dict: Order book data
        """
        try:
            inst_id = self._format_symbol(symbol, market_type)
            
            response = await self._make_request('market/books', {
                'instId': inst_id,
                'sz': min(depth, 400)
            })
            
            if not response.get('data'):
                raise ExchangeError("NO_DATA", f"No order book data for {symbol}")
            
            book_data = response['data'][0]
            
            return {
                'symbol': symbol,
                'bids': [[float(price), float(size)] for price, size in book_data['bids']],
                'asks': [[float(price), float(size)] for price, size in book_data['asks']],
                'timestamp': int(book_data['ts'])
            }
            
        except Exception as e:
            logger.error(f"Failed to fetch order book from public API: {str(e)}")
            raise

    async def get_instruments_public(self, market_type: str, symbol: str = None) -> List[Dict]:
        """
        Get instrument information from OKX public API.

        Args:
            market_type (str): Market type (spot, perpetual)
            symbol (str, optional): Specific symbol

        Returns:
            List[Dict]: Instrument information
        """
        try:
            inst_type = "SWAP" if market_type == "perpetual" else "SPOT"
            
            params = {'instType': inst_type}
            if symbol:
                params['instId'] = self._format_symbol(symbol, market_type)
            
            response = await self._make_request('public/instruments', params)
            
            if not response.get('data'):
                return []
            
            instruments = []
            for inst in response['data']:
                instruments.append({
                    'symbol': inst['instId'],
                    'base_currency': inst['baseCcy'],
                    'quote_currency': inst['quoteCcy'],
                    'min_size': float(inst['minSz']),
                    'lot_size': float(inst['lotSz']),
                    'tick_size': float(inst['tickSz']),
                    'status': inst['state']
                })
            
            return instruments
            
        except Exception as e:
            logger.error(f"Failed to fetch instruments from public API: {str(e)}")
            raise

    async def close(self):
        """Close the aiohttp session."""
        if self.session and not self.session.closed:
            await self.session.close()
            logger.info("OKX Public API session closed")

# Convenience function for easy access
async def fetch_ohlcv_from_public(symbol: str, timeframe: str, market_type: str, 
                                 since: Optional[int] = None, limit: Optional[int] = None,
                                 use_proxy: bool = True) -> List[List[float]]:
    """
    Convenience function to fetch OHLCV data from OKX public API.

    Args:
        symbol (str): Trading pair (e.g., BTC/USDT)
        timeframe (str): Timeframe (e.g., '3m', '15m', '60m')
        market_type (str): Market type (spot, perpetual)
        since (int, optional): Start time in milliseconds
        limit (int, optional): Number of candles
        use_proxy (bool): Whether to use proxy

    Returns:
        List[List[float]]: OHLCV data
    """
    api = OkxPublicAPI(use_proxy=use_proxy)
    try:
        return await api.fetch_ohlcv_public(symbol, timeframe, market_type, since, limit)
    finally:
        await api.close()