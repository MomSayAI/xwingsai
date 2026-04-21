# File: src/core/ohlcv_fetcher
# Purpose: Independent OHLCV data fetcher using public APIs.
# Notes: This module provides market data without API key authentication,avoiding IP whitelist restrictions.
# Updated: 2025-08-05

import os
import asyncio
import json
import logging
from typing import Dict, List, Optional
import aiohttp
import ccxt.async_support as ccxt
from .exchange import ExchangeError

# Initialize logger
logger = logging.getLogger(__name__)

class OHLCVFetcher:
    """Independent OHLCV data fetcher using public APIs."""
    
    def __init__(self, exchange_name: str = "okx"):
        """
        Initialize OHLCV Fetcher.

        Args:
            exchange_name (str): Exchange name (default: okx)
        """
        self.exchange_name = exchange_name.lower()
        self.session = None
        
        # Exchange-specific configurations
        self.exchange_configs = {
            "okx": {
                "base_url": "https://www.okx.com",
                "api_url": "https://www.okx.com/api/v5",
            }
        }
        
        if self.exchange_name not in self.exchange_configs:
            raise ValueError(f"Unsupported exchange: {exchange_name}")
        
        self.config = self.exchange_configs[self.exchange_name]
        
        # Rate limiting
        self.request_count = 0
        self.last_request_time = 0
        self.rate_limit = 20  # requests per second
        
        # CCXT client for fetching timeframes
        self.ccxt_client = getattr(ccxt, self.exchange_name)({
            'enableRateLimit': True,
            'sandbox': False
        })
        # Simple proxy configuration, following the approach in okx.py
        self.ccxt_client.httpProxy = 'http://127.0.0.1:10809'
        
        logger.info(f"OHLCV Fetcher initialized for {exchange_name}")

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session."""
        if self.session is None or self.session.closed:
            connector = aiohttp.TCPConnector(limit=100, limit_per_host=30)
            timeout = aiohttp.ClientTimeout(total=30, connect=10)
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            self.session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                headers=headers
            )
        
        return self.session

    async def _rate_limit(self):
        """Apply simple rate limiting."""
        current_time = asyncio.get_event_loop().time()
        if current_time - self.last_request_time < 1.0 / self.rate_limit:
            await asyncio.sleep(1.0 / self.rate_limit - (current_time - self.last_request_time))
        self.last_request_time = asyncio.get_event_loop().time()

    async def _make_request(self, endpoint: str, params: Dict = None) -> Dict:
        """
        Make HTTP request to exchange API.

        Args:
            endpoint (str): API endpoint
            params (Dict): Query parameters

        Returns:
            Dict: API response
        """
        await self._rate_limit()
        
        session = await self._get_session()
        url = f"{self.config['api_url']}/{endpoint}"
        
        try:
            # Use proxy settings from CCXT client
            proxy = self.ccxt_client.httpProxy if hasattr(self.ccxt_client, 'httpProxy') else None
            
            if proxy:
                async with session.get(url, params=params, proxy=proxy) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get('code') == '0':
                            return data
                        else:
                            raise ExchangeError("API_ERROR", f"{self.exchange_name.upper()} API error: {data}")
                    else:
                        raise ExchangeError("HTTP_ERROR", f"HTTP {response.status}: {await response.text()}")
            else:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get('code') == '0':
                            return data
                        else:
                            raise ExchangeError("API_ERROR", f"{self.exchange_name.upper()} API error: {data}")
                    else:
                        raise ExchangeError("HTTP_ERROR", f"HTTP {response.status}: {await response.text()}")
                        
        except aiohttp.ClientError as e:
            raise ExchangeError("NETWORK_ERROR", f"Network error: {str(e)}")
        except json.JSONDecodeError as e:
            raise ExchangeError("JSON_ERROR", f"JSON decode error: {str(e)}")
        except Exception as e:
            raise ExchangeError("REQUEST_ERROR", f"Request error: {str(e)}")

    async def _get_timeframe_map(self) -> Dict[str, str]:
        """
        Get timeframe mapping from CCXT exchange API.
        
        Returns:
            Dict[str, str]: Timeframe mapping from standard to exchange format
        """
        try:
            # Load markets to get timeframes
            await self.ccxt_client.load_markets()
            
            # Get timeframes from CCXT
            timeframes = self.ccxt_client.timeframes
            
            if not timeframes:
                logger.warning(f"No timeframes found for {self.exchange_name}, using default mapping")
                # Fallback to default mapping if CCXT doesn't provide timeframes
                return {
                    '1m': '1m', '3m': '3m', '5m': '5m', '15m': '15m', '30m': '30m',
                    '1h': '1H', '2h': '2H', '4h': '4H', '6h': '6H', '12h': '12H',
                    '1d': '1D', '1w': '1W', '1M': '1M'
                }
            
            logger.debug(f"CCXT timeframes for {self.exchange_name}: {timeframes}")
            
            # Create mapping from standard format to exchange format
            timeframe_map = {}
            for standard_tf, exchange_tf in timeframes.items():
                # Handle common variations
                if standard_tf in ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '12h', '1d', '1w', '1M']:
                    timeframe_map[standard_tf] = exchange_tf
                # Also map common variations
                elif standard_tf == '60m':
                    timeframe_map['1h'] = exchange_tf
                elif standard_tf == '1H':
                    timeframe_map['1h'] = exchange_tf
                elif standard_tf == '1D':
                    timeframe_map['1d'] = exchange_tf
                elif standard_tf == '1W':
                    timeframe_map['1w'] = exchange_tf
                elif standard_tf == '1M':
                    timeframe_map['1M'] = exchange_tf
            
            logger.debug(f"Timeframe mapping for {self.exchange_name}: {timeframe_map}")
            return timeframe_map
            
        except Exception as e:
            logger.error(f"Failed to get timeframes from CCXT for {self.exchange_name}: {str(e)}")
            # Fallback to default mapping
            return {
                '1m': '1m', '3m': '3m', '5m': '5m', '15m': '15m', '30m': '30m',
                '1h': '1H', '2h': '2H', '4h': '4H', '6h': '6H', '12h': '12H',
                '1d': '1D', '1w': '1W', '1M': '1M'
            }

    def _format_symbol(self, symbol: str, market_type: str) -> str:
        """
        Format symbol for exchange API.

        Args:
            symbol (str): Trading pair (e.g., BTC/USDT)
            market_type (str): Market type (spot, perpetual)

        Returns:
            str: Formatted symbol
        """
        if self.exchange_name == "okx":
            formatted = symbol.replace("/", "-")
            if market_type == "perpetual":
                formatted += "-SWAP"
            return formatted
        else:
            return symbol

    async def _convert_timeframe(self, timeframe: str) -> str:
        """
        Convert timeframe to exchange format using CCXT timeframes.

        Args:
            timeframe (str): Timeframe (e.g., 3m, 15m, 1h)

        Returns:
            str: Exchange timeframe format
        """
        timeframe_map = await self._get_timeframe_map()
        return timeframe_map.get(timeframe, timeframe)

    async def fetch_ohlcv(self, symbol: str, timeframe: str, market_type: str, 
                         since: Optional[int] = None, limit: Optional[int] = None) -> List[List[float]]:
        """
        Fetch OHLCV data from public API (no authentication required).

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
            if self.exchange_name == "okx":
                return await self._fetch_ohlcv_okx(symbol, timeframe, market_type, since, limit)
            else:
                raise ValueError(f"Unsupported exchange: {self.exchange_name}")
                
        except Exception as e:
            logger.error(f"Failed to fetch OHLCV: {str(e)}")
            raise

    async def _fetch_ohlcv_okx(self, symbol: str, timeframe: str, market_type: str, 
                              since: Optional[int] = None, limit: Optional[int] = None) -> List[List[float]]:
        """
        Fetch OHLCV data from OKX public API.

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
            okx_timeframe = await self._convert_timeframe(timeframe)
            
            params = {
                'instId': inst_id,
                'bar': okx_timeframe
            }
            
            if since:
                params['after'] = since
            if limit:
                params['limit'] = min(limit, 300)  # OKX max limit for public API
            
            logger.debug(f"Fetching OHLCV from {self.exchange_name.upper()} public API: inst_id={inst_id}, timeframe={okx_timeframe}, params={params}")
            
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
            logger.error(f"Failed to fetch OHLCV from {self.exchange_name.upper()}: {str(e)}")
            raise

    async def close(self):
        """Close the session and CCXT client."""
        if self.session and not self.session.closed:
            await self.session.close()
            logger.info("OHLCV Fetcher session closed")
        
        if self.ccxt_client:
            await self.ccxt_client.close()
            logger.info("OHLCV Fetcher CCXT client closed")

# Global instance for easy access
_ohlcv_fetcher = None

async def get_ohlcv_fetcher(exchange_name: str = "okx") -> OHLCVFetcher:
    """
    Get or create a global OHLCV fetcher instance.

    Args:
        exchange_name (str): Exchange name

    Returns:
        OHLCVFetcher: OHLCV fetcher instance
    """
    global _ohlcv_fetcher
    if _ohlcv_fetcher is None:
        _ohlcv_fetcher = OHLCVFetcher(exchange_name)
    return _ohlcv_fetcher

async def fetch_ohlcv_public(symbol: str, timeframe: str, market_type: str, 
                           since: Optional[int] = None, limit: Optional[int] = None,
                           exchange_name: str = "okx") -> List[List[float]]:
    """
    Convenience function to fetch OHLCV data from public API.

    Args:
        symbol (str): Trading pair (e.g., BTC/USDT)
        timeframe (str): Timeframe (e.g., '3m', '15m', '60m')
        market_type (str): Market type (spot, perpetual)
        since (int, optional): Start time in milliseconds
        limit (int, optional): Number of candles
        exchange_name (str): Exchange name (default: okx)

    Returns:
        List[List[float]]: OHLCV data [timestamp_ms, open, high, low, close, volume]
    """
    fetcher = await get_ohlcv_fetcher(exchange_name)
    return await fetcher.fetch_ohlcv(symbol, timeframe, market_type, since, limit)