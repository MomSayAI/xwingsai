from collections import defaultdict
import asyncio


class DataManager:
    def __init__(self):
        self._ha_open = defaultdict(dict)  # {(symbol, market_type): {timeframe: ha_open}}
        self._xWings = defaultdict(dict)
        self._lock = asyncio.Lock()

    async def update_all_indicators(self, symbol, market_type, timeframe, ha_open, xwings):
        async with self._lock:
            key = (symbol, market_type)
            self._ha_open[key][timeframe] = ha_open
            self._xWings[key][timeframe] = xwings

    async def get_cross_timeframe_open(self, symbol, market_type, timeframes):
        key = (symbol, market_type)
        result = {}
        for tf in timeframes:
            if tf in self._ha_open[key]:
                result[tf] = self._ha_open[key][tf]
        return result

    async def get_cross_timeframe_xWings(self, symbol, market_type, timeframes):
        key = (symbol, market_type)
        result = {}
        for tf in timeframes:
            if tf in self._xWings[key]:
                result[tf] = self._xWings[key][tf]
        return result
