import asyncio
import time

import pandas as pd
from pathlib import Path
import numpy as np
import os
from datetime import datetime, timezone, timedelta

from src.core.signal_generator import SignalGenerator
import logging

logger = logging.getLogger(__name__)


class MarketDataCenter:
    """
    行情中心，负责采集所有symbol+timeframe的行情，只采集一次，缓存到内存。
    """

    def __init__(self, exchange, database=None):
        self.exchange = exchange
        self.database = database
        self.kline_cache = {}  # {(symbol, timeframe, market_type): pd.DataFrame}
        self.subscribers = {}  # {(symbol, timeframe, market_type): [callback, ...]}
        self.lock = asyncio.Lock()
        self.signal_generators = {}  # {(symbol, timeframe, market_type): SignalGenerator}
        # --- 健康监控 ---
        self.ticker_health = {}  # key: (symbol, market_type), value: {'last_msg_time': datetime, 'count': int}
        # 新增：k线健康监控
        self.candle_health = {}  # {(symbol, timeframe, market_type): {'last_msg_time': datetime, 'count': int}}

    async def start(self, symbols, timeframes, market_types, max_data_points=100):
        for symbol in symbols:
            for market_type in market_types:
                for timeframe in timeframes:
                    key = (symbol, timeframe, market_type)
                    if key not in self.signal_generators:
                        self.signal_generators[key] = SignalGenerator(
                            exchange=self.exchange,
                            symbol=symbol,
                            market_type=market_type,
                            timeframe=timeframe,
                            database=self.database
                        )
                        logger.info(f"[MarketDataCenter] 初始化SignalGenerator: {symbol}/{market_type}/{timeframe}")
                        await self.signal_generators[key].initialize()

                        asyncio.create_task(self._fetch_kline_history(symbol, timeframe, market_type, max_data_points))
                    else:
                        logger.warning(f"[MarketDataCenter] SignalGenerator已存在: {symbol}/{market_type}/{timeframe}，跳过初始化。")

    async def _fetch_kline_history(self, symbol, timeframe, market_type, max_data_points):
        try:
            ohlcv = await self.exchange.fetch_ohlcv(
                symbol=symbol,
                timeframe=timeframe,
                market_type=market_type,
                since=None,
                limit=max_data_points
            )
            if ohlcv:
                PROJECT_ROOT = Path(__file__).resolve().parents[2]
                key = (symbol, timeframe, market_type)
                collection_name = f"{symbol.replace('/', '_')}_{market_type}_{timeframe}"
                # 参考SignalGenerator格式处理
                df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']).assign(
                    symbol=symbol, market_type=market_type, timeframe=timeframe,
                    x=np.nan, Wings=np.nan, x_minus_Wings=np.nan, x_minus_Wings_delta=np.nan,
                    signal="Hold", trade_price=np.nan, extremum_type=None, extremum_value=np.nan
                )
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True).dt.tz_convert(
                    timezone(timedelta(hours=8)))
                df.set_index('timestamp', inplace=True)
                async with self.lock:
                    self.kline_cache[key] = df
                # 直接用SignalGenerator计算信号
                sg = self.signal_generators[key]
                sg.data = {(symbol, timeframe, market_type): df}
                # 由SignalGenerator内部完成所有计算
                sg.calculate_indicators((symbol, timeframe, market_type), False)
                # 从SignalGenerator取回处理后的DataFrame
                processed_df = sg.data[(symbol, timeframe, market_type)]
                # 保存K线数据到数据库（如有database）
                if sg.database:
                    ohlcv_data_list = []
                    for _, row in processed_df.iterrows():
                        ohlcv_data = {
                            'symbol': symbol,
                            'market_type': market_type,
                            'timeframe': timeframe,
                            'timestamp': row.name,
                            'open': float(row['open']),
                            'high': float(row['high']),
                            'low': float(row['low']),
                            'close': float(row['close']),
                            'confirm': 1,
                            'volume': float(row['volume'])
                        }
                        ohlcv_data_list.append(ohlcv_data)
                    if ohlcv_data_list:
                        await sg.database.save_ohlcv_batch(ohlcv_data_list)
                # 保存信号文件到csv
                # signals_dir = os.path.join(PROJECT_ROOT, "signals")
                # os.makedirs(signals_dir, exist_ok=True)
                # csv_path = os.path.join(signals_dir, f"{collection_name}.csv")
                # processed_df.to_csv(csv_path, index=False)
                # 可选：保存信号到数据库
                # if hasattr(sg, 'save_signals_to_database') and sg.database:
                #     await sg.save_signals_to_database((timeframe, symbol, market_type))
                # 通知订阅者
                for cb in self.subscribers.get((symbol, timeframe, market_type), []):
                    await cb(processed_df)
        except Exception as err:
            print(f"MarketDataCenter fetch error: {err}")

    def get_latest_kline(self, symbol, timeframe, market_type):
        return self.kline_cache.get((symbol, timeframe, market_type))

    def subscribe(self, symbol, timeframe, market_type, callback):
        self.subscribers.setdefault((symbol, timeframe, market_type), []).append(callback)

    async def start_websocket_ticker(self, symbols, market_types):
        """
        启动WebSocket ticker订阅，支持多个symbol和market_type，回调on_ticker_update
        """
        for symbol in symbols:
            for market_type in market_types:
                if hasattr(self.exchange, 'subscribe_ticker'):
                    await self.exchange.subscribe_ticker(symbol, market_type, self.on_ticker_update)
                else:
                    print(f"Exchange {self.exchange} 不支持ticker/kline订阅接口")

    async def start_websocket_candles(self, symbols, market_types, timeframes):
        """
        启动WebSocket candle订阅，支持多个symbol和market_type，回调on_candle_update
        """
        for symbol in symbols:
            for market_type in market_types:
                for timeframe in timeframes:
                    channel = self._convert_channel(timeframe)
                    if hasattr(self.exchange, 'subscribe_candle'):
                        await self.exchange.subscribe_candle(symbol, market_type, channel, self.on_candle_update)
                    else:
                        print(f"Exchange {self.exchange} 不支持{channel}订阅接口")

    def on_candle_update(self, candle_msg):
        """
        WebSocket candle 推送回调函数，自动更新current_kline并计算信号。
        """
        try:
            # 现在candle_msg是完整的WebSocket消息结构
            # 格式: {"arg": {"channel": "candle3m", "instId": "BTC-USDT"}, "data": [['1753597853000', '118293.9', ...]]}
            # print(f"收到完整candle消息: {candle_msg}")
            
            # 解析symbol和market_type
            inst_id = candle_msg.get('arg', {}).get('instId')
            channel = candle_msg.get('arg', {}).get('channel')
            k_timeframe = channel.replace('candle', '').lower()
            if not inst_id:
                # print("无法获取instId，跳过处理")
                return
                
            symbol = inst_id.replace('-SWAP', '').replace('-', '/')
            market_type = 'perpetual' if 'SWAP' in inst_id else 'spot'
            # print(f"解析得到: symbol={symbol}, market_type={market_type}")
            
            # 获取K线数据
            if 'data' not in candle_msg or not candle_msg['data']:
                print("没有data字段，跳过处理")
                return
                
            kline = candle_msg['data'][0]
            # print(f"K线数据: {kline}")
            
            kline_data = [
                int(float(kline[0])),  # timestamp(ms)
                float(kline[1]),       # open
                float(kline[2]),       # high
                float(kline[3]),       # low
                float(kline[4]),       # close
                float(kline[6]),       # volume
                int(kline[8]),       # confirm
            ]
            # print(f"解析后的kline_data: {kline_data}")
            
            # 新增：k线健康监控
            from datetime import datetime
            # print(f"当前signal_generators数量: {len(self.signal_generators)}")
            # print(f"当前signal_generators keys: {list(self.signal_generators.keys())}")
            # print(f"当前解析的symbol: {symbol}, market_type: {market_type}")
            
            for key_sg, sg in self.signal_generators.items():
                sg_symbol = key_sg[0]
                timeframe = key_sg[1]
                sg_market_type = key_sg[2]
                
                # print(f"检查SignalGenerator: {key_sg} (symbol={sg_symbol}, timeframe={timeframe}, market_type={sg_market_type})")
                # print(f"条件判断: sg_symbol({sg_symbol}) == symbol({symbol}) = {sg_symbol == symbol}")
                # print(f"条件判断: sg_market_type({sg_market_type}) == market_type({market_type}) = {sg_market_type == market_type}")
                
                # 修复：1s的candle数据用来更新所有timeframe的SignalGenerator
                # 只匹配symbol和market_type，不限制timeframe
                if sg_symbol == symbol and sg_market_type == market_type and k_timeframe == timeframe:
                    key = (symbol, timeframe, market_type)
                    now = datetime.now()
                    prev_count = self.candle_health.get(key, {}).get('count', 0)
                    self.candle_health[key] = {'last_msg_time': now, 'count': prev_count + 1}
                    # print(f"更新SignalGenerator: {key}")
                    asyncio.create_task(sg.update_kline(kline_data, timeframe, channel))

                else:
                    # print(f"跳过不匹配的SignalGenerator: {key_sg} vs 当前: {symbol}/{market_type}")
                    continue
        except Exception as e:
            print(f"on_candle_update error: {e}")
            import traceback
            traceback.print_exc()

    async def stop(self):
        """
        停止行情中心，释放资源。
        """
        # 如果有WebSocket连接或异步任务，需在此关闭/取消
        if hasattr(self.exchange, 'ws') and self.exchange.ws:
            try:
                await self.exchange.ws.close()
            except Exception as e:
                logger.warning(f"关闭WebSocket时异常: {e}")
        logger.info("MarketDataCenter stopped.")

    # def _get_interval(self, timeframe):
    #     # 例如 '3m' -> 180秒
    #     if timeframe.endswith('m'):
    #         return int(timeframe[:-1]) * 60
    #     elif timeframe.endswith('h'):
    #         return int(timeframe[:-1]) * 3600
    #     return 60
    #
    def on_ticker_update(self, ticker: dict):
        """
        WebSocket ticker 推送回调函数，自动更新current_kline并计算信号，并尝试归档K线。
        """
        try:
            inst_id = ticker.get('instId')
            if not inst_id:
                return

            symbol = inst_id.replace('-SWAP', '').replace('-', '/')
            market_type = 'perpetual' if 'SWAP' in inst_id else 'spot'
            price = float(ticker.get('last', 0))
            timestamp_ms = int(ticker.get('ts', 0)) if 'ts' in ticker else int(pd.Timestamp.now(tz='Asia/Shanghai').timestamp() * 1000)
            volume = float(ticker.get('vol24h', 0))

            # --- 健康监控 ---
            key = (symbol, market_type)
            now = datetime.now()
            prev_count = self.ticker_health.get(key, {}).get('count', 0)
            self.ticker_health[key] = {'last_msg_time': now, 'count': prev_count + 1}

            # 2. 遍历所有相关的SignalGenerator实例
            for key_sg, sg in self.signal_generators.items():
                if key_sg[0] == symbol and key_sg[2] == market_type:
                    kline_data = [
                        timestamp_ms,
                        price,  # open TODO
                        price,  # high
                        price,  # low
                        price,  # close
                        volume  # volume
                    ]
                    timeframe = key_sg[1]
                    asyncio.create_task(sg.update_kline(kline_data, timeframe, self._convert_channel(timeframe)))
                    # 新增：每次ticker后尝试归档K线
                    # asyncio.create_task(sg.try_finalize_forming_kline(timeframe))
        except Exception as e:
            print(f"on_ticker_update error: {e}")

    async def monitor_ticker_health(self, interval=10, timeout=30):
        """
        定时检查所有ticker订阅健康状态，超时未收到推送则warning。
        interval: 检查间隔秒数，timeout: 超时时间秒
        """
        while True:
            now = datetime.now()
            for key, info in self.ticker_health.items():
                delta = (now - info['last_msg_time']).total_seconds()
                if delta > timeout:
                    logger.warning(f"[WS健康] Ticker超时未收到: {key}, 已{delta:.1f}秒无推送")
                else:
                    logger.debug(f"[WS健康] Ticker正常: {key}, 距上次{delta:.1f}秒, 总推送{info['count']}")
            await asyncio.sleep(interval)


    def _convert_channel(self, timeframe: str) -> str:
        """
        Convert timeframe to OKX format.

        Args:
            timeframe (str): Timeframe (e.g., 3m, 15m, 1h)

        Returns:
            str: OKX channel format
        """
        timeframe_map = {
            '1w': 'candle1W',
            '1d': 'candle1D',
            '2d': 'candle2D',
            '3d': 'candle3D',
            '5d': 'candle5D',
            '12h': 'candle12H',
            '6h': 'candle6H',
            '4h': 'candle4H',
            '2h': 'candle2H',
            '1h': 'candle1H',
            '30m': 'candle30m',
            '15m': 'candle15m',
            '5m': 'candle5m',
            '3m': 'candle3m',
            # '1m': 'candle1m',
            # '1s': 'candle1s',
            '3mutc': 'candle3Mutc',
            '1mutc': 'candle1Mutc',
            '1wutc': 'candle1Wutc',
            '1dutc': 'candle1Dutc',
            '2dutc': 'candle2Dutc',
            '3dutc': 'candle3Dutc',
            '5dutc': 'candle5Dutc',
            '12hutc': 'candle12Hutc',
            '6hutc': 'candle6Hutc'
        }
        return timeframe_map.get(timeframe,None)