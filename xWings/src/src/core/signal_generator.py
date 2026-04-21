# File: src/core/signal_generator.py
# 非交易版 
# Purpose: Generate real-time trading signals based on x and Wings indicators for multiple timeframes
#          with full position state machine and amplitude tracking for trade execution
# Dependencies: pandas, numpy, talib, pymongo, ccxt.async_support, core.config_manager, core.database, core.utils
# Notes: 
#   - Timestamps in UTC+8
#   - Uses markets_encrypted.yaml for timeframes and symbols
#   - Maintains 60 historical K-lines per timeframe
#   - Saves completed K-lines and signals to database
#   - Includes position state machine (open_side, position_side) for signal cycling
#   - Tracks amplitude extremes (historical_min_amplitude/max_amplitude) for entry price calculation
#   - Supports signal callbacks for online trading execution
#   - Features: 
#       * Real-time K-line updates with forming candle handling
#       * β-based signal generation (Buy/Sell on slope changes)
#       * Entry price calculation with historical min amplitude adjustment
#       * Profit calculation based on position cycling
#       * Maximum safe leverage estimation
# Updated: 2026-02-27
# Version: 2.1.4 (With c_avg restored + Fixed Window + Profit=0 default + Fixed signal_type)

import sys
from pathlib import Path
import pandas as pd
import numpy as np
import talib
import json
from typing import List, Union, Any, Dict, Tuple
from datetime import datetime, timezone, timedelta
import asyncio
import threading
from time import time
import logging
from collections import defaultdict
import warnings

warnings.filterwarnings("ignore", category=FutureWarning)
# Set project root and ensure paths
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.extend([str(path) for path in [PROJECT_ROOT, PROJECT_ROOT / 'src', PROJECT_ROOT / 'src/core'] if
                 str(path) not in sys.path])

from src.core.config_manager import ConfigManager
from src.core.database import Database
from src.core.utils import PathManager

logger = logging.getLogger(__name__)


class SignalGenerator:
    """
    信号生成器 - 负责K线数据处理和交易信号生成。
    主要功能：
    1. 维护K线数据
    2. 计算技术指标
    3. 生成交易信号
    """
    global_callback_registry = {}
    def __init__(self, exchange, symbol: str, market_type: str, timeframe: str, database: Database = None):
        """
        初始化信号生成器。

        Args:
            exchange: 交易所实例
            symbol: 交易对 (e.g., 'BTC/USDT')
            market_type: 市场类型 ('spot'/'perpetual')
            timeframes: 时间周期列表 (e.g., ['3m', '15m', '30m', '1h'])
            database: 数据库实例 (可选)
        """
        # 基础配置
        self.last_vol = {}
        self.exchange = exchange
        self.symbol = symbol
        self.market_type = market_type
        self.timeframe = timeframe  # 使用主程序传递的时间周期
        self.database = database
        self.subaccount_id = getattr(exchange, 'subaccount_id', None)
        # 数据存储
        self.data = {}  # 存储每个时间周期的K线数据
        self.current_klines = {}  # 存储当前正在形成的K线
        self.last_signal = {}  # 存储每个时间周期的最后一个信号
        self.is_initialized = {(symbol, timeframe, market_type): False}
        self._already_initialized = False
        self.last_written_kline_timestamp = {}  # 防止重复写入
        self.kline_locks = defaultdict(asyncio.Lock)  # key: (symbol, timeframe, market_type)
        self.signal_generated = {}  # 新增：每个K线周期是否已生成信号
        self.kline_init = {}
        self.prev_α = {}
        self.prev_β = {}
        self.prev_γ = {}
        self.prev_x_minus_Wings = {}
        self.pre_trade_price = {}
        self.prev_signal_type = {}  # 上一根K线的信号
        self.signal_type = {}       # 当前forming K线的信号
        self.alignes_timestamp = {}  # 新增：每个K线下个周期
        self.client_manager = None        # 性能指标
        self.extreme_tracker = {}    # 2026-02-19 极值追踪
        self.entry_price_config = {'adjustment_ratio': 0.90  # 2026-02-19 调整系数 
                                   }
        self.pre_entry_price = {}  # 存储每次交易的entry_price，用于下次计算profit
        self._extreme_tracker = {}    # 当前正在监测的tracker
        self._pending_amplitude = {}   # 新增：待存储的振幅数据（上一个信号的）
        self._has_first_signal = {}    # 新增：标记是否已经产生过第一个信号
        self.performance_metrics = {"kline_update_duration": []}
        self.signal_callbacks = []
        # 【新增】从全局注册表加载回调
        # 构造 Key 必须与 main.py 和 MarketDataCenter 中使用的顺序一致：(symbol, timeframe, market_type)
        registry_key = (symbol, timeframe, market_type)
        if registry_key in SignalGenerator.global_callback_registry:
            callbacks = SignalGenerator.global_callback_registry[registry_key]
            self.signal_callbacks.extend(callbacks)
            logger.info(f"[全局注册表] {registry_key} 初始化时加载了 {len(self.signal_callbacks)} 个回调")
        else:
            logger.debug(f"[全局注册表] {registry_key} 暂无注册回调")

        # 同步锁
        self.lock = threading.Lock()
        self.async_lock = asyncio.Lock()

        # 加载配置
        self._load_config()
        logger.info(f"SignalGenerator initialized for {self.subaccount_id or 'public'}/{symbol}/{market_type}")

        logger.info(
            f"[DEBUG INIT] Object ID: {id(self)}, Key: {registry_key}, Callbacks Count: {len(self.signal_callbacks)}, Global Registry Keys: {list(SignalGenerator.global_callback_registry.keys())}")

    def _load_config(self):
        """加载和初始化配置参数"""
        try:
            path_manager = PathManager(PROJECT_ROOT)
            self.config_manager = ConfigManager(key_file=str(path_manager.get_config_path('encryption.key')))
            self.config = self.config_manager.get_config('parameters_encrypted')

            # 配置参数
            self.max_data_points = self._get_nested_config('data.max_data_points', 60)
            self.signal_price_type = self._get_nested_config('signal.price_type', 'prev_close')
            # 指标参数
            self.indicators_config = {
                'x': {
                    'long_period': self._get_nested_config('indicators.x.long_period', 14),
                    'k_smooth': self._get_nested_config('indicators.x.k_smooth', 3)
                },
                'wings': {
                    'period': self._get_nested_config('indicators.Wings.period', 14),
                    'smooth_period': self._get_nested_config('indicators.Wings.smooth_period', 3),
                    'second_smooth_period': self._get_nested_config('indicators.Wings.second_smooth_period', 3)
                }
            }
            # 读取 historical_min 配置
            self.historical_min_config = self._get_nested_config('historical_min', {})
            # 初始化数据结构
            key = (self.symbol, self.timeframe, self.market_type)
            self.data[key] = pd.DataFrame(columns=[
                'open', 'high', 'low', 'close', 'volume',
                'symbol', 'market_type', 'timeframe', 'c_avg',
                'x', 'Wings', 'x_minus_Wings', 'α',
                'signal', 'trade_price', 'entry_price', 'profit'
            ])
            self.data[key].index.name = 'timestamp'
            self.prev_signal_type[key] = 'hold'
            self.signal_type[key] = 'hold'
            
        except Exception as e:
            logger.error(f"Failed to load configuration: {str(e)}")
            raise
    
    def _get_nested_config(self, key: str, default: Any = None) -> Any:
        """
        Access nested configuration value with default.

        Args:
            key (str): Configuration key
            default (Any): Default value

        Returns:
            Any: Configuration value or default
        """
        try:
            keys = key.split('.')
            value = self.config
            for k in keys:
                value = value.get(k, default)
                if value is None:
                    return default
            return value
        except Exception as e:
            logger.error(f"Failed to access config key {key}: {str(e)}")
            return default

    def _get_start_time(self):
        """
        Get start time from start_time.json, default to current UTC+8 time.

        Returns:
            datetime: Start time
        """
        # 行情数据获取开始时间
        start_time_path = PathManager(PROJECT_ROOT).get_config_path('start_time.json')
        try:
            start_time_config = json.load(open(start_time_path, 'r')).get('start_time',
                                                                          'dynamic') if start_time_path.exists() else 'dynamic'
            if start_time_config == 'dynamic':
                start_time = datetime.now(timezone(timedelta(hours=8)))
                json.dump({'start_time': start_time.isoformat()}, open(start_time_path, 'w'))
                return start_time
            return datetime.fromisoformat(start_time_config).replace(tzinfo=timezone(timedelta(hours=8)))
        except Exception as e:
            logger.warning(f"Invalid start_time.json: {str(e)}, using current time")
            start_time = datetime.now(timezone(timedelta(hours=8)))
            json.dump({'start_time': start_time.isoformat()}, open(start_time_path, 'w'))
            return start_time

    async def initialize(self):
        """
        初始化历史数据。
        1. 获取历史K线
        2. 计算技术指标
        3. 生成初始信号
        """
        if self._already_initialized:
            logger.warning(
                f"[防重复] SignalGenerator for {self.subaccount_id or 'public'}/{self.symbol}/{self.market_type} 已初始化，跳过initialize。")
            return
        self._already_initialized = True
        try:
            await self._initialize_historical_data(self.timeframe)
            key = (self.symbol, self.timeframe, self.market_type)
            self.is_initialized[key] = True
            logger.info(
                f"Initialized {self.subaccount_id or 'public'}/{self.symbol}/{self.market_type}/{self.timeframe}")
        except Exception as e:
            logger.error(f"Initialization failed: {str(e)}")
            raise

    async def _initialize_historical_data(self, timeframe: str):
        """
        Initialize historical K-line data for a specific timeframe.
        """
        import os
        async with self.async_lock:
            try:
                start_time = time()
                key = (self.symbol, timeframe, self.market_type)
                collection_name = f"{self.symbol.replace('/', '_')}_{self.market_type}_{timeframe}"
                # 使用私有接口获取K线数据，添加重试机制
                if hasattr(self.exchange, "fetch_ohlcv"):
                    max_retries = 3
                    retry_delay = 2
                    for attempt in range(max_retries):
                        try:
                            logger.info(
                                f"Attempting to fetch OHLCV data (attempt {attempt + 1}/{max_retries}) for {self.symbol}/{timeframe}/{self.market_type}")
                            ohlcv = await self.exchange.fetch_ohlcv(
                                symbol=self.symbol,
                                timeframe=timeframe,
                                market_type=self.market_type,
                                since=None,
                                limit=self.max_data_points
                            )
                            logger.info(
                                f"Successfully fetched {len(ohlcv) if ohlcv else 0} OHLCV candles for {self.symbol}/{timeframe}/{self.market_type}")
                            break
                        except Exception as e:
                            logger.warning(
                                f"Attempt {attempt + 1} failed for {self.symbol}/{timeframe}/{self.market_type}: {str(e)}")
                            if attempt < max_retries - 1:
                                logger.info(f"Retrying in {retry_delay} seconds...")
                                await asyncio.sleep(retry_delay)
                                retry_delay *= 2
                            else:
                                logger.error(
                                    f"All {max_retries} attempts failed for {self.symbol}/{timeframe}/{self.market_type}")
                                raise
                else:
                    raise ValueError("当前交易所实例不支持 fetch_ohlcv 方法")
                if not ohlcv:
                    logger.error(f"未获取到ohlcv数据")
                    raise ValueError("No OHLC data fetched")
                with self.lock:
                    df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']).assign(
                        symbol=self.symbol, market_type=self.market_type, timeframe=timeframe, c_avg=np.nan,
                        x=np.nan, Wings=np.nan, x_minus_Wings=np.nan, α=np.nan,
                        signal="Hold", trade_price=np.nan, entry_price=np.nan, profit=np.nan
                    )
                    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True).dt.tz_convert(
                        timezone(timedelta(hours=8)))
                    df.set_index('timestamp', inplace=True)
                    # 去除获取到的未完成的那根K线
                    # df = df.iloc[:-1]

                    self.data[key] = df
                    # 保存K线数据到数据库
                    if self.database:
                        ohlcv_data_list = []
                        for _, row in df.iterrows():
                            ohlcv_data = {
                                'symbol': self.symbol,
                                'market_type': self.market_type,
                                'timeframe': timeframe,
                                'timestamp': row.name,
                                'open': float(row['open']),
                                'high': float(row['high']),
                                'low': float(row['low']),
                                'close': float(row['close']),
                                'c_avg': float((row['open'] + row['high'] + row['low'] + row['close']) / 4),
                                'confirm': 1,
                                'volume': float(row['volume'])
                            }
                            ohlcv_data_list.append(ohlcv_data)
                        if ohlcv_data_list:
                            await self.database.save_ohlcv_batch(ohlcv_data_list)
                            logger.info(
                                f"Saved {len(ohlcv_data_list)} historical K-lines to database for {self.symbol}/{self.market_type}/{timeframe}")
                    # 根据数据开始计算
                    self.calculate_indicators(key, False)
                    self.is_initialized[key] = True
                    self.performance_metrics["kline_update_duration"].append(time() - start_time)

                    # 2026-02-21 添加类型检查日志 - 放在这里最合适
                    logger.debug(f"self.data[{key}] type after init: {type(self.data[key])}")

                logger.debug(f"Fetched {len(ohlcv)} K-lines for {timeframe} from private API")
                return
            except Exception as e:
                logger.error(f"Initialize historical data error for {key}: {str(e)}")
                raise

    def calculate_indicators(self, key: Tuple[str, str, str], signalFlg: bool):
        """
        Calculate x, Wings, x_minus_Wings, and delta indicators for historical data.
        注意：此函数仅在初始化时调用一次，计算所有历史K线的指标。
        """
        try:
            required_k_lines = max(
                self.indicators_config['x']['long_period'],
                self.indicators_config['wings']['period'] + self._get_nested_config('indicators.Wings.smooth_period',
                                                                                3) - 1 + self._get_nested_config(
                    'indicators.Wings.second_smooth_period', 3) - 1
            )
            # 保证'timestamp'为列且全为Timestamp类型
            df = self.data[key]
            if df.index.name == 'timestamp' or isinstance(df.index, pd.DatetimeIndex):
                df = df.reset_index()
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
            df = df.sort_values('timestamp').copy()

            if len(df) < required_k_lines:
                logger.debug(f"Insufficient historical data for {key}: {len(df)} < {required_k_lines}")
                return
        
            # 先计算 c_avg，再按 c_avg 口径做指标
            df['c_avg'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4
            df['c_avg'] = df['c_avg'].round(2)
            
            # 使用 c_avg 价格计算指标
            df = df.dropna(subset=['high', 'low', 'c_avg'])
            
            # 计算X指标
            if len(df) >= self.indicators_config['x']['long_period']:
                lowest_low = talib.MIN(df['low'], timeperiod=self.indicators_config['x']['long_period'])
                highest_high = talib.MAX(df['high'], timeperiod=self.indicators_config['x']['long_period'])
                rsv = 100 * (df['c_avg'] - lowest_low) / (highest_high - lowest_low).replace(0, np.nan)
                rsv = rsv.replace([np.inf, -np.inf], np.nan).ffill().clip(0, 100)
                alpha = 2 / (self._get_nested_config('indicators.x.k_smooth', 3) + 1)
                ema_period = int((2 / alpha) - 1)
                df['x'] = talib.EMA(rsv, timeperiod=ema_period)

            # 计算Wings指标
            if len(df) >= self.indicators_config['wings']['period']:
                df['prev_c_avg'] = df['c_avg'].shift(1).fillna(df['c_avg'].iloc[0])
                base_value = (df['c_avg'] > df['prev_c_avg']).rolling(
                    window=self.indicators_config['wings']['period']).mean() * 100
                base_value = base_value.replace([np.inf, -np.inf], np.nan).ffill().clip(0, 100)
                base_ma = talib.SMA(base_value, timeperiod=self._get_nested_config('indicators.Wings.smooth_period', 3))
                base_ma_double = talib.SMA(base_ma, timeperiod=self._get_nested_config('indicators.Wings.second_smooth_period', 3))
                df['Wings'] = -base_ma_double

            df[['x', 'Wings']] = df[['x', 'Wings']].round(2)
            df['x_minus_Wings'] = df['x'] - df['Wings']
        
            # 计算派生指标
            df['x_minus_Wings_avg'] = talib.EMA(df['x_minus_Wings'], timeperiod=3).round(2)
            df['α'] = df['x_minus_Wings_avg'].diff().round(2)
            df['β'] = df['α'].diff().round(2)
            df['γ'] = df['β'].diff().round(2)
            df['signal'] = "Hold"
            df['trade_price'] = np.nan
        
            # 填充空值
            df = df.fillna({
                'x_minus_Wings_avg': 0, 
                'α': 0, 
                'β': 0,
                'γ': 0,
                'x': 0, 
                'Wings': 0, 
                'x_minus_Wings': 0
            })

            # 更新 prev_α 供后续使用
            if len(df) > 0:
                self.prev_α[key] = df['α'].iloc[-1]
                if 'β' in df.columns and len(df) > 0:
                    self.prev_β[key] = df['β'].iloc[-1]
                if 'γ' in df.columns and len(df) > 0:
                    self.prev_γ[key] = df['γ'].iloc[-1]

            self.data[key] = df.sort_values('timestamp')
            logger.info(f"Calculated indicators for {key} - shape: {df.shape}")
            
        except Exception as e:
            logger.error(f"Calculate indicators error for {key}: {str(e)}")
            raise

    async def update_kline(self, kline: list, timeframe: str, channel: str) -> None:
        """
        实时更新当前正在形成的K线（current_klines），并在新K线开始时写入历史K线。
        K线数据更新后，自动计算指标并生成信号。
        :param kline: [timestamp, open, high, low, close, volume, confirm]
        :param timeframe: K线周期
        :param channel: 频道名
        """
        try:
            logger.info(f"update_kline {kline}")
            key = (self.symbol, timeframe, self.market_type)
            k_state = int(kline[6])
            
            # 计算c_avg
            c_avg = round((kline[1] + kline[2] + kline[3] + kline[4]) / 4, 2)
            
            # 更新当前K线
            self.current_klines[key] = {
                'timestamp': datetime.fromtimestamp(kline[0] / 1000, tz=timezone(timedelta(hours=8))),
                'open': kline[1],
                'high': kline[2],
                'low': kline[3],
                'close': kline[4],
                'volume': kline[5],
                'c_avg': c_avg,
                'confirm': k_state,
                'symbol': self.symbol,
                'market_type': self.market_type,
                'timeframe': timeframe,
                'x': np.nan,
                'Wings': np.nan,
                'x_minus_Wings': np.nan,
                'x_minus_Wings_avg': np.nan,
                'α': np.nan,
                'β': np.nan,
                'γ': np.nan,
                'signal': "Hold",
                'trade_price': np.nan,
                'entry_price': np.nan,
                'profit': np.nan,
            }
            
            # 确保self.data[key]是DataFrame
            if hasattr(self.data[key], '__await__') or asyncio.iscoroutine(self.data[key]):
                self.data[key] = await self.data[key]
                
            if not isinstance(self.data[key], pd.DataFrame):
                logger.error(f"self.data[{key}] is not a DataFrame: {type(self.data[key])}")
                return
            
            # 构建用于计算的DataFrame（历史数据 + 当前K线）
            historical_data = self.data[key]
            current_kline_df = pd.DataFrame([self.current_klines[key]])
            merged_data = pd.concat([historical_data, current_kline_df], ignore_index=True)
            merged_data = merged_data.drop_duplicates(subset=['timestamp'], keep='last')
            # 只计算当前K线的指标
            merged_data = await self.calculate_indicators_on_df(merged_data, key)
            
            # 更新self.data[key]为计算结果（历史数据 + 更新后的当前K线）
            self.data[key] = merged_data
            
            # 极值追踪
            self.track_extreme_after_signal(
                key,
                float(kline[4]),
                datetime.fromtimestamp(kline[0] / 1000, tz=timezone(timedelta(hours=8)))
            )
            
            # 记录回撤
            self.track_drawdown_pct(float(kline[4]), datetime.fromtimestamp(kline[0] / 1000, tz=timezone(timedelta(hours=8))))
            
            if k_state == 0:
                # K线未闭合，保存实时信号
                if not self.kline_init.get(key, False) and self.data[key] is not None and len(self.data[key]) > 0 and isinstance(self.data[key].iloc[-1], pd.Series):
                    self.kline_init[key] = True
                    raw_data = self.data[key].iloc[-1].to_dict()
                    signal_record_tmp = {
                        'timestamp': datetime.fromtimestamp(kline[0] / 1000, tz=timezone(timedelta(hours=8))),
                        'open': raw_data.get('open', np.nan),
                        'high': raw_data.get('high', np.nan),
                        'low': raw_data.get('low', np.nan),
                        'close': raw_data.get('close', np.nan),
                        'c_avg': raw_data.get('c_avg', np.nan),
                        'volume': kline[5],
                        'confirm': 0,
                        'symbol': self.symbol,
                        'market_type': self.market_type,
                        'timeframe': timeframe,
                        'x': raw_data.get('x', np.nan),
                        'Wings': raw_data.get('Wings', np.nan),
                    }
                    await self.database.insert("signals", signal_record_tmp)
                    
                if self.data[key] is not None and len(self.data[key]) > 0 and self.data[key].iloc[-1]['signal'] and self.data[key].iloc[-1]['signal'] != "Hold":
                    self.signal_generated[key] = True
                    raw_data = self.data[key].iloc[-1].to_dict()
                    logger.info(f"未闭合时触发信号 {self.symbol}_{self.market_type}_{self.timeframe}_{raw_data.get('signal', np.nan)}_{raw_data.get('profit', np.nan)}")
                    signal_record_tmp = {
                        'timestamp': datetime.fromtimestamp(kline[0] / 1000, tz=timezone(timedelta(hours=8))),
                        'open': raw_data.get('open', np.nan),
                        'c_avg': raw_data.get('c_avg', np.nan),
                        'symbol': self.symbol,
                        'market_type': self.market_type,
                        'timeframe': timeframe,
                        'signal': raw_data.get('signal', np.nan),
                        'trade_price': raw_data.get('trade_price', np.nan),
                        'entry_price': raw_data.get('entry_price', np.nan),
                        'profit': raw_data.get('profit', np.nan),
                    }

                    # 处理振幅数据（如果有）
                    pending = self._pending_amplitude.get(key)
                    if pending:
                        if raw_data.get('profit', 0.0) != 0.0:
                            signal_record_tmp['prev_amplitude_pct'] = pending['amplitude_pct']
                        signal_record_tmp['prev_extreme_price'] = pending['extreme_price']
                        signal_record_tmp['prev_signal_type'] = pending['type']
                        self._pending_amplitude.pop(key, None)

                    await self.database.save_signal_one(signal_record_tmp)
                    logger.info(
                        f"信号回调列表长度: {len(self.signal_callbacks) if hasattr(self, 'signal_callbacks') else 0}")
                    logger.info(
                        f"[DEBUG TRIGGER] Object ID: {id(self)}, Key: {(self.symbol, timeframe, self.market_type)}, Callbacks Count: {len(self.signal_callbacks)}")

                    # 触发所有注册的回调
                    if hasattr(self, 'signal_callbacks') and self.signal_callbacks:
                        for callback in self.signal_callbacks:
                            try:
                                # 传递信号数据给回调
                                signal_info = {
                                    'signal': raw_data.get('signal'),
                                    'entry_price': raw_data.get('entry_price'), # 传递挂单价
                                    'timestamp': datetime.fromtimestamp(kline[0] / 1000, tz=timezone(timedelta(hours=8))),
                                    'symbol': self.symbol,
                                    'market_type': self.market_type,
                                    'timeframe': timeframe
                                }
                                logger.info(f"开始创建异步任务，回调函数: {callback}")
                                asyncio.create_task(callback(signal_info, timeframe))
                                logger.info(f"异步任务创建完成")
                                logger.info(f"signal_info {signal_info}  timeframe {timeframe} 回调")
                            except Exception as e:
                                logger.error(f"执行信号回调失败: {e}")
                self.data[key] = self.data[key].iloc[:-1]
            elif k_state == 1:
                # K线闭合
                self.kline_init[key] = False
                self.signal_generated[key] = False
                
                if isinstance(self.data[key].iloc[-1], pd.Series):
                    raw_data = self.data[key].iloc[-1].to_dict()
                    logger.info(
                        f"闭合时 {self.symbol}_{self.market_type}_{self.timeframe}_{raw_data.get('signal', np.nan)}_{raw_data.get('profit', np.nan)}")
                    # 保存闭合K线到数据库
                    recode = {
                        'timestamp': datetime.fromtimestamp(kline[0] / 1000, tz=timezone(timedelta(hours=8))),
                        'open': kline[1],
                        'high': kline[2],
                        'low': kline[3],
                        'close': kline[4],
                        'c_avg': c_avg,
                        'volume': kline[5],
                        'confirm': 1,
                        'symbol': self.symbol,
                        'market_type': self.market_type,
                        'timeframe': timeframe,
                        'x': raw_data.get('x', np.nan),
                        'Wings': raw_data.get('Wings', np.nan),
                        'x_minus_Wings_avg': raw_data.get('x_minus_Wings_avg', np.nan),
                        'α': raw_data.get('α', np.nan),
                        'β': raw_data.get('β', np.nan),
                        'γ': raw_data.get('γ', np.nan),
                    }

                    await self.database.save_signal_one(recode)
                    
                    # 更新历史状态
                    self.prev_α[key] = raw_data.get('α', np.nan)
                    self.prev_β[key] = raw_data.get('β', np.nan)
                    self.prev_γ[key] = raw_data.get('γ', np.nan)
                    self.prev_x_minus_Wings[key] = raw_data.get('x_minus_Wings_avg', np.nan)
                    self.pre_trade_price[key] = raw_data.get('trade_price', np.nan)
                    # self.pre_entry_price[key] = raw_data.get('entry_price', np.nan)
                    
                    
                    # 保留刚闭合的这一行，并限制队列长度
                    self.data[key] = self.data[key].tail(self.max_data_points-1)
                    
        except Exception as e:
            logger.error(f"Update K-line error for {timeframe}: {str(e)}")
            raise

    async def calculate_indicators_on_df(self, df: pd.DataFrame, key: Tuple[str, str, str]):
        """
        只计算当前forming K线的指标，使用c_avg价格
        历史K线的指标已经计算好且固定不变
        """
        if not isinstance(df, pd.DataFrame):
            logger.error(f"calculate_indicators_on_df received non-DataFrame: {type(df)}")
            return df
        
        try:
            # 获取历史数据（已计算的固定指标）
            historical_data = df.iloc[:-1]
           # logger.info(f"calculate_indicators_on_df historical_data: {historical_data[['timestamp', 'symbol', 'market_type', 'timeframe', 'x_minus_Wings_avg', 'α']].tail(5)}")

            # 如果没有当前K线（df长度等于历史数据），直接返回
            if len(df) <= len(historical_data):
                return df
                
            # 只处理最后一行（当前forming K线）
            temp = df.copy()
            current_idx = temp.index[-1]
            
            # === 检查是否有足够的历史数据 ===
            required_k_lines = max(
                self.indicators_config['x']['long_period'],
                self.indicators_config['wings']['period'] + 
                self._get_nested_config('indicators.Wings.smooth_period', 3) - 1 + 
                self._get_nested_config('indicators.Wings.second_smooth_period', 3) - 1
            )
            
            if len(historical_data) < required_k_lines:
                logger.debug(f"Insufficient historical data: {len(historical_data)} < {required_k_lines}")
                if len(historical_data) > 0:
                    for col in ['x', 'Wings', 'x_minus_Wings', 'x_minus_Wings_avg', 'α', 'β']:
                        if col in historical_data.columns:
                            temp.loc[current_idx, col] = historical_data[col].iloc[-1]
                return temp
            
            # === 计算当前K线的X指标 ===
            x_window = pd.concat([
                historical_data,
                temp.loc[[current_idx]]
            ])

            lowest_low = talib.MIN(x_window['low'], timeperiod=self.indicators_config['x']['long_period'])
            highest_high = talib.MAX(x_window['high'], timeperiod=self.indicators_config['x']['long_period'])
            rsv = 100 * (x_window['c_avg'] - lowest_low) / (highest_high - lowest_low).replace(0, np.nan)
            rsv = rsv.replace([np.inf, -np.inf], np.nan).ffill().clip(0, 100)
            
            alpha = 2 / (self._get_nested_config('indicators.x.k_smooth', 3) + 1)
            ema_period = int((2 / alpha) - 1)
            x_values = talib.EMA(rsv, timeperiod=ema_period)
            temp.loc[current_idx, 'x'] = round(x_values.iloc[-1], 2)
            
            # === 计算当前K线的Wings指标 ===
            wings_window = pd.concat([
                historical_data,
                temp.loc[[current_idx]]
            ])

            wings_window['prev_c_avg'] = wings_window['c_avg'].shift(1).fillna(wings_window['c_avg'].iloc[0])
            base_value = (wings_window['c_avg'] > wings_window['prev_c_avg']).rolling(
                window=self.indicators_config['wings']['period']).mean() * 100
            base_value = base_value.replace([np.inf, -np.inf], np.nan).ffill().clip(0, 100)
            
            base_ma = talib.SMA(base_value, timeperiod=self._get_nested_config('indicators.Wings.smooth_period', 3))
            base_ma_double = talib.SMA(base_ma,
                                       timeperiod=self._get_nested_config('indicators.Wings.second_smooth_period', 3))
            temp.loc[current_idx, 'Wings'] = round(-base_ma_double.iloc[-1], 2)
            
            # === 计算派生指标 ===
            temp.loc[current_idx, 'x_minus_Wings'] = round(
                temp.loc[current_idx, 'x'] - temp.loc[current_idx, 'Wings'], 2
            )

            # 使用历史数据计算x_minus_Wings_avg
            if len(historical_data) >= 2:
                # 获取历史数据的最后x_minus_Wings_avg
                prev_avg = historical_data['x_minus_Wings_avg'].iloc[-1]
                current_x_minus = temp.loc[current_idx, 'x_minus_Wings']

                # 3周期EMA
                alpha_ema = 2 / (3 + 1)  # 标准EMA3系数
                current_avg = round(prev_avg * (1 - alpha_ema) + current_x_minus * alpha_ema, 2)
                temp.loc[current_idx, 'x_minus_Wings_avg'] = current_avg

                # 计算α (与上一根K线的差值)
                temp.loc[current_idx, 'α'] = round(current_avg - prev_avg, 2)

                # 计算β (α的差值)
                if 'α' in historical_data.columns and len(historical_data) >= 2:
                    prev_α = historical_data['α'].iloc[-1]
                    temp.loc[current_idx, 'β'] = round(temp.loc[current_idx, 'α'] - prev_α, 2)
                    if 'β' in historical_data.columns:
                        prev_β = historical_data['β'].iloc[-1]
                        temp.loc[current_idx, 'γ'] = round(temp.loc[current_idx, 'β'] - prev_β, 2)
                    else:
                        temp.loc[current_idx, 'γ'] = 0
                else:
                    temp.loc[current_idx, 'β'] = 0
            else:
                temp.loc[current_idx, 'x_minus_Wings_avg'] = temp.loc[current_idx, 'x_minus_Wings']
                temp.loc[current_idx, 'α'] = 0
                temp.loc[current_idx, 'β'] = 0
                temp.loc[current_idx, 'γ'] = 0
            
            # === 信号判断（使用历史数据）===
            if not self.signal_generated.get(key, False) and temp.loc[current_idx, 'confirm'] == 0:
                if len(historical_data) >= 3:
                    β_3 = historical_data['β'].iloc[-3]  # 3根前的β
                    prev_β = historical_data['β'].iloc[-1]  # 上一根K线的β
                    current_β = temp.loc[current_idx, 'β']
                    prev_γ = self.prev_γ.get(key,np.nan)
                    current_γ = temp.loc[current_idx, 'γ']
                    current_price = temp.loc[current_idx, 'close']
                    adjustment_ratio = self.entry_price_config.get('adjustment_ratio', 0.90)

                    if not pd.isna(prev_γ) and not pd.isna(current_γ):
                        profit = 0.0  # 默认值：非反向平仓时为0
                        # 卖出信号（尝试平多仓）
                        if (prev_β > 0 and current_γ <= 0
                            and self.prev_signal_type.get(key, 'hold') != 'short'):
                            temp.loc[current_idx, 'signal'] = f"{datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S')} Sell at {current_price}"
                            temp.loc[current_idx, 'trade_price'] = current_price

                            symbol_config = self.historical_min_config.get(self.symbol, {})
                            market_config = symbol_config.get(self.market_type, {})
                            hist_min = market_config.get(self.timeframe, 0.04)
                            entry_price = round(current_price * (1 + adjustment_ratio * hist_min / 100), 2)
                            temp.loc[current_idx, 'entry_price'] = entry_price

                            # 反向平仓：前一仓是 long
                            if self.prev_signal_type.get(key) == 'long' and self.pre_entry_price.get(key) is not None:
                                profit = round(entry_price - self.pre_entry_price[key], 2)

                            temp.loc[current_idx, 'profit'] = profit

                            self.prev_signal_type[key] = 'short'
                            self.signal_type[key] = 'short'  # 更新当前信号类型，用于极值追踪
                            self.pre_entry_price[key] = entry_price

                        # 买入信号（尝试平空仓）
                        elif (prev_β < 0 and current_γ >= 0
                              and self.prev_signal_type.get(key, 'hold') != 'long'):
                            temp.loc[current_idx, 'signal'] = f"{datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S')} Buy at {current_price}"
                            temp.loc[current_idx, 'trade_price'] = current_price

                            symbol_config = self.historical_min_config.get(self.symbol, {})
                            market_config = symbol_config.get(self.market_type, {})
                            hist_min = market_config.get(self.timeframe, 0.04)
                            entry_price = round(current_price * (1 - adjustment_ratio * hist_min / 100), 2)
                            temp.loc[current_idx, 'entry_price'] = entry_price

                            # 反向平仓：前一仓是 short
                            if self.prev_signal_type.get(key) == 'short' and self.pre_entry_price.get(key) is not None:
                                profit = round(self.pre_entry_price[key] - entry_price, 2)

                            temp.loc[current_idx, 'profit'] = profit

                            self.prev_signal_type[key] = 'long'
                            self.signal_type[key] = 'long'  # 更新当前信号类型，用于极值追踪
                            self.pre_entry_price[key] = entry_price
            
            # 历史数据已经填充过NaN，实时计算是增量计算不会产生NaN，无需再次填充
            
            logger.debug(f"Calculated indicators for current K-line only using c_avg price")
            return temp
            
        except Exception as e:
            logger.error(f"calculate_indicators_on_df error: {str(e)}")
            return df

    async def x_calculation(self, df):
        # 此函数保留但使用c_avg
        lowest_low = talib.MIN(df['low'], timeperiod=self.indicators_config['x']['long_period'])
        highest_high = talib.MAX(df['high'], timeperiod=self.indicators_config['x']['long_period'])
        rsv = 100 * (df['c_avg'] - lowest_low) / (highest_high - lowest_low).replace(0, np.nan)
        rsv = rsv.replace([np.inf, -np.inf], np.nan).ffill().clip(0, 100)
        alpha = 2 / (self._get_nested_config('indicators.x.k_smooth', 3) + 1)
        ema_period = int((2 / alpha) - 1)
        df['x'] = talib.EMA(rsv, timeperiod=ema_period)

    async def wings_calculation(self, df):
        # 此函数保留但使用c_avg
        df['prev_c_avg'] = df['c_avg'].shift(1).fillna(df['c_avg'].iloc[0])
        base_value = (df['c_avg'] > df['prev_c_avg']).rolling(
            window=self.indicators_config['wings']['period']).mean() * 100
        base_value = base_value.replace([np.inf, -np.inf], np.nan).ffill().clip(0, 100)
        base_ma = talib.SMA(base_value, timeperiod=self._get_nested_config('indicators.Wings.smooth_period', 3))
        base_ma_double = talib.SMA(base_ma,
                                   timeperiod=self._get_nested_config('indicators.Wings.second_smooth_period',
                                                                      3))
        df['Wings'] = -base_ma_double

    
    #2026-02-19 追踪信号触发之后的极值
    def track_extreme_after_signal(self, key: Tuple[str, str, str], current_price: float, current_timestamp):
        # 使用 self.signal_type 获取当前forming K线的信号
        current_type = self.signal_type.get(key, 'hold')
        if current_type == 'hold':
            return

        def _new_tracker(signal_type: str, price: float):
        # long: 只跟踪 min_price；short: 只跟踪 max_price
            return {
                'type': signal_type,
                'trade_price': price,
                'min_price': price if signal_type == 'long' else None,
                'max_price': price if signal_type == 'short' else None
            }

        tracker = self._extreme_tracker.get(key)

        # 首次进入方向，初始化
        if not self._has_first_signal.get(key, False) or tracker is None:
            self._extreme_tracker[key] = _new_tracker(current_type, current_price)
            self._has_first_signal[key] = True
            return

        # 同方向：只更新该方向对应极值
        if tracker['type'] == current_type:
            if current_type == 'long':
                if tracker['min_price'] is None or current_price < tracker['min_price']:
                    tracker['min_price'] = current_price
            else:  # short
                if tracker['max_price'] is None or current_price > tracker['max_price']:
                    tracker['max_price'] = current_price
            return

        # 方向切换：先结算上一段，再开启新段
        if tracker['type'] == 'long':
            settle_min = tracker['min_price'] if tracker['min_price'] is not None else tracker['trade_price']
            amplitude_pct = (tracker['trade_price'] - settle_min) / tracker['trade_price'] * 100
            extreme_price = settle_min
        else:  # previous short
            settle_max = tracker['max_price'] if tracker['max_price'] is not None else tracker['trade_price']
            amplitude_pct = (settle_max - tracker['trade_price']) / tracker['trade_price'] * 100
            extreme_price = settle_max

        self._pending_amplitude[key] = {
            'type': tracker['type'],
            'trade_price': tracker['trade_price'],
            'amplitude_pct': amplitude_pct,
            'extreme_price': extreme_price
        }

        
        # 开启新方向：另一侧极值置空
        self._extreme_tracker[key] = _new_tracker(current_type, current_price)
    

    #2026-02-19 最小回撤幅度，修正entry_price
    def track_drawdown_pct(self, current_price: float, current_timestamp):
        key = (self.symbol, self.timeframe, self.market_type)
        tracker = self._extreme_tracker.get(key)
        if not tracker or 'amplitude_pct' not in tracker:
            return None

        current = tracker['amplitude_pct']  # 当前信号触发后的反向波动幅度
        
        # 这个方法现在只用来返回当前振幅，不更新历史极值
        return {
            'current': current
        }