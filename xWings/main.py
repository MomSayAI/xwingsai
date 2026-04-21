# File: main.py
# 交易版 (Online Trading Version)
# Purpose: Main entry point for xWings automated trading system - event-driven architecture
# Dependencies: asyncio, yaml, watchdog, pymongo, pandas, cryptography, tabulate
# Notes: 
#   - Timestamps in UTC+8 (Asia/Shanghai)
#   - Event-driven architecture with signal callbacks for real-time trading execution
#   - Features:
#       * Automatic account discovery from client_credentials_encrypted.yaml
#       * Market data center initialization for multiple exchanges (Binance/OKX)
#       * WebSocket connections for real-time candle data
#       * Signal callback registration for automated trade execution
#       * File monitoring for dynamic plugin reloading (exchanges directory)
#       * CSV export scheduler for signal recording (every 5 minutes)
#       * WebSocket health monitoring with visual table output
#       * Position state management via TradingState
#       * Funds allocation and management via ClientManager
#   - Architecture: 
#       1. MarketDataCenter receives real-time candles via WebSocket
#       2. SignalGenerator processes candles and generates Buy/Sell signals
#       3. Signal callbacks trigger execute_trade_async in ClientManager
#       4. Executor handles order placement and position management
#   - Supported timeframes: 30m, 2h, 6h (configurable via markets_encrypted.yaml)
#   - Supports Windows/Linux environment detection for log file naming
# Updated: 2026-02-22
# Version: 2.0.0 (Online Trading Version)

import sys
import os
import time
import math
import logging
import asyncio
from tabulate import tabulate
from datetime import datetime, timezone, timedelta
from threading import Thread
from typing import List, Tuple, Dict, Optional
import yaml
from pathlib import Path
from pymongo.errors import ServerSelectionTimeoutError
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from cryptography.fernet import InvalidToken
import pandas as pd

import csv
import json

# Configure project paths
PROJECT_ROOT = Path(__file__).resolve().parent
SRC_PATH = PROJECT_ROOT / 'src'
CORE_PATH = SRC_PATH / 'core'
for path in [PROJECT_ROOT, SRC_PATH, CORE_PATH]:
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))
        logging.info(f"Added {path} to sys.path")

try:
    from src.core.signal_generator import SignalGenerator
    from src.core.analytics import Analytics
    from src.core.executor import Executor
    from src.core.trading_state import TradingState
    from src.core.client_manager import ClientManager
    from src.core.config_manager import ConfigManager
    from src.core.logging_config import configure_logging
    from src.core.exchange import ExchangeAPI
    from src.core.database import Database
    from src.core.funds_manager import FundsManager
    from src.core.funds_allocation_manager import FundsAllocationManager
    from src.core.utils import PathManager
    from src.tools.file_monitor import FileMonitor
    from src.config.schema import ClientCredentialsConfig, MarketConfig, ParametersConfig
    from src.core.position_sync_manager import PositionSyncManager
    from src.core.market_data_center import MarketDataCenter

except ImportError as e:
    logging.error(
        f"Failed to import required modules: {str(e)}. Ensure src/core/ contains __init__.py and all required modules.")
    raise

# Initialize paths and logging
path_manager = PathManager(PROJECT_ROOT)
log_file = "main_local.log" if os.name == 'nt' else "main_server.log"
log_dir = path_manager.get_log_path(log_file).parent

log_dir.mkdir(parents=True, exist_ok=True)
logger = configure_logging(log_file=str(path_manager.get_log_path(log_file)), log_level='DEBUG')
logger.info(f"PROJECT_ROOT: {PROJECT_ROOT} on {'Windows' if os.name == 'nt' else 'Linux'}")
logger.debug(f"Python executable: {sys.executable}, version: {sys.version}")
logger.debug(f"Current working directory: {Path.cwd()}")


class Scheduler:
    def __init__(self, symbols: List[str], market_types: List[str], timeframes: List[str], database: Database = None):
        self.symbols = symbols
        self.market_types = market_types
        self.timeframes = timeframes
        self.database = database
        self.start_time_csv = {} #每个K线周期的开始时间
        self.last_ts = {}
    async def start_scheduler(self, interval_minutes: int = 5):
        while True:
            try:
                logging.info("start scheduler ")
                # await self.export_ohlcv_to_csv()
                await self.export_signals_to_csv()
                await asyncio.sleep(interval_minutes * 60)
            except KeyboardInterrupt:
                logging.info("Exporter stopped by user")
                break
            except Exception as e:
                logging.error(f"Scheduler error: {str(e)}")
                time.sleep(60)

    def ensure_cst_time(self, dt):
        cst = timezone(timedelta(hours=8))
        if isinstance(dt, str):
            dt = datetime.fromisoformat(dt)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc).astimezone(cst)
        return dt.astimezone(cst)

    def safe_str_convert(self, val, default=''):
        try:
            return default if val is None else str(val) if val == val else default  # NaN ≠ NaN 是True
        except (TypeError, ValueError):
            return default
    def _get_last_timestamp(self, csv_path: Path, timeframe: str) -> datetime:
        if not csv_path.exists():
            # if not csv_path.exists() or csv_path.stat().st_size <= 1:
            return self._get_time_minus_timeframe(timeframe)

        try:
            df_original = pd.read_csv(csv_path)
            if df_original.empty or 'timestamp' not in df_original.columns:
                return self._get_time_minus_timeframe(timeframe)
            last_row = df_original.iloc[-1]
            return datetime.fromisoformat(
                last_row['timestamp']).replace(tzinfo=timezone(timedelta(hours=8))) if not last_row.empty and 'timestamp' in last_row else datetime.now(
                timezone(timedelta(hours=8)))
        except Exception:
            return self._get_time_minus_timeframe(timeframe)
    def _get_time_minus_timeframe(self, timeframe: str) -> datetime:
        """
        根据timeframe计算当前时间减去对应周期的时间。
        timeframe支持：30m, 2h, 6h
        """
        now = datetime.now(timezone(timedelta(hours=8)))

        if timeframe.endswith('m'):
            n = int(timeframe[:-1])
            return now - timedelta(minutes=n)
        elif timeframe.endswith('h'):
            n = int(timeframe[:-1])
            return now - timedelta(hours=n)
        else:
            # 默认返回当前时间
            return now
    async def export_signals_to_csv(self):
        signals_dir = PROJECT_ROOT / "signals"
        signals_dir.mkdir(exist_ok=True)
        for symbol in self.symbols:
            for market_type in self.market_types:
                for timeframe in self.timeframes:
                    try:
                        key = (symbol, market_type, timeframe)
                        collection_name = f"{symbol.replace('/', '_')}_{market_type}_{timeframe}"
                        csv_path = signals_dir / f"{collection_name}_signals.csv"
                        if not self.start_time_csv.get(key, False):
                            self.last_ts[key] = self._get_last_timestamp(csv_path, timeframe)
                            csv_path.touch(exist_ok=True)
                            self.start_time_csv[key] = True

                        logging.info(f"export_signals_to_csv start_time:{self.ensure_cst_time(self.last_ts[key])}")
                        signals = await self.database.get_csv_signals(symbol=symbol, market_type=market_type,
                                                                      timeframe=timeframe,
                                                                      start_time=self.ensure_cst_time(self.last_ts[key]))
                        if signals is None or not isinstance(signals, (list, pd.DataFrame)):
                            continue
                        if isinstance(signals, pd.DataFrame):
                            signals = signals.to_dict('records')

                        headers = [
                                    'timestamp', 'timeframe', 'confirm', 'symbol', 'market_type',
                                    'open', 'high', 'low', 'close', 'c_avg', 'volume',
                                    'x_minus_Wings_avg', 'α', 'β', 'γ',
                                    'signal', 'trade_price', 'entry_price', 'profit',
                                    'prev_amplitude_pct'
                                ]

                        # temp_path = csv_path.with_suffix('.tmp')
                        with open(csv_path, 'a', newline='', buffering=8192, encoding='utf-8-sig') as f:
                            writer = csv.DictWriter(f, fieldnames=headers)
                            if csv_path.stat().st_size == 0:
                                writer.writeheader()
                            for signal in signals:
                                writer.writerow({
                                    'timestamp': datetime.fromisoformat(str(signal.get('timestamp', ''))).replace(tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=8))),
                                    'open': signal.get('open', ''),
                                    'high': signal.get('high', ''),
                                    'low': signal.get('low', ''),
                                    'close': signal.get('close', ''),
                                    'c_avg': signal.get('c_avg', ''),
                                    'volume': signal.get('volume', ''),
                                    'symbol': signal.get('symbol', ''),
                                    'market_type': signal.get('market_type', ''),
                                    'timeframe': signal.get('timeframe', ''),
                                    'confirm': signal.get('confirm', ''),
                                    'x_minus_Wings_avg': self.safe_str_convert(signal.get('x_minus_Wings_avg')),
                                    'α': self.safe_str_convert(signal.get('α')),
                                    'β': self.safe_str_convert(signal.get('β')),
                                    'γ': self.safe_str_convert(signal.get('γ')),
                                    'signal': self.safe_str_convert(signal.get('signal')),
                                    'trade_price': self.safe_str_convert(signal.get('trade_price')),
                                    'entry_price': self.safe_str_convert(signal.get('entry_price')),
                                    'profit': self.safe_str_convert(signal.get('profit')),
                                    'prev_amplitude_pct': self.safe_str_convert(signal.get('prev_amplitude_pct')),
                                })
                        if signals:
                            self.last_ts[key] = self.ensure_cst_time(signals[-1]['timestamp'])
                        logging.info(f"Exported {len(signals)} new records to {collection_name}")
                    except Exception as e:
                        logging.error(f"Failed to export {symbol}:{market_type}:{timeframe} signals: {str(e)}")
                        continue

def get_timeframe_minutes(tf: str) -> float:
    if tf.endswith('m'):
        return float(tf[:-1])
    elif tf.endswith('h'):
        return float(tf[:-1]) * 60
    raise ValueError(f"Invalid timeframe: {tf}")


class PluginReloadHandler(FileSystemEventHandler):
    def __init__(self, main_app, loop):
        self.main_app = main_app
        self.loop = loop
        logger.info("Initialized PluginReloadHandler")

    def on_modified(self, event):
        try:
            if not event.is_directory and event.src_path.endswith('.py'):
                exchange_name = Path(event.src_path).stem
                if exchange_name in ['binance', 'okx']:
                    logger.info(f"Detected modification in {event.src_path}, scheduling reload for {exchange_name}")
                    asyncio.run_coroutine_threadsafe(
                        self.main_app.reload_exchanges(exchange_name),
                        self.loop
                    )
        except Exception as e:
            logger.error(f"Plugin reload handler error: {str(e)}")

async def print_ws_health(market_data_centers, interval=10, timeout=30):
    from tabulate import tabulate
    while True:
        now = datetime.now()
        ticker_rows = []
        candle_rows = []
        for ex, mdc in market_data_centers.items():
            # Ticker健康
            async with mdc.lock:
                for key, info in mdc.ticker_health.items():
                    symbol, market_type = key
                    last_time = info['last_msg_time']
                    count = info['count']
                    delta = (now - last_time).total_seconds()
                    status = '健康' if delta <= timeout else '超时'
                    ticker_rows.append([ex, symbol, market_type, last_time.strftime('%Y-%m-%d %H:%M:%S'), f'{delta:.1f}', count, status])
                # Candle健康
                for key, info in getattr(mdc, 'candle_health', {}).items():
                    symbol, timeframe, market_type = key
                    last_time = info['last_msg_time']
                    count = info['count']
                    delta = (now - last_time).total_seconds()
                    status = '健康' if delta <= timeout else '超时'
                    candle_rows.append([ex, symbol, timeframe, market_type, last_time.strftime('%Y-%m-%d %H:%M:%S'), f'{delta:.1f}', count, status])
        # 打印ticker健康
        if ticker_rows:
            headers = ['交易所', 'Symbol', 'MarketType', '最近推送时间', '距现在(秒)', '累计推送', '状态']
            print('\n' + '='*30 + ' Ticker健康状态 ' + '='*30)
            try:
                if tabulate:
                    print(tabulate(ticker_rows, headers=headers, tablefmt='grid', stralign='center'))
                else:
                    print(headers)
                    for row in ticker_rows:
                        print(row)
            except BrokenPipeError:
                pass  # 忽略管道错误
        else:
            print('暂无ticker健康数据')
        # 打印candle健康
        if candle_rows:
            headers = ['交易所', 'Symbol', 'Timeframe', 'MarketType', '最近推送时间', '距现在(秒)', '累计推送', '状态']
            print('\n' + '='*30 + ' Candle(1s K线)健康状态 ' + '='*30)
            try:
                if tabulate:
                    print(tabulate(candle_rows, headers=headers, tablefmt='grid', stralign='center'))
                else:
                    print(headers)
                    for row in candle_rows:
                        print(row)
            except BrokenPipeError:
                pass  # 忽略管道错误
        else:
            print('暂无candle健康数据')
        
        await asyncio.sleep(interval)


async def main():
    # 启动时记录当前时间到 start_time.json
    config_dir = Path(__file__).resolve().parent / 'src' / 'config'
    config_dir.mkdir(parents=True, exist_ok=True)
    start_time_path = config_dir / 'start_time.json'
    now = pd.Timestamp.now(tz='Asia/Shanghai').isoformat()
    with open(start_time_path, 'w', encoding='utf-8') as f:
        json.dump({'start_time': now}, f, ensure_ascii=False, indent=2)

    db = None
    market_data_centers = {}  # 保证始终已定义
    try:
        # 初始化路径和配置
        PROJECT_ROOT = Path(__file__).resolve().parent
        path_manager = PathManager(PROJECT_ROOT)
        key_file = path_manager.get_config_path("encryption.key")
        config_manager = ConfigManager(key_file=str(key_file))
        parameters = config_manager.get_config('parameters_encrypted')
        markets = config_manager.get_config('markets_encrypted')
        client_credentials = config_manager.get_config('client_credentials_encrypted')
        
        # 创建唯一的数据库实例
        db = Database(parameters['database'])

        # 1. 递归获取accounts（含子账户）
        accounts = []
        def _traverse_accounts(account_path, account_config):
            login_name = account_config.get('login_name')
            if login_name:
                new_path = account_path[:-1] + [login_name]
            else:
                new_path = account_path
            account_id = ".".join(new_path)
            accounts.append({'account_id': account_id, 'config': account_config})
            # 递归 subaccounts 字段
            subaccounts = account_config.get('subaccounts', {})
            if isinstance(subaccounts, dict):
                for sub_name, sub_config in subaccounts.items():
                    _traverse_accounts(new_path + [sub_name], sub_config)
            # 兼容老式直接嵌套trading_config的子账户
            for k, v in account_config.items():
                if isinstance(v, dict) and 'trading_config' in v and k != 'subaccounts':
                    _traverse_accounts(new_path + [k], v)
        for exchange, subaccounts_dict in client_credentials.get('subaccounts', {}).items():
            for account, account_config in subaccounts_dict.items():
                _traverse_accounts([exchange, account], account_config)

        # 2. symbols、timeframes、market_types 只从markets_encrypted.yaml获取
        symbols = set()
        timeframes = set()
        market_types = set()
        for exchange, ex_config in markets.get('exchanges', {}).items():
            tfs = ex_config.get('timeframes', [])
            for tf in tfs:
                timeframes.add(tf)
            for market_type, mt_config in ex_config.get('markets', {}).get('crypto', {}).items():
                if isinstance(mt_config, dict):
                    syms = mt_config.get('symbols', [])
                    for s in syms:
                        symbols.add(s)
                    market_types.add(market_type)
        symbols = list(symbols)
        timeframes = list(timeframes)
        market_types = list(market_types)

        # 初始化ClientManager，传入已创建的数据库实例
        client_manager = ClientManager(
            key_file=str(key_file),
            database=db,  # 传入已创建的Database实例
            file_monitor=FileMonitor(key_file=str(key_file), config_data=parameters)
        )
        await client_manager.async_init(accounts)

        # ========== 在这里添加启动代码 ==========
        # 获取 file_monitor 实例
        file_monitor = client_manager.file_monitor

        # 启动文件监控
        if file_monitor:
            file_monitor.start()
            logger.info("文件监控已启动 - 修改以下模块将自动生效：")
            logger.info(f"监控模块: {file_monitor.core_modules}")

        # 初始化并启动行情中心
        print("开始初始化行情中心")
        market_data_centers = {}
        for exchange_name, exchange_config in markets.get('exchanges', {}).items():
            if not exchange_config.get('enabled', True):
                continue

            public_exchange_instance = client_manager.get_public_exchange(exchange_name)
            print(f"为 {exchange_name} 创建 MarketDataCenter")
            mdc = MarketDataCenter(exchange=public_exchange_instance, database=db)
            market_data_centers[exchange_name] = mdc
            
            # 启动该交易所的行情服务
            await mdc.start(list(symbols), list(timeframes), list(market_types), max_data_points=parameters['data']['max_data_points'])
        print("开始启动 websocket ticker 订阅")
        for exchange_name, mdc in market_data_centers.items():
            print(f"启动 {exchange_name} 的 ticker 订阅")
            for key, sg in mdc.signal_generators.items():
                # key 是 (symbol, timeframe, market_type)
                if key in SignalGenerator.global_callback_registry:
                    global_callbacks = SignalGenerator.global_callback_registry[key]
                    # 将全局回调添加到实例列表中（去重）
                    for cb in global_callbacks:
                        if cb not in sg.signal_callbacks:
                            sg.signal_callbacks.append(cb)

                    logger.info(f"[强制同步] {key} 实例 (ID:{id(sg)}) 同步后回调数：{len(sg.signal_callbacks)}")
            # await mdc.start_websocket_ticker(list(symbols), list(market_types))
            await mdc.start_websocket_candles(list(symbols), list(market_types), list(timeframes))
        # --- 新增：自动启动ticker健康监控 ---
        for mdc in market_data_centers.values():
            asyncio.create_task(mdc.monitor_ticker_health())
        # --- 新增：本地命令行可视化 ---
        asyncio.create_task(print_ws_health(market_data_centers))

        scheduler = Scheduler(
            symbols=list(symbols),
            market_types=list(market_types),
            timeframes=list(timeframes),
            database=db
        )
        # await scheduler.start_scheduler()
        scheduler_task = asyncio.create_task(scheduler.start_scheduler())
        logger.info("CSV导出调度器已启动为后台任务")

        # 创建并初始化交易组件
        trading_states = {}
        executors = {}

        # 处理每个账户的交易配置
        for account in accounts:
            subaccount_id = account['account_id']
            config = account['config']
            
            # 获取交易所名称
            exchange_name = subaccount_id.split('.')[0]
            
            # 创建或获取 TradingState
            if subaccount_id not in trading_states:
                trading_state = TradingState(
                    account_id=subaccount_id,
                    database=db,
                    exchange=client_manager.exchanges[subaccount_id],
                    config_manager=config_manager
                )
                trading_states[subaccount_id] = trading_state
                # 加载历史仓位
                await trading_state.load_positions()

            # 获取该账户的交易配置
            trading_config = config.get('trading_config', {}).get('symbols', {})

            # 为每个交易对创建 Executor
            for symbol, symbol_config in trading_config.items():
                for market_type in market_types:
                    # 检查该交易对在该市场类型是否可用
                    market_config = markets.get('exchanges', {}).get(exchange_name, {}).get('markets', {}).get('crypto', {}).get(market_type, {})
                    if symbol not in market_config.get('symbols', []):
                        continue

                    executor_key = (exchange_name, symbol, market_type)
                    # 创建 Executor
                    if executor_key not in executors:
                        executor = Executor(
                            subaccount_id=subaccount_id,
                            exchange_name=exchange_name,
                            symbol=symbol,
                            market_type=market_type,
                            client_manager=client_manager,
                            trading_state=trading_states[subaccount_id],
                            database=db,
                            configs=config_manager
                        )
                        executors[executor_key] = executor

        logger.info(f"Initialized {len(executors)} executors for {len(accounts)} accounts")

        # --- 主循环逻辑重构：信号驱动下单 ---
        # 1. 构建 tradable_items 列表（用于注册回调，不再轮询）
        tradable_items = []
        for account in accounts:
            subaccount_id = account['account_id']
            config = account['config']
            exchange_name = subaccount_id.split('.')[0]
            trading_config = config.get('trading_config', {}).get('symbols', {})
            for symbol, symbol_config in trading_config.items():
                symbol_timeframes = symbol_config.get('timeframes', [])
                leverage = symbol_config.get('leverage', 1)
                for market_type in market_types:
                    market_config = markets.get('exchanges', {}).get(exchange_name, {}).get('markets', {}).get('crypto', {}).get(market_type, {})
                    if symbol not in market_config.get('symbols', []):
                        continue
                    for timeframe in symbol_timeframes:
                        tradable_items.append({
                            "subaccount_id": subaccount_id,
                            "exchange_name": exchange_name,
                            "symbol": symbol,
                            "market_type": market_type,
                            "timeframe": timeframe,
                            "leverage": leverage
                        })

        logger.info(f"Registering signal callbacks for {len(tradable_items)} tradable items.")

        # 2. 注册信号回调，信号生成后自动下单
        async def make_order_callback(subaccount_id, symbol, market_type, timeframe, client_manager):
            async def on_signal(signal_data, tf):
                try:
                    logger.info(f"[回调入口] 收到信号数据: {signal_data}")
                    if signal_data and signal_data.get('signal') and (
                            'Buy' in signal_data['signal'] or 'Sell' in signal_data['signal']):
                        logger.info(
                            f"[信号回调] {subaccount_id} {symbol} {market_type} {tf} 收到信号: {signal_data['signal']} 触发价:{signal_data.get('price')} 挂单价:{signal_data.get('entry_price')}")

                        # 解析交易所名称
                        exchange_name = subaccount_id.split('.')[0]

                        # 调用ClientManager执行交易 - 传递挂单价
                        trade_records = await client_manager.execute_trade_async(
                            subaccount_id=subaccount_id,
                            exchange_name=exchange_name,
                            symbol=signal_data['symbol'],
                            market_type=signal_data['market_type'],
                            timeframe=signal_data['timeframe'],
                            signal=signal_data['signal'],
                            entry_price=signal_data.get('entry_price')  # 传递挂单价
                        )

                        if trade_records:
                            logger.info(
                                f"[交易执行成功] {subaccount_id} {symbol} {market_type} {tf} 执行了 {len(trade_records)} 笔交易")
                        else:
                            logger.warning(f"[交易执行失败或无交易] {subaccount_id} {symbol} {market_type} {tf}")

                except Exception as e:
                    logger.error(f"[信号回调异常] {subaccount_id} {symbol} {market_type} {tf}: {e}")

            return on_signal

        # 3. 为每个SignalGenerator注册回调
        for item in tradable_items:
            subaccount_id = item["subaccount_id"]
            exchange_name = item["exchange_name"]
            symbol = item["symbol"]
            market_type = item["market_type"]
            timeframe = item["timeframe"]
            # 构造注册表 Key (必须与 SignalGenerator 内部构造一致)
            registry_key = (symbol, timeframe, market_type)
            # sg_key = (symbol, timeframe, market_type)
            executor_key = (exchange_name, symbol, market_type)
            # signal_generator = market_data_centers[exchange_name].signal_generators.get(sg_key)
            executor = executors.get(executor_key)
            funds_manager = client_manager.funds_managers.get(subaccount_id)
            # logger.info(f"SignalGenerator对象: {signal_generator}")
            logger.info(f"subaccount_id: {subaccount_id}  exchange_name {exchange_name} registry_key {registry_key}")
            logger.info(f"Executor对象: {executor}")
            logger.info(f"FundsManager对象: {funds_manager}")

            if executor and funds_manager:
                # 创建回调函数
                on_signal = await make_order_callback(subaccount_id, symbol, market_type, timeframe, client_manager)

                # 【修改 1】注册到全局注册表 (核心修复)
                if registry_key not in SignalGenerator.global_callback_registry:
                    SignalGenerator.global_callback_registry[registry_key] = []

                # 避免重复注册
                if on_signal not in SignalGenerator.global_callback_registry[registry_key]:
                    SignalGenerator.global_callback_registry[registry_key].append(on_signal)
                    logger.info(
                        f"[全局注册] {registry_key} 注册成功，当前全局回调数：{len(SignalGenerator.global_callback_registry[registry_key])}")

                # 【修改 2】兼容：如果此时能获取到实例，也手动添加一份（双重保险）
                signal_generator = market_data_centers[exchange_name].signal_generators.get(registry_key)
                if signal_generator:
                    if not hasattr(signal_generator, 'signal_callbacks'):
                        signal_generator.signal_callbacks = []
                    if on_signal not in signal_generator.signal_callbacks:
                        signal_generator.signal_callbacks.append(on_signal)
                        logger.info(
                            f"[实例直连] {registry_key} 也添加了回调，当前实例回调数：{len(signal_generator.signal_callbacks)}")
                else:
                    logger.warning(f"[实例直连] 未找到 SignalGenerator 实例: {registry_key}，仅依赖全局注册表")
            else:
                logger.warning(f"缺少 Executor 或 FundsManager，跳过注册：{registry_key}")

        logger.info("所有信号回调注册完成（基于全局注册表），系统进入事件驱动模式。")
        # 主程序不再需要while True轮询，事件驱动即可
        while True:
            await asyncio.sleep(3600)  # 保持主进程存活

    except Exception as e:
        logger.error(f"Fatal error in main: {str(e)}")
        raise
    finally:
        # 确保所有资源都被清理
        if 'db' in locals() and db is not None:
            await db.close()
        if 'market_data_centers' in locals() and market_data_centers is not None:
            for mdc in market_data_centers.values():
                try:
                    await mdc.stop()
                except Exception as err:
                    pass



if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
    finally:
        logger.info("Program terminated")
