# File: src/tools/file_monitor.py
# Purpose: Unified monitoring for local configuration files and exchange API updates in xWings trading system
# Dependencies: watchdog, aiohttp, bs4, yaml, pathlib, logging, schema, json
# Notes: Monitors parameters_encrypted.yaml, client_credentials_encrypted.yaml, markets_encrypted.yaml,
#        and exchange API documentation (Binance, OKX). Checks updates every 24 hours.
#        Saves API changes to logs/api_changes.log and data/processed/api_changes.json.
#        Timestamps in UTC+8 for consistency with database.py and signal_generator.py.
#        Adapted for Docker with PathManager for cross-platform path handling.
# Updated: 2025-07-06

import sys
import asyncio
import aiohttp
from bs4 import BeautifulSoup
import re
from pathlib import Path
from typing import Dict, Callable, Optional, List
from datetime import datetime, timezone, timedelta
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from logging import getLogger
import json
import traceback

# Configure project paths
PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_PATH = PROJECT_ROOT / 'src'
CORE_PATH = SRC_PATH / 'core'
for path in [PROJECT_ROOT, SRC_PATH, CORE_PATH]:
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))
        getLogger(__name__).debug(f"Added {path} to sys.path")

from src.core.utils import PathManager
from src.config.schema import FileMonitorConfig

# Initialize logging (configured in main.py)
path_manager = PathManager(PROJECT_ROOT)
logger = getLogger(__name__)
api_logger = getLogger("api_changes")

class ConfigUpdateHandler(FileSystemEventHandler):
    def __init__(self, file_monitor):
        """
        Initialize file system event handler for configuration files.

        Args:
            file_monitor: FileMonitor instance to handle updates
        """
        self.file_monitor = file_monitor
        logger.debug("Initialized ConfigUpdateHandler")

    def on_modified(self, event):
        """
        Handle file modification events for configuration files.

        Args:
            event: File system event
        """
        if not event.is_directory and event.src_path.endswith(('.yaml', '.yml')):
            config_name = Path(event.src_path).stem
            if config_name in self.file_monitor.config_callbacks:
                logger.info(f"Detected modification in {event.src_path}, scheduling update for {config_name}")
                self.file_monitor.notify_config_update(config_name)

class FileMonitor:
    def __init__(self, key_file: str, config_data: Dict, db_callback: Optional[Callable[[Dict], None]] = None):
        """
        Initialize FileMonitor for configuration files and API updates.

        Args:
            key_file (str): Path to encryption key file
            config_data (Dict): Parameters configuration data
            db_callback (Optional[Callable]): Callback for database operations

        Raises:
            ValueError: If configuration is invalid
        """
        self.config_path = path_manager.config_path
        try:
            self.api_config = FileMonitorConfig(**config_data.get('api_monitor', {})) if config_data.get('api_monitor') else FileMonitorConfig(exchanges={}, check_interval=86400)
        except ValueError as e:
            logger.error(f"Invalid api_monitor configuration: {str(e)}\n{traceback.format_exc()}")
            raise
        self.exchanges = getattr(self.api_config, 'exchanges', {
            'binance': {'url': 'https://developers.binance.com/docs/zh-CN/binance_link/change-log'},
            'okx': {'url': 'https://www.okx.com/docs-v5/zh/#overview'}
        })
        self.check_interval = getattr(self.api_config, 'check_interval', 86400)  # Default: 24 hours
        self.max_retries = getattr(self.api_config, 'max_retries', 3)
        self.retry_delay = getattr(self.api_config, 'retry_delay', 5)
        self.db_callback = db_callback
        self.config_callbacks = {}
        self.observer = Observer()
        self.handler = ConfigUpdateHandler(self)
        self.running = False
        self.last_check = {}
        self.api_changes_file = path_manager.get_data_path("processed/api_changes.json")
        self.api_changes_file.parent.mkdir(parents=True, exist_ok=True)
        self._validate_config()
        logger.debug(f"Initialized FileMonitor for {self.config_path} and exchanges {list(self.exchanges.keys())}")
        # 模块重载相关属性
        self.module_callbacks = {}  # 存储模块名到回调函数的映射
        self.module_handler = ModuleReloadHandler(self)  # 模块重载处理器
        self.module_observer = Observer()  # 单独的观察者用于模块监控
        
        # 监控的核心模块列表
        self.core_modules = [
            'signal_generator.py',
            'client_manager.py', 
            'fund_manager.py'
        ]
        
        logger.debug(f"Initialized FileMonitor with module reload support for {self.core_modules}")

    def _validate_config(self):
        """
        Validate configuration for file and API monitoring.
        """
        try:
            if not self.exchanges:
                logger.warning("No exchanges defined in api_monitor config, skipping API monitoring")
            for exchange, config in self.exchanges.items():
                if not config.get('url'):
                    logger.error(f"No URL defined for exchange {exchange}")
                    raise ValueError(f"No URL defined for exchange {exchange}")
            if self.check_interval <= 0:
                logger.error(f"Invalid check_interval: {self.check_interval}")
                raise ValueError("check_interval must be positive")
            if self.max_retries <= 0:
                logger.error(f"Invalid max_retries: {self.max_retries}")
                raise ValueError("max_retries must be positive")
            if self.retry_delay < 0:
                logger.error(f"Invalid retry_delay: {self.retry_delay}")
                raise ValueError("retry_delay must be non-negative")
            logger.debug("FileMonitor configuration validated successfully")
        except Exception as e:
            logger.error(f"Configuration validation failed: {str(e)}\n{traceback.format_exc()}")
            raise

    def register_callback(self, config_name: str, callback: Callable[[str], None]):
        """
        Register callback for configuration file updates.

        Args:
            config_name (str): Name of configuration file (without .yaml)
            callback (Callable): Function to call when file is updated
        """
        self.config_callbacks[config_name] = callback
        logger.debug(f"Registered callback for {config_name}")

    def notify_config_update(self, config_name: str):
        """
        Notify registered callback for configuration update.

        Args:
            config_name (str): Name of configuration file (without .yaml)
        """
        if config_name in self.config_callbacks:
            try:
                self.config_callbacks[config_name](config_name)
                logger.info(f"Notified update for {config_name}")
            except Exception as e:
                logger.error(f"Failed to notify update for {config_name}: {str(e)}\n{traceback.format_exc()}")

    async def fetch_page(self, url: str) -> Optional[str]:
        """
        Fetch webpage content asynchronously with retry mechanism.

        Args:
            url (str): URL to fetch

        Returns:
            Optional[str]: Page content or None if failed
        """
        for attempt in range(self.max_retries):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, timeout=10) as response:
                        if response.status != 200:
                            logger.error(f"Failed to fetch {url}: Status {response.status}, attempt {attempt + 1}/{self.max_retries}")
                            if attempt < self.max_retries - 1:
                                await asyncio.sleep(self.retry_delay)
                            continue
                        content = await response.text()
                        logger.debug(f"Fetched page {url}: {len(content)} bytes")
                        return content
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.error(f"Error fetching {url}: {str(e)}, attempt {attempt + 1}/{self.max_retries}\n{traceback.format_exc()}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay)
        logger.error(f"Failed to fetch {url} after {self.max_retries} attempts")
        return None

    async def parse_api_changes(self, exchange: str, content: str) -> List[Dict]:
        """
        Parse API change log from webpage content.

        Args:
            exchange (str): Exchange name (e.g., 'binance', 'okx')
            content (str): HTML content of the page

        Returns:
            List[Dict]: List of changes with exchange, date, description, timestamp, category
        """
        try:
            soup = BeautifulSoup(content, 'html.parser')
            changes = []
            timestamp = datetime.now(timezone(timedelta(hours=8))).isoformat()
            if exchange == 'binance':
                # Parse Binance change log (example: extract list items from changelog)
                changelog = soup.find('div', class_='changelog-content')
                if changelog:
                    for item in changelog.find_all('li'):
                        text = item.get_text(strip=True)
                        date_match = re.search(r'(\d{4}-\d{2}-\d{2})', text)
                        date = date_match.group(1) if date_match else datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d')
                        category = 'critical' if 'error' in text.lower() else 'general'
                        changes.append({
                            'exchange': exchange,
                            'date': date,
                            'description': text,
                            'timestamp': timestamp,
                            'category': category
                        })
            elif exchange == 'okx':
                # Parse OKX change log (example: extract version updates)
                updates = soup.find_all('div', class_='update-item')
                for update in updates:
                    date = update.find('span', class_='date')
                    desc = update.find('div', class_='description')
                    date = date.get_text(strip=True) if date else datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d')
                    desc = desc.get_text(strip=True) if desc else 'No description'
                    category = 'critical' if 'breaking' in desc.lower() else 'general'
                    changes.append({
                        'exchange': exchange,
                        'date': date,
                        'description': desc,
                        'timestamp': timestamp,
                        'category': category
                    })
            logger.debug(f"Parsed {len(changes)} API changes for {exchange}")
            return changes
        except Exception as e:
            logger.error(f"Failed to parse API changes for {exchange}: {str(e)}\n{traceback.format_exc()}")
            return []

    async def save_api_changes(self, changes: List[Dict]):
        """
        Save API changes to logs/api_changes.log and data/processed/api_changes.json.

        Args:
            changes (List[Dict]): List of API changes to save
        """
        if not changes:
            logger.debug("No API changes to save")
            return
        try:
            # Log to api_changes.log
            for change in changes:
                api_logger.info(f"Detected API changes for {change['exchange']}: {change['description']}")

            # Append to api_changes.json
            existing_changes = []
            if self.api_changes_file.exists():
                try:
                    with self.api_changes_file.open('r', encoding='utf-8') as f:
                        existing_changes = json.load(f)
                        if not isinstance(existing_changes, list):
                            existing_changes = []
                except json.JSONDecodeError:
                    logger.error(f"Invalid JSON in {self.api_changes_file}, starting with empty list")
                    existing_changes = []
            existing_changes.extend(changes)
            with self.api_changes_file.open('w', encoding='utf-8') as f:
                json.dump(existing_changes, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved {len(changes)} API changes to {self.api_changes_file}")

            # Save to database if callback is provided
            if self.db_callback:
                try:
                    for change in changes:
                        self.db_callback(change)
                    logger.info(f"Saved {len(changes)} API changes to database")
                except Exception as e:
                    logger.error(f"Failed to save API changes to database: {str(e)}\n{traceback.format_exc()}")
        except Exception as e:
            logger.error(f"Failed to save API changes: {str(e)}\n{traceback.format_exc()}")

    async def check_updates(self):
        """
        Check for updates in exchange API documentation.
        """
        for exchange, config in self.exchanges.items():
            url = config.get('url')
            if not url:
                logger.warning(f"No URL configured for {exchange}, skipping")
                continue
            last_check = self.last_check.get(exchange, 0)
            now = datetime.now(timezone(timedelta(hours=8))).timestamp()
            if now - last_check < self.check_interval:
                logger.debug(f"Skipping {exchange} API check, last checked at {datetime.fromtimestamp(last_check, tz=timezone(timedelta(hours=8)))}")
                continue
            content = await self.fetch_page(url)
            if content:
                changes = await self.parse_api_changes(exchange, content)
                if changes:
                    await self.save_api_changes(changes)
                self.last_check[exchange] = now
                logger.debug(f"Updated last check time for {exchange} to {datetime.fromtimestamp(now, tz=timezone(timedelta(hours=8)))}")

    def start(self):
        """
        Start monitoring configuration files and exchange APIs.
        """
        if self.running:
            logger.warning("FileMonitor already running")
            return
        self.running = True
        self.observer.schedule(self.handler, str(self.config_path), recursive=False)
        self.observer.start()
        logger.info(f"Started monitoring configuration files in {self.config_path}")
        self.start_module_monitoring()
        asyncio.create_task(self.run_api_monitor())
        logger.info("Started API monitoring")

    async def run_api_monitor(self):
        """
        Run periodic API monitoring in a loop.
        """
        while self.running:
            try:
                await self.check_updates()
                await asyncio.sleep(self.check_interval)
            except Exception as e:
                logger.error(f"Error in API monitoring loop: {str(e)}\n{traceback.format_exc()}")
                await asyncio.sleep(self.retry_delay)

    def stop(self):
        """
        Stop monitoring configuration files and exchange APIs.
        """
        if not self.running:
            logger.warning("FileMonitor not running")
            return
        self.running = False
        self.observer.stop()
        self.observer.join()
        self.module_observer.stop()
        self.module_observer.join()
        logger.info("Stopped FileMonitor")
        
    def register_module_callback(self, module_name: str, callback: Callable[[str, str], None]):
        """
        注册模块重载回调函数
        
        Args:
            module_name (str): 模块文件名（如'signal_generator.py'）
            callback (Callable): 重载时调用的函数，接收模块名和文件路径
        """
        self.module_callbacks[module_name] = callback
        logger.debug(f"Registered module callback for {module_name}")

    def notify_module_update(self, module_name: str, file_path: str):
        """
        通知注册的回调函数模块已更新
        
        Args:
            module_name (str): 模块文件名
            file_path (str): 模块文件路径
        """
        if module_name in self.module_callbacks:
            try:
                self.module_callbacks[module_name](module_name, file_path)
                logger.info(f"Notified module update for {module_name}")
            except Exception as e:
                logger.error(f"Failed to notify module update for {module_name}: {str(e)}\n{traceback.format_exc()}")

    def start_module_monitoring(self):
        """
        启动核心模块文件监控
        """
        try:
            # 获取核心模块路径
            core_path = path_manager.core_path
            
            # 监控每个核心模块
            for module_name in self.core_modules:
                module_path = core_path / module_name
                if module_path.exists():
                    self.module_observer.schedule(self.module_handler, str(module_path.parent), recursive=False)
                    logger.info(f"Started monitoring for {module_name} at {module_path}")
                else:
                    logger.warning(f"Core module {module_name} not found at {module_path}")
            
            self.module_observer.start()
            logger.info("Started core module monitoring")
            
        except Exception as e:
            logger.error(f"Failed to start module monitoring: {str(e)}\n{traceback.format_exc()}")

class ModuleReloadHandler(FileSystemEventHandler):
    """处理Python模块文件修改事件的处理器"""
    
    def __init__(self, file_monitor):
        """
        初始化模块重载处理器
        
        Args:
            file_monitor: FileMonitor实例
        """
        self.file_monitor = file_monitor
        self.logger = getLogger(__name__)
        self.logger.debug("Initialized ModuleReloadHandler")
    
    def on_modified(self, event):
        """
        处理Python模块文件修改事件
        
        Args:
            event: 文件系统事件
        """
        try:
            if not event.is_directory and event.src_path.endswith('.py'):
                file_name = Path(event.src_path).name
                if file_name in self.file_monitor.module_callbacks:
                    self.logger.info(f"Detected modification in {event.src_path}, scheduling reload for {file_name}")
                    self.file_monitor.notify_module_update(file_name, event.src_path)
        except Exception as e:
            self.logger.error(f"Module reload handler error: {str(e)}\n{traceback.format_exc()}")
