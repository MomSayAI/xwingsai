# File: src/core/analytics.py
# Purpose: Analyze trading signals, price differences, and performance metrics
# Dependencies: pandas, numpy, core.exchange, core.config_manager, core.utils
# Notes: Supports nested subaccounts. Timestamps in UTC+8.
#        Enhanced with thread safety, async optimization, and performance monitoring.
#        Uses markets_encrypted.yaml for timeframes and symbols.
#        Removed configure_logging to prevent duplicate initialization.
# Updated: 2025-07-06

import sys
from pathlib import Path
import pandas as pd
import numpy as np
import logging
from typing import List, Dict, Any
from datetime import datetime, timedelta, timezone
import asyncio
import threading
from time import time

# Set project root and ensure paths
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.extend([str(path) for path in [PROJECT_ROOT, PROJECT_ROOT / 'src', PROJECT_ROOT / 'src/core'] if str(path) not in sys.path])

from core.utils import PathManager
from core.exchange import ExchangeAPI
from core.config_manager import ConfigManager

# Initialize logging
path_manager = PathManager(PROJECT_ROOT)
logger = logging.getLogger(__name__)


class Analytics:
    def __init__(self):
        self.lock = threading.Lock()
        # Loaded configuration key again, can be omitted or passed as a parameter, to be optimized.
        self.config_manager = ConfigManager(key_file=str(path_manager.get_config_path("encryption.key")))
        self.performance_metrics = {"api_call_duration": []}

    async def track_signal_ohlc(self, df: pd.DataFrame, signal_col: str = "signal", k_lines: int = 5) -> List[Dict[str, Any]]:
        """
        Track OHLC data following trading signals. Timestamps in UTC+8.

        Args:
            df (pd.DataFrame): DataFrame with OHLC and signal data
            signal_col (str): Column name for signals (default: 'signal')
            k_lines (int): Number of K-lines to track (default: 5)

        Returns:
            List[Dict[str, Any]]: List of signal OHLC data
        """
        async with asyncio.Lock():
            try:
                df = self._prepare_dataframe(df, signal_col)
                signal_indices = df[df[signal_col].str.contains("Buy at|Sell at", na=False)].index
                ohlc_after_signals = []
                for idx in signal_indices:
                    signal_time = idx
                    signal_type = df.loc[idx, signal_col].split(" at ")[0]
                    next_k_lines = df.loc[signal_time:].head(k_lines + 1)[["open", "high", "low", "close"]]
                    if len(next_k_lines) > 1:
                        ohlcv_data = next_k_lines[1:].to_dict("records")
                        for record in ohlcv_data:
                            record["timestamp"] = next_k_lines.index[ohlcv_data.index(record)]
                        ohlc_after_signals.append({
                            "signal_time": signal_time,
                            "signal_type": signal_type,
                            "ohlc": ohlcv_data
                        })
                logger.debug(f"Tracked {len(ohlc_after_signals)} signals for {k_lines} K-lines")
                return ohlc_after_signals
            except Exception as e:
                logger.error(f"Track signal OHLC error: {str(e)}")
                raise

    async def calculate_price_difference(
        self, df: pd.DataFrame, exchange: ExchangeAPI, symbol: str, market_type: str, subaccount_id: str, leverage: float = None
    ) -> List[tuple]:
        """
        Calculate price differences and profits/losses between consecutive trades.

        Args:
            df (pd.DataFrame): DataFrame with trade data
            exchange (ExchangeAPI): Exchange instance
            symbol (str): Trading pair symbol
            market_type (str): Market type (spot, perpetual)
            subaccount_id (str): Subaccount identifier
            leverage (float, optional): Leverage value

        Returns:
            List[tuple]: List of price difference tuples
        """
        async with asyncio.Lock():
            try:
                start_time = time()
                df = self._prepare_dataframe(df, "trade_price")
                subaccount_config = self.config_manager.get_subaccount_config(subaccount_id, exchange.exchange_name)
                markets_config = self.config_manager.get_config("markets_encrypted")
                valid_leverages = markets_config.get("exchanges", {}).get(exchange.exchange_name, {}).get("markets", {}).get("crypto", {}).get(market_type, {}).get("leverages", [1])
                leverage = float(subaccount_config.get("trading_config", {}).get("symbols", {}).get(symbol, {}).get("leverage", leverage or valid_leverages[0])) if market_type == "perpetual" else 1.0
                trade_prices = df[df["trade_price"].notna()]["trade_price"].tolist()
                signals = df[df["trade_price"].notna()]["signal"].apply(lambda x: x.split(" at ")[0]).tolist()
                timestamps = df[df["trade_price"].notna()].index.tolist()
                price_differences = []
                fees = await exchange.fetch_trading_fees(symbol, params={"market_type": market_type})
                taker_fee = fees.get("taker", 0.001)

                for i in range(1, len(trade_prices)):
                    curr_signal = signals[i]
                    curr_price = trade_prices[i]
                    curr_time = timestamps[i]
                    prev_signal = signals[i-1]
                    prev_price = trade_prices[i-1]
                    prev_time = timestamps[i-1]
                    if (curr_signal == "Sell" and prev_signal == "Buy") or (curr_signal == "Buy" and prev_signal == "Sell"):
                        gross_profit = abs(curr_price - prev_price) * leverage
                        fee = (curr_price + prev_price) * taker_fee * leverage
                        net_profit = gross_profit - fee
                        loss_after_fee = -net_profit if net_profit < 0 else np.nan
                        price_differences.append((prev_time, curr_time, prev_price, curr_price, gross_profit, net_profit, loss_after_fee, prev_signal, curr_signal))
                        df.loc[curr_time, ["gross_profit", "net_profit", "loss_after_fee"]] = [gross_profit, net_profit, loss_after_fee]

                if len(trade_prices) > 0:
                    df.loc[timestamps[0], ["gross_profit", "net_profit", "loss_after_fee"]] = [0, 0, np.nan]

                self.performance_metrics["api_call_duration"].append(time() - start_time)
                logger.debug(f"Calculated {len(price_differences)} price differences for {subaccount_id}/{symbol}/{market_type}, API call took {time() - start_time:.3f}s")
                return price_differences
            except Exception as e:
                logger.error(f"Calculate price difference error: {str(e)}")
                raise

    async def calculate_metrics(
        self, df: pd.DataFrame, exchange: ExchangeAPI, symbol: str, market_type: str, subaccount_id: str, 
        initial_capital: float = None, risk_free_rate: float = 0.02
    ) -> Dict[str, float]:
        """
        Calculate performance metrics based on trade data.

        Args:
            df (pd.DataFrame): DataFrame with trade data
            exchange (ExchangeAPI): Exchange instance
            symbol (str): Trading pair symbol
            market_type (str): Market type (spot, perpetual)
            subaccount_id (str): Subaccount identifier
            initial_capital (float, optional): Initial capital
            risk_free_rate (float): Risk-free rate (default: 0.02)

        Returns:
            Dict[str, float]: Performance metrics
        """
        async with asyncio.Lock():
            try:
                start_time = time()
                df = self._prepare_dataframe(df, "trade_price")
                subaccount_config = self.config_manager.get_subaccount_config(subaccount_id, exchange.exchange_name)
                markets_config = self.config_manager.get_config("markets_encrypted")
                valid_leverages = markets_config.get("exchanges", {}).get(exchange.exchange_name, {}).get("markets", {}).get("crypto", {}).get(market_type, {}).get("leverages", [1])
                leverage = float(subaccount_config.get("trading_config", {}).get("symbols", {}).get(symbol, {}).get("leverage", valid_leverages[0])) if market_type == "perpetual" else 1.0
                initial_capital = subaccount_config.get("initial_capital", 10000.0) if initial_capital is None else initial_capital

                price_differences = await self.calculate_price_difference(df, exchange, symbol, market_type, subaccount_id, leverage)
                trades = df[df["net_profit"].notna()]
                total_trades = len(trades)
                if total_trades == 0:
                    current_balance = float((await exchange.fetch_balance(subaccount_id)).get("total", {}).get("USDT", 0.0))
                    metrics = {
                        "base_win_rate": 0.0, "net_win_rate": 0.0, "profit_loss_ratio": 0.0, "sharpe_ratio": 0.0,
                        "net_return": 0.0, "fee_ratio": 0.0, "avg_profit_per_trade": 0.0, "avg_loss_per_trade": 0.0,
                        "current_balance": current_balance, "total_asset_value": current_balance, "market_type": market_type
                    }
                else:
                    profitable_trades = len(trades[trades["net_profit"] > 0])
                    base_win_rate = (len(trades[trades["gross_profit"] > 0]) / total_trades * 100)
                    net_win_rate = (profitable_trades / total_trades * 100)
                    avg_profit = trades[trades["gross_profit"] > 0]["gross_profit"].mean() or 0.0
                    avg_loss = trades[trades["net_profit"] < 0]["loss_after_fee"].mean() or 0.0
                    profit_loss_ratio = avg_profit / abs(avg_loss) if avg_loss != 0 else float("inf")
                    returns = trades["net_profit"] / initial_capital
                    sharpe_ratio = (returns.mean() - risk_free_rate) / returns.std() if returns.std() > 0 else 0.0
                    net_return = trades["net_profit"].sum() / initial_capital
                    fees = await exchange.fetch_trading_fees(symbol, params={"market_type": market_type})
                    total_fees = trades["net_profit"].count() * (trades["trade_price"].mean() * fees.get("taker", 0.001) * 2)
                    fee_ratio = total_fees / trades["gross_profit"].sum() if trades["gross_profit"].sum() > 0 else 0.0
                    current_balance = float((await exchange.fetch_balance(subaccount_id)).get("total", {}).get("USDT", 0.0))
                    total_asset_value = current_balance
                    if hasattr(exchange, "fetch_asset_valuation"):
                        try:
                            valuation = await exchange.fetch_asset_valuation(subaccount_id)
                            total_asset_value = float(valuation.get("totalVal", current_balance))
                        except Exception as e:
                            logger.warning(f"Asset valuation failed: {str(e)}")
                    metrics = {
                        "base_win_rate": base_win_rate, "net_win_rate": net_win_rate, "profit_loss_ratio": profit_loss_ratio,
                        "sharpe_ratio": sharpe_ratio, "net_return": net_return, "fee_ratio": fee_ratio,
                        "avg_profit_per_trade": avg_profit, "avg_loss_per_trade": avg_loss,
                        "current_balance": current_balance, "total_asset_value": total_asset_value, "market_type": market_type
                    }

                self.performance_metrics["api_call_duration"].append(time() - start_time)
                logger.debug(f"Calculated metrics for {subaccount_id}/{symbol}/{market_type}, API call took {time() - start_time:.3f}s")
                return metrics
            except Exception as e:
                logger.error(f"Calculate metrics error: {str(e)}")
                raise

    async def periodic_analysis(
        self, df: pd.DataFrame, exchange: ExchangeAPI, symbol: str, market_type: str, subaccount_id: str, period: str = "D"
    ) -> Dict[pd.Timestamp, Dict[str, float]]:
        """
        Perform periodic analysis of metrics. Timestamps in UTC+8.

        Args:
            df (pd.DataFrame): DataFrame with trade data
            exchange (ExchangeAPI): Exchange instance
            symbol (str): Trading pair symbol
            market_type (str): Market type (spot, perpetual)
            subaccount_id (str): Subaccount identifier
            period (str): Period for grouping (default: 'D')

        Returns:
            Dict[pd.Timestamp, Dict[str, float]]: Metrics by period
        """
        async with asyncio.Lock():
            try:
                df = self._prepare_dataframe(df, "trade_price")
                grouped = df.groupby(pd.Grouper(freq=period))
                metrics_by_period = {}
                for period_name, group in grouped:
                    metrics = await self.calculate_metrics(group, exchange, symbol, market_type, subaccount_id)
                    metrics_by_period[period_name] = metrics
                logger.debug(f"Periodic analysis completed for {subaccount_id}/{symbol}/{market_type}, period={period}")
                return metrics_by_period
            except Exception as e:
                logger.error(f"Periodic analysis error: {str(e)}")
                raise

    def _prepare_dataframe(self, df: pd.DataFrame, required_col: str) -> pd.DataFrame:
        """
        Prepare DataFrame with consistent UTC+8 index and required columns.

        Args:
            df (pd.DataFrame): Input DataFrame
            required_col (str): Required column name

        Returns:
            pd.DataFrame: Processed DataFrame
        """
        try:
            if required_col not in df.columns:
                logger.error(f"DataFrame missing required column: {required_col}")
                raise ValueError(f"DataFrame missing required column: {required_col}")
            df = df.copy()
            if not isinstance(df.index, pd.DatetimeIndex):
                if "timestamp" not in df.columns:
                    logger.error("DataFrame must have 'timestamp' column or DatetimeIndex")
                    raise ValueError("DataFrame must have 'timestamp' column or DatetimeIndex")
                df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True).dt.tz_convert(timezone(timedelta(hours=8)))
                df.set_index("timestamp", inplace=True)
            return df
        except Exception as e:
            logger.error(f"DataFrame preparation error: {str(e)}")
            raise