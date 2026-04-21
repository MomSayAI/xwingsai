# File: src/tools/signal_query.py
# Purpose: Signal Query ToolFor viewing and analyzing signal data in MongoDB
# Updated: 2025-08-05

import asyncio
import logging
import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional

# Set project paths
PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_PATH = PROJECT_ROOT / 'src'
sys.path.insert(0, str(SRC_PATH))

from core.database import Database
from core.config_manager import ConfigManager
from core.utils import PathManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SignalQueryTool:
    """Signal Query Tool Class"""
    
    def __init__(self):
        self.path_manager = PathManager(PROJECT_ROOT)
        self.config_manager = ConfigManager(key_file=str(self.path_manager.get_config_path('encryption.key')))
        self.db_config = self.config_manager.load_config('parameters_encrypted.yaml')['database']
        self.database = Database(self.db_config)
    
    async def get_signals_summary(self, subaccount_id: str = None, 
                                symbol: str = None, market_type: str = None,
                                timeframe: str = None, days: int = 7) -> Dict[str, Any]:
        """
        Retrieve summary statistics of signals
        
        Args:
            subaccount_id: Subaccount ID
            symbol: Trading pair
            market_type: Market type
            timeframe: Timeframe
            days: Number of days to query
            
        Returns:
            Dict: Signal summary statistics
        """
        try:
            end_time = datetime.now(timezone(timedelta(hours=8)))
            start_time = end_time - timedelta(days=days)
            
            signals = await self.database.get_signals(
                subaccount_id=subaccount_id,
                symbol=symbol,
                market_type=market_type,
                timeframe=timeframe,
                start_time=start_time,
                end_time=end_time,
                limit=1000
            )
            
            if not signals:
                return {"message": "No signals found", "total": 0}
            
            # Aggregate signal types
            signal_types = {}
            symbols = {}
            timeframes = {}
            
            for signal in signals:
                # Signal type statistics
                signal_type = signal.get('signal_type', 'unknown')
                signal_types[signal_type] = signal_types.get(signal_type, 0) + 1
                
                # Trading pair statistics
                symbol_key = f"{signal.get('symbol', 'unknown')}_{signal.get('market_type', 'unknown')}"
                symbols[symbol_key] = symbols.get(symbol_key, 0) + 1
                
                # Timeframe statistics
                tf = signal.get('timeframe', 'unknown')
                timeframes[tf] = timeframes.get(tf, 0) + 1
            
            return {
                "total_signals": len(signals),
                "period": f"{start_time.strftime('%Y-%m-%d %H:%M')} to {end_time.strftime('%Y-%m-%d %H:%M')}",
                "signal_types": signal_types,
                "symbols": symbols,
                "timeframes": timeframes,
                "latest_signals": signals[:5]  # Latest 5 signals
            }
            
        except Exception as e:
            logger.error(f"Failed to get signals summary: {str(e)}")
            return {"error": str(e)}
    
    async def get_latest_signals(self, limit: int = 10, 
                               subaccount_id: str = None,
                               symbol: str = None) -> List[Dict[str, Any]]:
        """
        Retrieve latest signals
        
        Args:
            limit: Number of signals to return
            subaccount_id: Subaccount ID
            symbol: Trading pair
            
        Returns:
            List: List of latest signals
        """
        try:
            signals = await self.database.get_signals(
                subaccount_id=subaccount_id,
                symbol=symbol,
                limit=limit
            )
            
            return signals
            
        except Exception as e:
            logger.error(f"Failed to get latest signals: {str(e)}")
            return []
    
    async def get_signal_performance(self, subaccount_id: str, symbol: str,
                                   market_type: str, timeframe: str,
                                   days: int = 30) -> Dict[str, Any]:
        """
        Retrieve signal performance analysis
        
        Args:
            subaccount_id: Subaccount ID
            symbol: Trading pair
            market_type: Market type
            timeframe: Timeframe
            days: Number of days for analysis
            
        Returns:
            Dict: Signal performance analysis
        """
        try:
            end_time = datetime.now(timezone(timedelta(hours=8)))
            start_time = end_time - timedelta(days=days)
            
            signals = await self.database.get_signals(
                subaccount_id=subaccount_id,
                symbol=symbol,
                market_type=market_type,
                timeframe=timeframe,
                start_time=start_time,
                end_time=end_time,
                limit=1000
            )
            
            if not signals:
                return {"message": "No signals found for analysis"}
            
            # Analyze signal performance
            buy_signals = [s for s in signals if s.get('signal_type') == 'buy']
            sell_signals = [s for s in signals if s.get('signal_type') == 'sell']
            
            # Calculate signal intervals
            signal_intervals = []
            for i in range(1, len(signals)):
                prev_time = signals[i-1]['timestamp']
                curr_time = signals[i]['timestamp']
                if isinstance(prev_time, str):
                    prev_time = datetime.fromisoformat(prev_time.replace('Z', '+00:00'))
                if isinstance(curr_time, str):
                    curr_time = datetime.fromisoformat(curr_time.replace('Z', '+00:00'))
                interval = (curr_time - prev_time).total_seconds() / 3600  # Hours
                signal_intervals.append(interval)
            
            return {
                "total_signals": len(signals),
                "buy_signals": len(buy_signals),
                "sell_signals": len(sell_signals),
                "avg_interval_hours": sum(signal_intervals) / len(signal_intervals) if signal_intervals else 0,
                "min_interval_hours": min(signal_intervals) if signal_intervals else 0,
                "max_interval_hours": max(signal_intervals) if signal_intervals else 0,
                "period": f"{start_time.strftime('%Y-%m-%d')} to {end_time.strftime('%Y-%m-%d')}",
                "signals": signals
            }
            
        except Exception as e:
            logger.error(f"Failed to get signal performance: {str(e)}")
            return {"error": str(e)}
    
    async def export_signals_to_csv(self, output_file: str, 
                                  subaccount_id: str = None,
                                  symbol: str = None,
                                  market_type: str = None,
                                  timeframe: str = None,
                                  days: int = 30) -> bool:
        """
        Export signals to CSV file
        
        Args:
            output_file: Output file path
            subaccount_id: Subaccount ID
            symbol: Trading pair
            market_type: Market type
            timeframe: Timeframe
            days: Number of days to export
            
        Returns:
            bool: Success status
        """
        try:
            import pandas as pd
            
            end_time = datetime.now(timezone(timedelta(hours=8)))
            start_time = end_time - timedelta(days=days)
            
            signals = await self.database.get_signals(
                subaccount_id=subaccount_id,
                symbol=symbol,
                market_type=market_type,
                timeframe=timeframe,
                start_time=start_time,
                end_time=end_time,
                limit=10000
            )
            
            if not signals:
                logger.warning("No signals to export")
                return False
            
            # Convert to DataFrame
            df = pd.DataFrame(signals)
            
            # Save to CSV
            df.to_csv(output_file, index=False)
            logger.info(f"Exported {len(signals)} signals to {output_file}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to export signals: {str(e)}")
            return False
    
    def close(self):
        """Close database connection"""
        if self.database:
            self.database.close()

async def main():
    """Main function"""
    tool = SignalQueryTool()
    
    try:
        print("=== xWings Signal Query Tool ===\n")
        
        # 1. Retrieve signal summary
        print("1. Signal summary for last 7 days:")
        summary = await tool.get_signals_summary(days=7)
        print(f"   Total signals: {summary.get('total_signals', 0)}")
        print(f"   Signal type distribution: {summary.get('signal_types', {})}")
        print(f"   Symbol distribution: {summary.get('symbols', {})}")
        print(f"   Timeframe distribution: {summary.get('timeframes', {})}")
        
        # 2. Retrieve latest signals
        print("\n2. Latest 10 signals:")
        latest_signals = await tool.get_latest_signals(limit=10)
        for i, signal in enumerate(latest_signals[:5], 1):
            print(f"   {i}. {signal.get('timestamp', 'N/A')} - {signal.get('symbol', 'N/A')} "
                  f"{signal.get('signal_type', 'N/A')} at {signal.get('price', 'N/A')}")
        
        # 3. Signal performance for specific configuration
        print("\n3. BTC/USDT perpetual 3m signal performance:")
        performance = await tool.get_signal_performance(
            subaccount_id="xWings",
            symbol="BTC/USDT",
            market_type="perpetual",
            timeframe="3m",
            days=7
        )
        print(f"   Total signals: {performance.get('total_signals', 0)}")
        print(f"   Buy signals: {performance.get('buy_signals', 0)}")
        print(f"   Sell signals: {performance.get('sell_signals', 0)}")
        print(f"   Average interval: {performance.get('avg_interval_hours', 0):.2f} hours")
        
        # 4. Export signals
        print("\n4. Export signals to CSV:")
        success = await tool.export_signals_to_csv(
            output_file="signals_export.csv",
            days=7
        )
        print(f"   Export result: {'Success' if success else 'Failed'}")
        
    except Exception as e:
        logger.error(f"Main function error: {str(e)}")
    finally:
        tool.close()

if __name__ == "__main__":
    asyncio.run(main())