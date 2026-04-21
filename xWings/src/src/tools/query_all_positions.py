# File: src/tools/query_all_positions.py
# Purpose: Query and manage OKX subaccount balance and positions with logging and database storage
# Dependencies: yaml, ccxt.async_support, asyncio, os, logging
# Notes: Supports asynchronous querying, error handling, and data persistence
# Updated: 2025-08-05

import yaml
import ccxt.async_support as ccxt
import asyncio
import os
import logging
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)
CONFIG_PATH = os.path.join(os.path.dirname(__file__), '../../src/config/client_credentials.yaml')

class PositionManager:
    def __init__(self, exchange, database=None):
        self.exchange = exchange
        self.database = database

    async def fetch_balance(self, subaccount_id):
        """Fetch account balance"""
        try:
            balance = await self.exchange.fetch_balance()
            if self.database:
                await self.database.save_balance({
                    'subaccount': subaccount_id,
                    'timestamp': datetime.now(timezone(timedelta(hours=8))),
                    'balance': balance['total']
                })
            return balance['total']
        except Exception as e:
            logger.error(f"Fetch balance failed for {subaccount_id}: {str(e)}")
            return {}

    async def fetch_positions(self, subaccount_id, symbol=None, market_type=None):
        """Fetch positions for a subaccount"""
        try:
            positions = await self.exchange.fetch_positions(symbol, market_type)
            non_zero_positions = [p for p in positions if float(p.get('contracts', 0)) != 0]
            if self.database and non_zero_positions:
                await self.database.save_positions([{
                    'subaccount': subaccount_id,
                    'timestamp': datetime.now(timezone(timedelta(hours=8))),
                    'symbol': p['symbol'],
                    'side': p['side'],
                    'quantity': p['contracts'],
                    'avg_price': p.get('entryPrice', p.get('avgPrice'))
                } for p in non_zero_positions])
            return non_zero_positions
        except Exception as e:
            logger.error(f"Fetch positions failed for {subaccount_id}: {str(e)}")
            return []

async def query_okx_subaccount(api_key, api_secret, passphrase, subaccount_name, database=None):
    """
    Query balance and positions for an OKX subaccount.
    
    Args:
        api_key (str): API key for OKX
        api_secret (str): API secret for OKX
        passphrase (str): Passphrase for OKX API
        subaccount_name (str): Name of the subaccount
        database: Database instance for storing results
    """
    exchange = ccxt.okx({
        'apiKey': api_key,
        'secret': api_secret,
        'password': passphrase,
        'enableRateLimit': True,
    })
    position_manager = PositionManager(exchange, database)
    try:
        logger.info(f"Querying OKX subaccount: {subaccount_name}")
        # Fetch balance
        balance = await position_manager.fetch_balance(subaccount_name)
        logger.info(f"Subaccount {subaccount_name} balance: {balance}")
        print(f"\n==== OKX Subaccount: {subaccount_name} ====")
        print("  Account Balance (Partial):")
        for asset, amount in balance.items():
            print(f"    {asset}: {amount}")

        # Fetch positions
        positions = await position_manager.fetch_positions(subaccount_name, market_type='perpetual')
        logger.info(f"Subaccount {subaccount_name} positions: {len(positions)} non-zero positions")
        print("  Positions:")
        if positions:
            for pos in positions:
                print(f"    {pos['symbol']} {pos['side']} Quantity: {pos['quantity']} AvgPrice: {pos['avg_price']}")
        else:
            print("  No positions")
    finally:
        await exchange.close()

async def main():
    """
    Load OKX subaccount configurations and query their balance and positions.
    """
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    subaccounts = config.get('subaccounts', {})
    tasks = []
    # Process only OKX subaccounts
    for ex, sub_dict in subaccounts.items():
        if ex.lower() != 'okx':
            continue
        for sub_name, sub_cfg in sub_dict.items():
            api_key = sub_cfg.get('api_key')
            api_secret = sub_cfg.get('api_secret')
            passphrase = sub_cfg.get('passphrase')
            if api_key and api_secret and passphrase:
                tasks.append(query_okx_subaccount(api_key, api_secret, passphrase, sub_name))
    if not tasks:
        logger.warning("No OKX subaccount configurations found")
        print("No OKX subaccount configurations found")
        return
    await asyncio.gather(*tasks)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())