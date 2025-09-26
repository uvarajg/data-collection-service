#!/usr/bin/env python3
"""
Simplified Polygon.io Data Collection for Specific Date Ranges
Collects historical data for a date range using existing enriched ticker list
"""

import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
import aiohttp
from dotenv import load_dotenv

# Load environment
load_dotenv('/workspaces/data-collection-service/.env')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SimpleRateLimiter:
    """Rate limiter for Polygon API - Starter plan has UNLIMITED calls"""

    def __init__(self, max_calls=1000, period=60):
        """Unlimited plan - high limit to allow fast processing"""
        self.max_calls = max_calls
        self.period = period
        self.calls = []

    async def acquire(self):
        """Minimal rate limiting for unlimited plan"""
        # For unlimited plan, we can be aggressive
        # Just add a tiny delay to avoid overwhelming the API
        await asyncio.sleep(0.01)  # 10ms delay between requests
        self.calls.append(time.time())


class PolygonCollector:
    """Simplified Polygon data collector"""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.polygon.io"
        self.rate_limiter = SimpleRateLimiter()
        self.session = None
        self.output_path = Path("/workspaces/data/raw_data/polygon")
        self.output_path.mkdir(parents=True, exist_ok=True)

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def get_ohlcv(self, ticker: str, start_date: str, end_date: str) -> Optional[List[Dict]]:
        """Get OHLCV data for date range"""
        url = f"{self.base_url}/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}"
        params = {
            'apikey': self.api_key,
            'adjusted': 'true',
            'sort': 'asc',
            'limit': 50000
        }

        for attempt in range(3):
            try:
                await self.rate_limiter.acquire()
                async with self.session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get('status') == 'OK' and 'results' in data:
                            results = []
                            for item in data['results']:
                                results.append({
                                    'date': datetime.fromtimestamp(item['t'] / 1000).strftime('%Y-%m-%d'),
                                    'open': item['o'],
                                    'high': item['h'],
                                    'low': item['l'],
                                    'close': item['c'],
                                    'volume': item['v'],
                                    'vwap': item.get('vw'),
                                    'transactions': item.get('n')
                                })
                            return results
                        else:
                            logger.warning(f"{ticker}: No data - {data.get('message', 'Unknown')}")
                            return None
                    elif response.status == 429:
                        logger.warning(f"{ticker}: Rate limited, waiting...")
                        await asyncio.sleep(60)
                        continue
                    else:
                        logger.error(f"{ticker}: API error {response.status}")
                        return None
            except Exception as e:
                logger.error(f"{ticker}: Request failed - {e}")
                if attempt < 2:
                    await asyncio.sleep(5)
                continue

        return None

    async def save_data(self, ticker: str, data: List[Dict]):
        """Save data to JSON files (one per date)"""
        for record in data:
            date_obj = datetime.strptime(record['date'], '%Y-%m-%d')
            file_path = (self.output_path / ticker / str(date_obj.year) /
                        f"{date_obj.month:02d}" / f"{record['date']}.json")
            file_path.parent.mkdir(parents=True, exist_ok=True)

            with open(file_path, 'w') as f:
                json.dump({
                    'ticker': ticker,
                    'date': record['date'],
                    'open': record['open'],
                    'high': record['high'],
                    'low': record['low'],
                    'close': record['close'],
                    'volume': record['volume'],
                    'vwap': record.get('vwap'),
                    'transactions': record.get('transactions'),
                    'source': 'polygon.io',
                    'collected_at': datetime.now().isoformat()
                }, f, indent=2)

    async def collect_ticker(self, ticker: str, start_date: str, end_date: str) -> bool:
        """Collect data for one ticker"""
        try:
            data = await self.get_ohlcv(ticker, start_date, end_date)
            if data:
                await self.save_data(ticker, data)
                logger.info(f"✓ {ticker}: {len(data)} records")
                return True
            else:
                logger.warning(f"✗ {ticker}: No data")
                return False
        except Exception as e:
            logger.error(f"✗ {ticker}: {e}")
            return False


async def main(start_date: str, end_date: str, tickers: List[str] = None):
    """Main collection function"""

    # Get API key
    api_key = os.getenv('POLYGON_API_KEY')
    if not api_key:
        logger.error("POLYGON_API_KEY not set")
        sys.exit(1)

    # Get tickers from enriched data if not provided
    if not tickers:
        input_path = Path("/workspaces/data/input_source")
        enriched_files = sorted(input_path.glob("enriched_yfinance_*.json"), reverse=True)

        if not enriched_files:
            logger.error("No enriched data found")
            sys.exit(1)

        with open(enriched_files[0], 'r') as f:
            enriched_data = json.load(f)

        tickers = [stock['ticker'] for stock in enriched_data]
        logger.info(f"Using {len(tickers)} tickers from {enriched_files[0].name}")

    logger.info(f"Collecting {start_date} to {end_date} for {len(tickers)} tickers")

    # Collect data
    async with PolygonCollector(api_key) as collector:
        successful = 0
        failed = 0

        # Process in larger batches for unlimited API plan
        batch_size = 50  # Increased from 5 to 50 for unlimited plan
        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i + batch_size]
            logger.info(f"Batch {i//batch_size + 1}/{(len(tickers) + batch_size - 1)//batch_size}")

            tasks = [collector.collect_ticker(t, start_date, end_date) for t in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, Exception):
                    failed += 1
                elif result:
                    successful += 1
                else:
                    failed += 1

            # Very brief pause between batches
            if i + batch_size < len(tickers):
                await asyncio.sleep(0.5)  # 500ms instead of 5s

            # Progress update
            logger.info(f"Progress: {successful + failed}/{len(tickers)} ({successful} ✓, {failed} ✗)")

        logger.info(f"Complete: {successful} successful, {failed} failed")

        # Save summary
        summary = {
            'date_range': {'start': start_date, 'end': end_date},
            'total_tickers': len(tickers),
            'successful': successful,
            'failed': failed,
            'success_rate': f"{(successful/len(tickers)*100):.1f}%",
            'completed_at': datetime.now().isoformat()
        }

        summary_file = Path("/workspaces/data/raw_data/polygon") / f"summary_{start_date}_{end_date}.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)

        logger.info(f"Summary saved: {summary_file}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python collect_polygon_dates.py START_DATE END_DATE [TICKERS...]")
        print("Example: python collect_polygon_dates.py 2025-09-01 2025-09-14")
        print("Example: python collect_polygon_dates.py 2025-09-01 2025-09-14 AAPL MSFT GOOGL")
        sys.exit(1)

    start = sys.argv[1]
    end = sys.argv[2]
    ticker_list = sys.argv[3:] if len(sys.argv) > 3 else None

    asyncio.run(main(start, end, ticker_list))