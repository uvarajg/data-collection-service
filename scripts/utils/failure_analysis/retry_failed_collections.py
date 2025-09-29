#!/usr/bin/env python3
"""
Retry Failed Collections with Enhanced Logging and Validation

This script:
1. Validates ticker status before collection
2. Implements retry logic with exponential backoff
3. Provides detailed error logging for each failure
4. Categorizes failures based on actual error responses
"""

import os
import sys
import json
import asyncio
import aiohttp
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict
from pathlib import Path
from dotenv import load_dotenv

# Add project root to path
sys.path.append('/workspaces/data-collection-service')

# Load environment variables
load_dotenv('/workspaces/data-collection-service/.env')

# Configure detailed logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class EnhancedPolygonCollector:
    def __init__(self):
        self.api_key = os.getenv('POLYGON_API_KEY')
        if not self.api_key:
            raise ValueError("POLYGON_API_KEY not found in environment")

        self.base_path = "/workspaces/data/raw_data/polygon"
        self.error_log_path = "/workspaces/data/error_records/polygon_failures/retry_logs"
        self.enriched_data = self._load_enriched_data()

        # Create directories
        os.makedirs(self.error_log_path, exist_ok=True)

        # Collection statistics
        self.stats = {
            'total_attempted': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'retry_success': 0,
            'categories': defaultdict(int)
        }

        # Detailed failure categories
        self.failure_categories = {
            'RATE_LIMIT': 'API rate limit exceeded',
            'NO_DATA': 'No data available for date range',
            'INVALID_TICKER': 'Ticker not found or invalid',
            'TIMEOUT': 'Request timeout',
            'NETWORK_ERROR': 'Network or connection error',
            'API_ERROR': 'API returned error status',
            'DATA_QUALITY': 'Data quality validation failed',
            'ALREADY_COLLECTED': 'Data already exists',
            'BELOW_THRESHOLD': 'Market cap below threshold',
            'NON_US': 'Non-US company (now included)',
            'UNKNOWN': 'Unknown error'
        }

        # Detailed error log
        self.error_log = []

    def _load_enriched_data(self) -> Dict[str, Any]:
        """Load enriched YFinance data for fundamentals"""
        input_source_path = "/workspaces/data/input_source"
        try:
            yfinance_files = [f for f in os.listdir(input_source_path)
                            if f.startswith("enriched_yfinance_")]
            if not yfinance_files:
                return {}

            latest_file = sorted(yfinance_files)[-1]
            logger.info(f"Loading enriched data from {latest_file}")

            with open(f"{input_source_path}/{latest_file}", 'r') as f:
                data = json.load(f)

            # Convert list to dict for easier lookup
            ticker_lookup = {}
            for item in data:
                if 'ticker' in item:
                    ticker_lookup[item['ticker']] = item

            return ticker_lookup
        except Exception as e:
            logger.error(f"Error loading enriched data: {e}")
            return {}

    async def validate_ticker(self, session: aiohttp.ClientSession, ticker: str) -> Tuple[bool, str, Dict[str, Any]]:
        """
        Pre-collection validation of ticker
        Returns: (is_valid, category, details)
        """
        details = {'ticker': ticker}

        # Check if already collected
        ticker_path = os.path.join(self.base_path, ticker, "2025")
        if os.path.exists(ticker_path):
            file_count = sum(1 for root, _, files in os.walk(ticker_path)
                           for f in files if f.endswith('.json'))
            if file_count > 50:  # Threshold for "already collected"
                details['existing_files'] = file_count
                return False, 'ALREADY_COLLECTED', details

        # Check enriched data
        ticker_info = self.enriched_data.get(ticker, {})
        market_cap = ticker_info.get('market_cap', 0)
        country = ticker_info.get('country', 'Unknown')

        details['market_cap'] = market_cap
        details['country'] = country

        # Include non-US companies trading on US exchanges
        # (Changed based on user feedback)
        if country != 'United States' and country != 'Unknown':
            details['note'] = 'Non-US company on US exchange (now included)'
            # Don't skip - continue with collection

        # Check market cap threshold (still apply)
        if 0 < market_cap < 2_000_000_000:
            details['reason'] = f'Market cap ${market_cap:,.0f} below $2B threshold'
            return False, 'BELOW_THRESHOLD', details

        # Validate with Polygon API
        try:
            url = f"https://api.polygon.io/v3/reference/tickers/{ticker}"
            params = {'apikey': self.api_key}

            async with session.get(url, params=params, timeout=10) as response:
                if response.status == 404:
                    details['api_status'] = 404
                    return False, 'INVALID_TICKER', details
                elif response.status == 429:
                    details['api_status'] = 429
                    return False, 'RATE_LIMIT', details
                elif response.status != 200:
                    details['api_status'] = response.status
                    return False, 'API_ERROR', details

                data = await response.json()
                ticker_info = data.get('results', {})

                details['active'] = ticker_info.get('active', False)
                details['delisted'] = ticker_info.get('delisted_utc')
                details['market'] = ticker_info.get('market')
                details['primary_exchange'] = ticker_info.get('primary_exchange')

                # Check if delisted
                if ticker_info.get('delisted_utc'):
                    details['reason'] = f"Delisted on {ticker_info['delisted_utc']}"
                    return False, 'INVALID_TICKER', details

                # Check if active
                if not ticker_info.get('active', True):
                    details['reason'] = 'Ticker marked as inactive'
                    return False, 'INVALID_TICKER', details

                # Valid ticker
                return True, 'VALID', details

        except asyncio.TimeoutError:
            details['error'] = 'Validation timeout'
            return False, 'TIMEOUT', details
        except Exception as e:
            details['error'] = str(e)
            return False, 'NETWORK_ERROR', details

    async def collect_ticker_data(self, session: aiohttp.ClientSession, ticker: str,
                                start_date: str, end_date: str, retry_count: int = 0) -> Dict[str, Any]:
        """
        Collect data for a single ticker with retry logic
        """
        max_retries = 3
        backoff_factor = 2

        try:
            # Validate ticker first
            is_valid, category, details = await self.validate_ticker(session, ticker)

            if not is_valid:
                self.stats['categories'][category] += 1

                if category != 'ALREADY_COLLECTED':  # Don't log already collected as error
                    self.error_log.append({
                        'ticker': ticker,
                        'category': category,
                        'details': details,
                        'timestamp': datetime.now().isoformat()
                    })
                    logger.warning(f"{ticker}: {category} - {details.get('reason', '')}")
                else:
                    logger.info(f"{ticker}: Already collected ({details.get('existing_files')} files)")

                self.stats['skipped'] += 1
                return {'status': 'skipped', 'category': category, 'details': details}

            # Collect data
            url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}"
            params = {
                'apikey': self.api_key,
                'adjusted': 'true',
                'sort': 'asc',
                'limit': 50000
            }

            async with session.get(url, params=params, timeout=30) as response:
                if response.status == 429:  # Rate limit
                    if retry_count < max_retries:
                        wait_time = backoff_factor ** retry_count
                        logger.info(f"{ticker}: Rate limited, retrying in {wait_time}s...")
                        await asyncio.sleep(wait_time)
                        return await self.collect_ticker_data(session, ticker, start_date, end_date, retry_count + 1)
                    else:
                        self.stats['categories']['RATE_LIMIT'] += 1
                        self.error_log.append({
                            'ticker': ticker,
                            'category': 'RATE_LIMIT',
                            'details': {'max_retries_exceeded': True},
                            'timestamp': datetime.now().isoformat()
                        })
                        return {'status': 'failed', 'category': 'RATE_LIMIT'}

                elif response.status != 200:
                    self.stats['categories']['API_ERROR'] += 1
                    self.error_log.append({
                        'ticker': ticker,
                        'category': 'API_ERROR',
                        'details': {'status_code': response.status},
                        'timestamp': datetime.now().isoformat()
                    })
                    logger.error(f"{ticker}: API error {response.status}")
                    return {'status': 'failed', 'category': 'API_ERROR', 'status_code': response.status}

                data = await response.json()
                results = data.get('results', [])

                if not results:
                    self.stats['categories']['NO_DATA'] += 1
                    self.error_log.append({
                        'ticker': ticker,
                        'category': 'NO_DATA',
                        'details': {'date_range': f"{start_date} to {end_date}"},
                        'timestamp': datetime.now().isoformat()
                    })
                    logger.warning(f"{ticker}: No data available for date range")
                    return {'status': 'failed', 'category': 'NO_DATA'}

                # Process and save data
                saved_count = await self._process_and_save_data(ticker, results)

                if retry_count > 0:
                    self.stats['retry_success'] += 1
                    logger.info(f"{ticker}: Retry successful after {retry_count} attempts")

                self.stats['successful'] += 1
                logger.info(f"{ticker}: Successfully collected {len(results)} days, saved {saved_count} files")

                return {
                    'status': 'success',
                    'days_collected': len(results),
                    'files_saved': saved_count,
                    'retry_count': retry_count
                }

        except asyncio.TimeoutError:
            self.stats['categories']['TIMEOUT'] += 1
            self.error_log.append({
                'ticker': ticker,
                'category': 'TIMEOUT',
                'details': {'timeout': 30},
                'timestamp': datetime.now().isoformat()
            })
            logger.error(f"{ticker}: Request timeout")
            return {'status': 'failed', 'category': 'TIMEOUT'}

        except Exception as e:
            self.stats['categories']['UNKNOWN'] += 1
            self.error_log.append({
                'ticker': ticker,
                'category': 'UNKNOWN',
                'details': {'error': str(e)},
                'timestamp': datetime.now().isoformat()
            })
            logger.error(f"{ticker}: Unexpected error - {e}")
            return {'status': 'failed', 'category': 'UNKNOWN', 'error': str(e)}

    async def _process_and_save_data(self, ticker: str, results: List[Dict]) -> int:
        """Process and save collected data with enrichments"""
        saved_count = 0
        ticker_info = self.enriched_data.get(ticker, {})

        for day_data in results:
            try:
                # Convert timestamp to date
                timestamp = day_data['t'] / 1000
                date = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
                year, month, day = date.split('-')

                # Create directory structure
                dir_path = os.path.join(self.base_path, ticker, year, month)
                os.makedirs(dir_path, exist_ok=True)

                # Prepare data structure
                processed_data = {
                    'record_id': f"{ticker}_{date}_{int(datetime.now().timestamp())}",
                    'ticker': ticker,
                    'date': date,
                    'basic_data': {
                        'open': day_data.get('o'),
                        'high': day_data.get('h'),
                        'low': day_data.get('l'),
                        'close': day_data.get('c'),
                        'volume': day_data.get('v'),
                        'adjusted_close': day_data.get('c')  # Polygon provides adjusted data
                    },
                    'metadata': {
                        'collection_timestamp': datetime.now().isoformat(),
                        'data_source': 'polygon.io',
                        'processing_status': 'retry_collection',
                        'retry_collection': True
                    }
                }

                # Add fundamental data from enriched source
                if ticker_info:
                    processed_data['fundamental_data'] = {
                        'market_cap': ticker_info.get('market_cap'),
                        'pe_ratio': ticker_info.get('pe_ratio'),
                        'debt_to_equity': ticker_info.get('debt_to_equity'),
                        'roe_percent': ticker_info.get('roe'),
                        'current_ratio': ticker_info.get('current_ratio'),
                        'operating_margin_percent': ticker_info.get('operating_margin'),
                        'revenue_growth_percent': ticker_info.get('revenue_growth'),
                        'profit_margin_percent': ticker_info.get('profit_margin'),
                        'dividend_yield_percent': ticker_info.get('dividend_yield'),
                        'book_value': ticker_info.get('book_value')
                    }

                    processed_data['company_data'] = {
                        'sector': ticker_info.get('sector'),
                        'industry': ticker_info.get('industry'),
                        'country': ticker_info.get('country'),
                        'exchange': ticker_info.get('exchange')
                    }

                # Save to file
                file_path = os.path.join(dir_path, f"{date}.json")
                with open(file_path, 'w') as f:
                    json.dump(processed_data, f, indent=2)

                saved_count += 1

            except Exception as e:
                logger.error(f"Error processing {ticker} data for {date}: {e}")

        return saved_count

    async def retry_failed_collections(self, failed_tickers: List[str],
                                     start_date: str = "2025-06-01",
                                     end_date: str = "2025-09-01"):
        """
        Retry collection for failed tickers with enhanced logging and validation
        """
        logger.info(f"Starting retry collection for {len(failed_tickers)} failed tickers")
        logger.info(f"Date range: {start_date} to {end_date}")

        self.stats['total_attempted'] = len(failed_tickers)

        # Process in batches to avoid overwhelming the API
        batch_size = 20

        async with aiohttp.ClientSession() as session:
            for i in range(0, len(failed_tickers), batch_size):
                batch = failed_tickers[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                total_batches = (len(failed_tickers) + batch_size - 1) // batch_size

                logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} tickers)")

                tasks = [self.collect_ticker_data(session, ticker, start_date, end_date)
                        for ticker in batch]

                results = await asyncio.gather(*tasks)

                # Update statistics
                for result in results:
                    if result['status'] == 'success':
                        self.stats['successful'] += 1
                    elif result['status'] == 'failed':
                        self.stats['failed'] += 1

                # Progress update
                completed = min(i + batch_size, len(failed_tickers))
                success_rate = (self.stats['successful'] / completed) * 100 if completed > 0 else 0
                logger.info(f"Progress: {completed}/{len(failed_tickers)} "
                          f"(Success rate: {success_rate:.1f}%)")

                # Rate limiting between batches
                if i + batch_size < len(failed_tickers):
                    await asyncio.sleep(1)

        # Generate final report
        await self._generate_retry_report()

    async def _generate_retry_report(self):
        """Generate comprehensive retry collection report"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Calculate success metrics
        total = self.stats['total_attempted']
        successful = self.stats['successful']
        failed = self.stats['failed']
        skipped = self.stats['skipped']

        success_rate = (successful / total) * 100 if total > 0 else 0
        retry_success_rate = (self.stats['retry_success'] / failed) * 100 if failed > 0 else 0

        report = {
            'report_metadata': {
                'title': 'Failed Ticker Retry Collection Report',
                'generated_at': datetime.now().isoformat(),
                'collection_period': '2025-06-01 to 2025-09-01'
            },

            'summary_statistics': {
                'total_attempted': total,
                'successful_collections': successful,
                'failed_collections': failed,
                'skipped_collections': skipped,
                'success_rate': f"{success_rate:.1f}%",
                'retry_success_count': self.stats['retry_success'],
                'retry_success_rate': f"{retry_success_rate:.1f}%"
            },

            'failure_categories': dict(self.stats['categories']),

            'category_descriptions': {
                category: {
                    'count': count,
                    'percentage': (count / total) * 100 if total > 0 else 0,
                    'description': self.failure_categories.get(category, 'Unknown')
                }
                for category, count in self.stats['categories'].items()
            },

            'detailed_error_log': self.error_log[:100],  # First 100 errors

            'recommendations': self._generate_recommendations()
        }

        # Save report
        report_path = f"{self.error_log_path}/retry_collection_report_{timestamp}.json"
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)

        # Save full error log
        if self.error_log:
            error_log_path = f"{self.error_log_path}/detailed_errors_{timestamp}.json"
            with open(error_log_path, 'w') as f:
                json.dump(self.error_log, f, indent=2)

        # Print summary
        print("\n" + "="*80)
        print("RETRY COLLECTION COMPLETE")
        print("="*80)
        print(f"Total Attempted: {total}")
        print(f"Successful: {successful} ({success_rate:.1f}%)")
        print(f"Failed: {failed}")
        print(f"Skipped: {skipped}")

        if self.stats['retry_success'] > 0:
            print(f"Retry Success: {self.stats['retry_success']} "
                  f"({retry_success_rate:.1f}% of failures recovered)")

        print(f"\nFailure Categories:")
        for category, count in sorted(self.stats['categories'].items(),
                                    key=lambda x: x[1], reverse=True):
            if count > 0:
                pct = (count / total) * 100
                print(f"  {category}: {count} ({pct:.1f}%)")

        print(f"\nReports saved to: {self.error_log_path}")
        print("="*80)

        logger.info(f"Reports saved: {report_path}")

    def _generate_recommendations(self) -> List[str]:
        """Generate recommendations based on retry results"""
        recommendations = []

        categories = self.stats['categories']

        if categories.get('RATE_LIMIT', 0) > 10:
            recommendations.append("Implement more aggressive rate limiting or upgrade API tier")

        if categories.get('NO_DATA', 0) > 20:
            recommendations.append("Investigate tickers with no data - may be recently listed")

        if categories.get('INVALID_TICKER', 0) > 5:
            recommendations.append("Clean input data to remove invalid/delisted tickers")

        if categories.get('TIMEOUT', 0) > 10:
            recommendations.append("Increase timeout values or optimize request batching")

        if categories.get('BELOW_THRESHOLD', 0) > 0:
            recommendations.append(f"Consider adjusting market cap threshold (currently $2B)")

        if self.stats['successful'] > 0:
            recommendations.append(f"Successfully recovered {self.stats['successful']} tickers - "
                                 "update main collection to include these")

        return recommendations

async def main():
    """Main function to run retry collection"""

    # Load failed tickers from previous analysis
    analysis_file = "/workspaces/data/error_records/polygon_failures/no_api_analysis/failure_analysis_20250926_231953.json"

    try:
        with open(analysis_file, 'r') as f:
            analysis = json.load(f)

        # Get all failed tickers (including non-US since we now include them)
        all_failed_tickers = []

        for category_data in analysis['detailed_breakdown'].values():
            all_failed_tickers.extend(category_data['all_tickers'])

        logger.info(f"Loaded {len(all_failed_tickers)} failed tickers for retry")

        # Run retry collection
        collector = EnhancedPolygonCollector()
        await collector.retry_failed_collections(all_failed_tickers)

    except FileNotFoundError:
        logger.error(f"Analysis file not found: {analysis_file}")
        logger.info("Using sample failed tickers for testing...")

        # Test with a small sample
        sample_tickers = ['TEM', 'ATHS', 'AUR', 'BBUC', 'BULLW', 'STLA', 'ARM', 'NU']
        collector = EnhancedPolygonCollector()
        await collector.retry_failed_collections(sample_tickers)

    except Exception as e:
        logger.error(f"Error in retry collection: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())