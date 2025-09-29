#!/usr/bin/env python3
"""
Investigate Failed Tickers with Polygon API

Performs detailed investigation of all 480 failed tickers using Polygon API
to properly categorize failure reasons and generate actionable insights.
"""

import os
import json
import asyncio
import aiohttp
from datetime import datetime
from collections import defaultdict, Counter
from typing import Dict, List, Any, Optional
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv('/workspaces/data-collection-service/.env')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FailedTickerInvestigator:
    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv('POLYGON_API_KEY')
        if not self.api_key:
            raise ValueError("POLYGON_API_KEY environment variable is required")

        self.error_records_path = "/workspaces/data/error_records/polygon_failures"

        # Detailed failure categories
        self.failure_categories = {
            'DELISTED': 'Stock delisted or no longer trading',
            'NOT_FOUND': 'Ticker not found in Polygon database',
            'INACTIVE': 'Ticker exists but marked as inactive',
            'NO_HISTORICAL_DATA': 'No historical price data available for date range',
            'RECENTLY_LISTED': 'Recently listed with insufficient history',
            'OTC_MARKET': 'OTC or non-major exchange ticker',
            'CRYPTO_FOREX': 'Crypto or Forex ticker (not stock)',
            'INDEX_ETN': 'Index, ETN, or derivative product',
            'SUSPENDED': 'Trading suspended',
            'API_ERROR': 'API request failed or timeout',
            'RATE_LIMITED': 'API rate limit exceeded',
            'UNKNOWN': 'Unable to determine failure reason'
        }

    async def investigate_all_failures(self):
        """Investigate all failed tickers"""
        logger.info("Starting investigation of 480 failed tickers...")

        # Load failed tickers list
        failed_tickers = self._load_failed_tickers()
        logger.info(f"Loaded {len(failed_tickers)} failed tickers for investigation")

        # Investigate in batches
        investigation_results = await self._investigate_tickers_batch(failed_tickers)

        # Categorize and analyze
        categorized_results = self._categorize_results(investigation_results)

        # Generate reports
        await self._generate_investigation_reports(categorized_results)

        return categorized_results

    def _load_failed_tickers(self) -> List[str]:
        """Load list of failed tickers"""
        analysis_file = f"{self.error_records_path}/final_reports/complete_analysis_20250926_210449.json"

        try:
            with open(analysis_file, 'r') as f:
                analysis = json.load(f)

            unknown_failures = analysis['detailed_breakdown']['failed_collections']['categories']['UNKNOWN']['tickers']
            return [item['ticker'] for item in unknown_failures]

        except Exception as e:
            logger.error(f"Error loading failed tickers: {e}")
            return []

    async def _investigate_tickers_batch(self, tickers: List[str]) -> Dict[str, Dict[str, Any]]:
        """Investigate tickers in batches with rate limiting"""
        results = {}
        batch_size = 50
        delay_between_batches = 1.0  # 1 second between batches

        total_batches = (len(tickers) + batch_size - 1) // batch_size

        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i + batch_size]
            batch_num = (i // batch_size) + 1

            logger.info(f"Investigating batch {batch_num}/{total_batches} ({len(batch)} tickers)...")

            # Process batch
            batch_results = await self._investigate_batch(batch)
            results.update(batch_results)

            # Progress update
            completed = min(i + batch_size, len(tickers))
            logger.info(f"Progress: {completed}/{len(tickers)} tickers investigated ({(completed/len(tickers))*100:.1f}%)")

            # Rate limiting delay
            if i + batch_size < len(tickers):
                await asyncio.sleep(delay_between_batches)

        return results

    async def _investigate_batch(self, tickers: List[str]) -> Dict[str, Dict[str, Any]]:
        """Investigate a batch of tickers"""
        results = {}

        async with aiohttp.ClientSession() as session:
            tasks = [self._investigate_single_ticker(session, ticker) for ticker in tickers]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)

            for ticker, result in zip(tickers, batch_results):
                if isinstance(result, Exception):
                    results[ticker] = {
                        'ticker': ticker,
                        'category': 'API_ERROR',
                        'reason': f'Investigation error: {str(result)}',
                        'details': {},
                        'timestamp': datetime.now().isoformat()
                    }
                else:
                    results[ticker] = result

        return results

    async def _investigate_single_ticker(self, session: aiohttp.ClientSession, ticker: str) -> Dict[str, Any]:
        """Investigate a single ticker with Polygon API"""
        investigation = {
            'ticker': ticker,
            'category': 'UNKNOWN',
            'reason': 'Investigation incomplete',
            'details': {},
            'timestamp': datetime.now().isoformat()
        }

        try:
            # Step 1: Check if ticker exists
            ticker_info = await self._get_ticker_info(session, ticker)

            if not ticker_info:
                investigation['category'] = 'NOT_FOUND'
                investigation['reason'] = 'Ticker not found in Polygon database'
                return investigation

            investigation['details']['ticker_info'] = ticker_info

            # Step 2: Check ticker status
            if ticker_info.get('delisted_utc'):
                investigation['category'] = 'DELISTED'
                investigation['reason'] = f"Delisted on {ticker_info['delisted_utc']}"
                return investigation

            if not ticker_info.get('active', True):
                investigation['category'] = 'INACTIVE'
                investigation['reason'] = 'Ticker marked as inactive'
                return investigation

            # Step 3: Check market type
            market = ticker_info.get('market', '').lower()
            if market != 'stocks':
                if market == 'crypto':
                    investigation['category'] = 'CRYPTO_FOREX'
                    investigation['reason'] = f'Crypto ticker (market: {market})'
                elif market == 'fx':
                    investigation['category'] = 'CRYPTO_FOREX'
                    investigation['reason'] = f'Forex ticker (market: {market})'
                elif market == 'otc':
                    investigation['category'] = 'OTC_MARKET'
                    investigation['reason'] = f'OTC market ticker'
                else:
                    investigation['category'] = 'INDEX_ETN'
                    investigation['reason'] = f'Non-stock ticker (market: {market})'
                return investigation

            # Step 4: Check primary exchange
            primary_exchange = ticker_info.get('primary_exchange', '').upper()
            if primary_exchange in ['OTCM', 'OTC']:
                investigation['category'] = 'OTC_MARKET'
                investigation['reason'] = f'OTC exchange ticker ({primary_exchange})'
                return investigation

            # Step 5: Check historical data availability
            data_available = await self._check_historical_data(session, ticker)

            if not data_available:
                # Check listing date if available
                listing_date = ticker_info.get('listing_date', '')
                if listing_date and listing_date >= '2024-01-01':
                    investigation['category'] = 'RECENTLY_LISTED'
                    investigation['reason'] = f'Recently listed ({listing_date}) - insufficient history'
                else:
                    investigation['category'] = 'NO_HISTORICAL_DATA'
                    investigation['reason'] = 'No historical data available for collection period'
                return investigation

            # If we get here, it's unclear why collection failed
            investigation['category'] = 'UNKNOWN'
            investigation['reason'] = 'Ticker appears valid but collection failed - requires manual review'
            investigation['details']['investigation_note'] = 'Ticker exists, active, on major exchange, with historical data'

        except asyncio.TimeoutError:
            investigation['category'] = 'API_ERROR'
            investigation['reason'] = 'API request timeout'
        except aiohttp.ClientResponseError as e:
            if e.status == 429:
                investigation['category'] = 'RATE_LIMITED'
                investigation['reason'] = 'API rate limit exceeded during investigation'
            else:
                investigation['category'] = 'API_ERROR'
                investigation['reason'] = f'API error: {e.status}'
        except Exception as e:
            investigation['category'] = 'API_ERROR'
            investigation['reason'] = f'Investigation error: {str(e)}'

        return investigation

    async def _get_ticker_info(self, session: aiohttp.ClientSession, ticker: str) -> Optional[Dict[str, Any]]:
        """Get ticker information from Polygon API"""
        try:
            url = f"https://api.polygon.io/v3/reference/tickers/{ticker}"
            params = {'apikey': self.api_key}

            async with session.get(url, params=params, timeout=10) as response:
                if response.status == 404:
                    return None
                elif response.status != 200:
                    raise aiohttp.ClientResponseError(
                        request_info=response.request_info,
                        history=response.history,
                        status=response.status
                    )

                data = await response.json()
                return data.get('results')

        except Exception as e:
            logger.debug(f"Error getting ticker info for {ticker}: {e}")
            return None

    async def _check_historical_data(self, session: aiohttp.ClientSession, ticker: str) -> bool:
        """Check if historical data is available for ticker"""
        try:
            # Test with a date in the middle of our collection period
            test_date = "2025-07-15"
            url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{test_date}/{test_date}"
            params = {'apikey': self.api_key}

            async with session.get(url, params=params, timeout=10) as response:
                if response.status != 200:
                    return False

                data = await response.json()
                results = data.get('results', [])
                return len(results) > 0

        except Exception as e:
            logger.debug(f"Error checking historical data for {ticker}: {e}")
            return False

    def _categorize_results(self, investigation_results: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Categorize investigation results"""
        logger.info("Categorizing investigation results...")

        categories = defaultdict(list)
        total_failures = len(investigation_results)

        # Group by category
        for ticker, result in investigation_results.items():
            category = result['category']
            categories[category].append(result)

        # Calculate percentages
        category_summary = {}
        for category, failures in categories.items():
            count = len(failures)
            percentage = (count / total_failures) * 100 if total_failures > 0 else 0

            category_summary[category] = {
                'count': count,
                'percentage': percentage,
                'description': self.failure_categories.get(category, 'Unknown category'),
                'failures': failures
            }

        return {
            'investigation_metadata': {
                'timestamp': datetime.now().isoformat(),
                'total_investigated': total_failures,
                'api_key_used': bool(self.api_key),
                'investigation_complete': True
            },
            'summary': {
                'total_failures': total_failures,
                'categories_found': len(categories),
                'category_breakdown': {k: {'count': v['count'], 'percentage': v['percentage']}
                                     for k, v in category_summary.items()}
            },
            'detailed_categories': category_summary,
            'top_failure_reasons': dict(sorted(
                [(k, v['percentage']) for k, v in category_summary.items()],
                key=lambda x: x[1],
                reverse=True
            )[:5])
        }

    async def _generate_investigation_reports(self, categorized_results: Dict[str, Any]):
        """Generate comprehensive investigation reports"""
        logger.info("Generating investigation reports...")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        os.makedirs(f"{self.error_records_path}/investigation_results", exist_ok=True)

        # 1. Executive Summary
        exec_summary = {
            'report_type': 'Failure Investigation - Executive Summary',
            'generated_at': categorized_results['investigation_metadata']['timestamp'],
            'investigation_summary': categorized_results['summary'],
            'top_failure_reasons': categorized_results['top_failure_reasons'],
            'key_findings': self._generate_key_findings(categorized_results),
            'remediation_priorities': self._generate_remediation_priorities(categorized_results)
        }

        exec_path = f"{self.error_records_path}/investigation_results/executive_summary_{timestamp}.json"
        with open(exec_path, 'w') as f:
            json.dump(exec_summary, f, indent=2)

        # 2. Complete Investigation Results
        complete_path = f"{self.error_records_path}/investigation_results/complete_investigation_{timestamp}.json"
        with open(complete_path, 'w') as f:
            json.dump(categorized_results, f, indent=2)

        # 3. Category-specific reports
        for category, data in categorized_results['detailed_categories'].items():
            if data['count'] > 0:
                category_report = {
                    'category': category,
                    'description': data['description'],
                    'count': data['count'],
                    'percentage': data['percentage'],
                    'failed_tickers': [f['ticker'] for f in data['failures']],
                    'sample_details': data['failures'][:10],  # First 10
                    'recommended_actions': self._get_category_actions(category)
                }

                safe_category = category.lower().replace('_', '-')
                category_path = f"{self.error_records_path}/investigation_results/category_{safe_category}_{timestamp}.json"
                with open(category_path, 'w') as f:
                    json.dump(category_report, f, indent=2)

        logger.info(f"Investigation reports saved:")
        logger.info(f"  Executive Summary: {exec_path}")
        logger.info(f"  Complete Results: {complete_path}")
        logger.info(f"  Category Reports: {len(categorized_results['detailed_categories'])} files")

    def _generate_key_findings(self, categorized_results: Dict[str, Any]) -> List[str]:
        """Generate key findings from investigation"""
        findings = []

        total = categorized_results['summary']['total_failures']
        findings.append(f"Investigated all {total} collection failures with Polygon API")

        # Top failure reason
        if categorized_results['top_failure_reasons']:
            top_category, top_percentage = list(categorized_results['top_failure_reasons'].items())[0]
            count = categorized_results['detailed_categories'][top_category]['count']
            findings.append(f"Primary failure reason: {top_category} ({count} failures, {top_percentage:.1f}%)")

        # Actionable vs non-actionable
        non_actionable = ['DELISTED', 'OTC_MARKET', 'CRYPTO_FOREX', 'INDEX_ETN']
        non_actionable_count = sum(
            categorized_results['detailed_categories'][cat]['count']
            for cat in non_actionable
            if cat in categorized_results['detailed_categories']
        )

        if non_actionable_count > 0:
            findings.append(f"{non_actionable_count} failures are non-actionable (delisted, OTC, or non-stock tickers)")

        actionable_count = total - non_actionable_count
        if actionable_count > 0:
            findings.append(f"{actionable_count} failures are actionable and may be recoverable")

        return findings

    def _generate_remediation_priorities(self, categorized_results: Dict[str, Any]) -> Dict[str, List[str]]:
        """Generate remediation priorities"""
        priorities = {
            'high_priority': [],
            'medium_priority': [],
            'low_priority': []
        }

        categories = categorized_results['detailed_categories']

        # High priority: Potentially recoverable failures
        for category in ['NO_HISTORICAL_DATA', 'RECENTLY_LISTED', 'UNKNOWN', 'API_ERROR']:
            if category in categories and categories[category]['count'] > 0:
                count = categories[category]['count']
                priorities['high_priority'].append(
                    f"Investigate {count} {category} failures - may be recoverable"
                )

        # Medium priority: Process improvements
        for category in ['RATE_LIMITED', 'SUSPENDED']:
            if category in categories and categories[category]['count'] > 0:
                count = categories[category]['count']
                priorities['medium_priority'].append(
                    f"Review {count} {category} failures - process improvements needed"
                )

        # Low priority: Expected failures
        for category in ['DELISTED', 'OTC_MARKET', 'CRYPTO_FOREX', 'INDEX_ETN', 'NOT_FOUND', 'INACTIVE']:
            if category in categories and categories[category]['count'] > 0:
                count = categories[category]['count']
                priorities['low_priority'].append(
                    f"Filter {count} {category} tickers from input data - expected exclusions"
                )

        return priorities

    def _get_category_actions(self, category: str) -> List[str]:
        """Get recommended actions for category"""
        actions = {
            'DELISTED': [
                'Remove from ticker universe',
                'Add to exclusion list',
                'No further action required'
            ],
            'NOT_FOUND': [
                'Verify ticker symbols are correct',
                'Check for ticker symbol changes',
                'Remove invalid tickers from input data'
            ],
            'INACTIVE': [
                'Remove from active ticker list',
                'Check if temporarily suspended or permanently inactive',
                'Update ticker filtering logic'
            ],
            'NO_HISTORICAL_DATA': [
                'Retry with different date ranges',
                'Check data availability on Polygon',
                'Consider alternative data sources'
            ],
            'RECENTLY_LISTED': [
                'Flag for future collection when sufficient history exists',
                'Reduce historical lookback requirements',
                'Add to monitoring list for quarterly retry'
            ],
            'OTC_MARKET': [
                'Filter OTC tickers from input data',
                'Update collection criteria to exclude OTC',
                'No action needed - expected exclusion'
            ],
            'CRYPTO_FOREX': [
                'Remove crypto/forex tickers from stock universe',
                'Update input data filtering',
                'No action needed - wrong asset class'
            ],
            'INDEX_ETN': [
                'Filter index/ETN products from input data',
                'Update ticker classification logic',
                'Expected exclusion - no action required'
            ],
            'SUSPENDED': [
                'Monitor for trading resumption',
                'Add to watchlist for retry',
                'Check suspension reason and duration'
            ],
            'API_ERROR': [
                'Retry with enhanced error handling',
                'Check API connectivity and authentication',
                'Review rate limiting strategy'
            ],
            'RATE_LIMITED': [
                'Implement better rate limiting',
                'Add delays between batch requests',
                'Consider API tier upgrade if persistent'
            ],
            'UNKNOWN': [
                'Manual investigation required',
                'Review individual ticker details',
                'Check collection logs for specific errors'
            ]
        }

        return actions.get(category, ['Review and investigate manually'])


async def main():
    """Main function"""
    investigator = FailedTickerInvestigator()

    try:
        results = await investigator.investigate_all_failures()

        print("\n" + "="*80)
        print("FAILURE INVESTIGATION COMPLETE")
        print("="*80)

        summary = results['summary']
        print(f"Total Failures Investigated: {summary['total_failures']}")
        print(f"Failure Categories Found: {summary['categories_found']}")

        print(f"\nTop Failure Categories:")
        for category, percentage in list(results['top_failure_reasons'].items())[:5]:
            count = results['detailed_categories'][category]['count']
            print(f"  {category}: {count} failures ({percentage:.1f}%)")

        print(f"\nReports saved to: /workspaces/data/error_records/polygon_failures/investigation_results/")
        print("="*80)

    except Exception as e:
        logger.error(f"Investigation failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())