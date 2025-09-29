#!/usr/bin/env python3
"""
Polygon.io Failure Analysis Tool

Analyzes collection failures from Polygon.io data collection and categorizes them
for tracking, investigation, and remediation.
"""

import os
import json
import asyncio
import aiohttp
from datetime import datetime, timedelta
from collections import defaultdict, Counter
from typing import Dict, List, Any, Optional
import logging
import argparse
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PolygonFailureAnalyzer:
    def __init__(self, polygon_api_key: str = None):
        self.polygon_api_key = polygon_api_key or os.getenv('POLYGON_API_KEY')
        self.base_path = "/workspaces/data/raw_data/polygon"
        self.error_records_path = "/workspaces/data/error_records/polygon_failures"

        # Failure categories
        self.failure_categories = {
            'NO_DATA': 'No data available for date range',
            'API_ERROR': 'API request failed (4xx/5xx errors)',
            'RATE_LIMIT': 'Rate limit exceeded',
            'TICKER_NOT_FOUND': 'Ticker symbol not recognized',
            'DELISTED': 'Stock delisted or no longer trading',
            'INSUFFICIENT_HISTORY': 'Insufficient historical data',
            'DATA_QUALITY': 'Data quality issues (missing OHLCV)',
            'NETWORK_ERROR': 'Network connectivity issues',
            'TIMEOUT': 'Request timeout',
            'UNKNOWN': 'Unknown/unclassified error'
        }

        # Create error records directory
        os.makedirs(self.error_records_path, exist_ok=True)

    async def analyze_failures(self, start_date: str = "2025-06-01", end_date: str = "2025-09-01"):
        """Analyze all failures from the collection period"""
        logger.info(f"Starting failure analysis for {start_date} to {end_date}")

        # Load collection summary and filtered tickers
        collection_summary = self._load_collection_summary()
        filtered_tickers = self._load_filtered_tickers()

        # Get expected vs actual tickers
        failed_tickers = await self._identify_failed_tickers(start_date, end_date)

        # Categorize failures
        failure_analysis = await self._categorize_failures(failed_tickers, start_date, end_date)

        # Generate reports
        await self._generate_failure_reports(failure_analysis, collection_summary, filtered_tickers)

        return failure_analysis

    def _load_collection_summary(self) -> Dict[str, Any]:
        """Load collection summary from polygon data"""
        summary_path = f"{self.base_path}/collection_summary.json"
        try:
            with open(summary_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning(f"Collection summary not found at {summary_path}")
            return {}

    def _load_filtered_tickers(self) -> Dict[str, Any]:
        """Load filtered tickers data"""
        filtered_path = f"{self.base_path}/filtered_tickers.json"
        try:
            with open(filtered_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning(f"Filtered tickers not found at {filtered_path}")
            return {}

    async def _identify_failed_tickers(self, start_date: str, end_date: str) -> List[str]:
        """Identify tickers that failed collection"""
        logger.info("Identifying failed tickers...")

        # Get all tickers that should have been collected
        input_source_path = "/workspaces/data/input_source"
        expected_tickers = set()

        # Find latest enriched yfinance file
        yfinance_files = [f for f in os.listdir(input_source_path) if f.startswith("enriched_yfinance_")]
        if yfinance_files:
            latest_file = sorted(yfinance_files)[-1]
            with open(f"{input_source_path}/{latest_file}", 'r') as f:
                yfinance_data = json.load(f)
                # Handle both dict and list formats
                if isinstance(yfinance_data, dict):
                    expected_tickers = set(yfinance_data.keys())
                elif isinstance(yfinance_data, list):
                    expected_tickers = {item['ticker'] for item in yfinance_data if 'ticker' in item}
                else:
                    logger.warning(f"Unexpected data format in {latest_file}")
                    expected_tickers = set()

        # Get tickers that actually have data
        collected_tickers = set()
        if os.path.exists(self.base_path):
            collected_tickers = {d for d in os.listdir(self.base_path)
                               if os.path.isdir(os.path.join(self.base_path, d))
                               and not d.startswith('.')}

        # Failed tickers = expected - collected
        failed_tickers = list(expected_tickers - collected_tickers)
        logger.info(f"Identified {len(failed_tickers)} failed tickers")

        return failed_tickers

    async def _categorize_failures(self, failed_tickers: List[str], start_date: str, end_date: str) -> Dict[str, Any]:
        """Categorize failures by reason"""
        logger.info(f"Categorizing {len(failed_tickers)} failures...")

        failure_data = {
            'analysis_date': datetime.now().isoformat(),
            'period': f"{start_date} to {end_date}",
            'total_failures': len(failed_tickers),
            'categories': defaultdict(list),
            'category_counts': defaultdict(int),
            'ticker_details': {}
        }

        # Analyze each failed ticker
        batch_size = 50
        for i in range(0, len(failed_tickers), batch_size):
            batch = failed_tickers[i:i + batch_size]
            await self._analyze_ticker_batch(batch, failure_data)

            # Progress update
            completed = min(i + batch_size, len(failed_tickers))
            logger.info(f"Analyzed {completed}/{len(failed_tickers)} failed tickers")

        # Calculate percentages
        total_failures = failure_data['total_failures']
        failure_data['category_percentages'] = {
            category: (count / total_failures) * 100
            for category, count in failure_data['category_counts'].items()
        }

        return failure_data

    async def _analyze_ticker_batch(self, tickers: List[str], failure_data: Dict[str, Any]):
        """Analyze a batch of failed tickers"""
        if not self.polygon_api_key:
            # Without API key, classify as UNKNOWN
            for ticker in tickers:
                category = 'UNKNOWN'
                failure_data['categories'][category].append(ticker)
                failure_data['category_counts'][category] += 1
                failure_data['ticker_details'][ticker] = {
                    'category': category,
                    'reason': 'API key not available for detailed analysis',
                    'timestamp': datetime.now().isoformat()
                }
            return

        # Analyze with API calls
        async with aiohttp.ClientSession() as session:
            tasks = [self._analyze_single_ticker(session, ticker) for ticker in tickers]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for ticker, result in zip(tickers, results):
                if isinstance(result, Exception):
                    category = 'NETWORK_ERROR'
                    reason = f"Analysis failed: {str(result)}"
                else:
                    category, reason = result

                failure_data['categories'][category].append(ticker)
                failure_data['category_counts'][category] += 1
                failure_data['ticker_details'][ticker] = {
                    'category': category,
                    'reason': reason,
                    'timestamp': datetime.now().isoformat()
                }

    async def _analyze_single_ticker(self, session: aiohttp.ClientSession, ticker: str) -> tuple:
        """Analyze a single ticker failure"""
        try:
            # Test if ticker exists
            url = f"https://api.polygon.io/v3/reference/tickers/{ticker}"
            params = {'apikey': self.polygon_api_key}

            async with session.get(url, params=params) as response:
                if response.status == 404:
                    return 'TICKER_NOT_FOUND', f"Ticker {ticker} not found in Polygon API"
                elif response.status == 429:
                    return 'RATE_LIMIT', "Rate limit exceeded during analysis"
                elif response.status >= 400:
                    return 'API_ERROR', f"API error {response.status}"

                data = await response.json()

                # Check if delisted
                if 'results' in data:
                    ticker_info = data['results']
                    if ticker_info.get('delisted_utc'):
                        return 'DELISTED', f"Stock delisted on {ticker_info['delisted_utc']}"

                    # Check market status
                    if ticker_info.get('market') != 'stocks':
                        return 'TICKER_NOT_FOUND', f"Not a stock ticker (market: {ticker_info.get('market')})"

                # Test historical data availability
                test_date = "2025-08-15"  # Mid-period test
                agg_url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{test_date}/{test_date}"

                async with session.get(agg_url, params=params) as agg_response:
                    if agg_response.status == 404:
                        return 'NO_DATA', f"No historical data available for {test_date}"
                    elif agg_response.status == 429:
                        return 'RATE_LIMIT', "Rate limit exceeded during data test"
                    elif agg_response.status >= 400:
                        return 'API_ERROR', f"Data API error {agg_response.status}"

                    agg_data = await agg_response.json()
                    if not agg_data.get('results') or len(agg_data['results']) == 0:
                        return 'INSUFFICIENT_HISTORY', f"No OHLCV data for test date {test_date}"

                # If we get here, it's likely a collection issue
                return 'UNKNOWN', "Ticker appears valid but collection failed"

        except asyncio.TimeoutError:
            return 'TIMEOUT', "Request timeout during analysis"
        except Exception as e:
            return 'NETWORK_ERROR', f"Network error: {str(e)}"

    async def _generate_failure_reports(self, failure_analysis: Dict[str, Any],
                                      collection_summary: Dict[str, Any],
                                      filtered_tickers: Dict[str, Any]):
        """Generate comprehensive failure reports"""
        logger.info("Generating failure reports...")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # 1. Executive Summary Report
        await self._generate_executive_summary(failure_analysis, collection_summary,
                                             filtered_tickers, timestamp)

        # 2. Detailed Category Reports
        await self._generate_category_reports(failure_analysis, timestamp)

        # 3. Actionable Recommendations
        await self._generate_remediation_plan(failure_analysis, timestamp)

        # 4. Ticker-by-Ticker Details
        await self._generate_detailed_ticker_report(failure_analysis, timestamp)

        logger.info(f"All reports generated in {self.error_records_path}")

    async def _generate_executive_summary(self, failure_analysis: Dict[str, Any],
                                        collection_summary: Dict[str, Any],
                                        filtered_tickers: Dict[str, Any],
                                        timestamp: str):
        """Generate executive summary report"""

        total_tickers = collection_summary.get('total_tickers', 0)
        successful = collection_summary.get('successful_collections', 0)
        failed = collection_summary.get('failed_collections', 0)
        filtered = collection_summary.get('filtered_collections', 0)

        summary = {
            "report_type": "Executive Summary - Polygon.io Collection Failures",
            "generated_at": datetime.now().isoformat(),
            "period": failure_analysis['period'],

            "collection_overview": {
                "total_tickers_processed": total_tickers,
                "successful_collections": successful,
                "failed_collections": failed,
                "filtered_collections": filtered,
                "filtered_reason": filtered_tickers.get('filter_reason', 'Unknown')
            },

            "success_metrics": {
                "overall_success_rate": f"{(successful/total_tickers)*100:.1f}%" if total_tickers > 0 else "N/A",
                "relevant_success_rate": f"{(successful/(total_tickers-filtered))*100:.1f}%" if (total_tickers-filtered) > 0 else "N/A",
                "failure_rate": f"{(failed/total_tickers)*100:.1f}%" if total_tickers > 0 else "N/A"
            },

            "failure_breakdown": {
                "total_failures_analyzed": failure_analysis['total_failures'],
                "categories": failure_analysis['category_percentages']
            },

            "top_failure_reasons": dict(sorted(failure_analysis['category_percentages'].items(),
                                             key=lambda x: x[1], reverse=True)[:5]),

            "key_insights": self._generate_key_insights(failure_analysis, collection_summary)
        }

        report_path = f"{self.error_records_path}/executive_summary_{timestamp}.json"
        with open(report_path, 'w') as f:
            json.dump(summary, f, indent=2)

        logger.info(f"Executive summary saved to {report_path}")

    async def _generate_category_reports(self, failure_analysis: Dict[str, Any], timestamp: str):
        """Generate detailed reports for each failure category"""

        for category, tickers in failure_analysis['categories'].items():
            if not tickers:
                continue

            category_report = {
                "category": category,
                "description": self.failure_categories.get(category, "Unknown category"),
                "total_failures": len(tickers),
                "percentage_of_failures": failure_analysis['category_percentages'].get(category, 0),
                "failed_tickers": sorted(tickers),
                "sample_details": [],
                "recommended_actions": self._get_category_recommendations(category)
            }

            # Add sample ticker details (first 10)
            for ticker in tickers[:10]:
                if ticker in failure_analysis['ticker_details']:
                    details = failure_analysis['ticker_details'][ticker]
                    category_report['sample_details'].append({
                        "ticker": ticker,
                        "reason": details['reason'],
                        "timestamp": details['timestamp']
                    })

            # Save category report
            safe_category = category.lower().replace(' ', '_')
            report_path = f"{self.error_records_path}/category_{safe_category}_{timestamp}.json"
            with open(report_path, 'w') as f:
                json.dump(category_report, f, indent=2)

        logger.info(f"Category reports generated for {len(failure_analysis['categories'])} categories")

    async def _generate_remediation_plan(self, failure_analysis: Dict[str, Any], timestamp: str):
        """Generate actionable remediation plan"""

        plan = {
            "remediation_plan": "Polygon.io Collection Failures - Action Plan",
            "generated_at": datetime.now().isoformat(),
            "priority_actions": [],
            "category_specific_actions": {},
            "implementation_timeline": {},
            "success_metrics": {
                "target_success_rate": "85%",
                "target_reduction_in_failures": "50%",
                "timeline": "30 days"
            }
        }

        # Priority actions based on failure percentages
        sorted_categories = sorted(failure_analysis['category_percentages'].items(),
                                 key=lambda x: x[1], reverse=True)

        for category, percentage in sorted_categories[:3]:  # Top 3 categories
            if percentage > 5:  # Only significant categories
                plan['priority_actions'].append({
                    "category": category,
                    "impact": f"{percentage:.1f}% of failures",
                    "action": self._get_priority_action(category),
                    "timeline": self._get_action_timeline(category)
                })

        # Detailed actions per category
        for category, tickers in failure_analysis['categories'].items():
            if tickers:
                plan['category_specific_actions'][category] = {
                    "affected_tickers": len(tickers),
                    "recommended_actions": self._get_category_recommendations(category),
                    "implementation_steps": self._get_implementation_steps(category)
                }

        report_path = f"{self.error_records_path}/remediation_plan_{timestamp}.json"
        with open(report_path, 'w') as f:
            json.dump(plan, f, indent=2)

        logger.info(f"Remediation plan saved to {report_path}")

    async def _generate_detailed_ticker_report(self, failure_analysis: Dict[str, Any], timestamp: str):
        """Generate detailed ticker-by-ticker failure report"""

        detailed_report = {
            "report_type": "Detailed Ticker Failure Analysis",
            "generated_at": datetime.now().isoformat(),
            "total_failed_tickers": len(failure_analysis['ticker_details']),
            "ticker_failures": failure_analysis['ticker_details']
        }

        # Sort by category for easier review
        sorted_details = dict(sorted(failure_analysis['ticker_details'].items(),
                                   key=lambda x: x[1]['category']))
        detailed_report['ticker_failures'] = sorted_details

        report_path = f"{self.error_records_path}/detailed_ticker_failures_{timestamp}.json"
        with open(report_path, 'w') as f:
            json.dump(detailed_report, f, indent=2)

        logger.info(f"Detailed ticker report saved to {report_path}")

    def _generate_key_insights(self, failure_analysis: Dict[str, Any],
                             collection_summary: Dict[str, Any]) -> List[str]:
        """Generate key insights from the failure analysis"""
        insights = []

        total_failures = failure_analysis['total_failures']
        if total_failures == 0:
            return ["No failures detected - excellent collection performance"]

        # Top failure category
        top_category = max(failure_analysis['category_percentages'].items(), key=lambda x: x[1])
        insights.append(f"Primary failure mode: {top_category[0]} ({top_category[1]:.1f}% of failures)")

        # Coverage insights
        total_processed = collection_summary.get('total_tickers', 0)
        successful = collection_summary.get('successful_collections', 0)
        if total_processed > 0:
            coverage = (successful / total_processed) * 100
            if coverage > 70:
                insights.append(f"Good market coverage achieved: {coverage:.1f}% success rate")
            else:
                insights.append(f"Low market coverage: {coverage:.1f}% success rate needs improvement")

        # Actionable insights
        if 'DELISTED' in failure_analysis['category_percentages']:
            delisted_pct = failure_analysis['category_percentages']['DELISTED']
            if delisted_pct > 10:
                insights.append("High delisted stock rate suggests ticker list needs updating")

        if 'RATE_LIMIT' in failure_analysis['category_percentages']:
            rate_limit_pct = failure_analysis['category_percentages']['RATE_LIMIT']
            if rate_limit_pct > 5:
                insights.append("Rate limiting issues detected - consider request throttling")

        return insights

    def _get_category_recommendations(self, category: str) -> List[str]:
        """Get recommendations for specific failure category"""
        recommendations = {
            'NO_DATA': [
                "Verify ticker symbols are correct and active",
                "Check if data is available for the requested date range",
                "Consider alternative data sources for missing tickers"
            ],
            'API_ERROR': [
                "Review API key permissions and quotas",
                "Implement exponential backoff for retries",
                "Monitor API status and service announcements"
            ],
            'RATE_LIMIT': [
                "Implement request throttling (max 5 requests/minute for free tier)",
                "Consider upgrading to higher tier API plan",
                "Add delays between batch requests"
            ],
            'TICKER_NOT_FOUND': [
                "Validate ticker symbols against current market listings",
                "Update ticker list to remove invalid symbols",
                "Cross-reference with multiple data sources"
            ],
            'DELISTED': [
                "Filter out delisted stocks from collection list",
                "Implement delisting date checks",
                "Update ticker universe regularly"
            ],
            'INSUFFICIENT_HISTORY': [
                "Adjust historical lookback period requirements",
                "Handle newly listed stocks separately",
                "Consider shorter history for recent IPOs"
            ],
            'DATA_QUALITY': [
                "Implement data validation checks",
                "Handle missing OHLCV data gracefully",
                "Flag incomplete data for manual review"
            ],
            'NETWORK_ERROR': [
                "Implement retry logic for transient failures",
                "Add connection timeout handling",
                "Monitor network connectivity"
            ],
            'TIMEOUT': [
                "Increase request timeout values",
                "Implement asynchronous processing",
                "Break large requests into smaller batches"
            ],
            'UNKNOWN': [
                "Enable detailed error logging",
                "Implement comprehensive exception handling",
                "Review API documentation for edge cases"
            ]
        }
        return recommendations.get(category, ["Review and investigate manually"])

    def _get_priority_action(self, category: str) -> str:
        """Get priority action for category"""
        actions = {
            'NO_DATA': "Update ticker validation process",
            'API_ERROR': "Review API configuration and error handling",
            'RATE_LIMIT': "Implement proper request throttling",
            'TICKER_NOT_FOUND': "Clean up ticker list",
            'DELISTED': "Filter delisted stocks",
            'INSUFFICIENT_HISTORY': "Adjust history requirements",
            'DATA_QUALITY': "Enhance data validation",
            'NETWORK_ERROR': "Improve error handling",
            'TIMEOUT': "Optimize request timing",
            'UNKNOWN': "Enhance error logging"
        }
        return actions.get(category, "Investigate and resolve")

    def _get_action_timeline(self, category: str) -> str:
        """Get implementation timeline for category"""
        timelines = {
            'NO_DATA': "1 week",
            'API_ERROR': "3 days",
            'RATE_LIMIT': "1 day",
            'TICKER_NOT_FOUND': "2 days",
            'DELISTED': "1 day",
            'INSUFFICIENT_HISTORY': "2 days",
            'DATA_QUALITY': "1 week",
            'NETWORK_ERROR': "3 days",
            'TIMEOUT': "1 day",
            'UNKNOWN': "1 week"
        }
        return timelines.get(category, "1 week")

    def _get_implementation_steps(self, category: str) -> List[str]:
        """Get implementation steps for category"""
        steps = {
            'RATE_LIMIT': [
                "1. Add request delay configuration",
                "2. Implement rate limiting decorator",
                "3. Test with smaller batches",
                "4. Monitor success rate improvement"
            ],
            'TICKER_NOT_FOUND': [
                "1. Export failed ticker list",
                "2. Cross-reference with current market data",
                "3. Remove invalid tickers from input data",
                "4. Re-run collection for remaining tickers"
            ],
            'DELISTED': [
                "1. Query delisting dates from API",
                "2. Add delisting filter to collection logic",
                "3. Update ticker universe monthly",
                "4. Document delisted tickers for reference"
            ]
        }
        return steps.get(category, ["1. Analyze root cause", "2. Implement fix", "3. Test solution"])


async def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Analyze Polygon.io collection failures')
    parser.add_argument('--start-date', default='2025-06-01',
                       help='Start date for analysis (YYYY-MM-DD)')
    parser.add_argument('--end-date', default='2025-09-01',
                       help='End date for analysis (YYYY-MM-DD)')
    parser.add_argument('--api-key', help='Polygon.io API key for detailed analysis')

    args = parser.parse_args()

    analyzer = PolygonFailureAnalyzer(args.api_key)

    try:
        await analyzer.analyze_failures(args.start_date, args.end_date)
        logger.info("Failure analysis completed successfully")
    except Exception as e:
        logger.error(f"Failure analysis failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())