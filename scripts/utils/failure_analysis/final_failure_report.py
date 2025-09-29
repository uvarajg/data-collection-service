#!/usr/bin/env python3
"""
Final Comprehensive Failure Report Generator

Creates detailed failure reports with categorization, percentage breakdown,
and actionable remediation plans for Polygon.io collection failures.
"""

import os
import json
from datetime import datetime
from collections import defaultdict, Counter
from typing import Dict, List, Any
import logging
import asyncio
import aiohttp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FinalFailureReportGenerator:
    def __init__(self):
        self.base_path = "/workspaces/data/raw_data/polygon"
        self.error_records_path = "/workspaces/data/error_records/polygon_failures"
        self.polygon_api_key = os.getenv('POLYGON_API_KEY')

        os.makedirs(f"{self.error_records_path}/final_reports", exist_ok=True)

    async def generate_comprehensive_report(self):
        """Generate the final comprehensive failure report"""
        logger.info("Generating comprehensive failure analysis report...")

        # Load data
        input_data = self._load_input_data()
        collection_summary = self._load_collection_summary()
        filtered_data = self._load_filtered_data()

        # Analyze collections
        analysis = await self._comprehensive_failure_analysis(input_data, filtered_data)

        # Generate final report
        final_report = await self._create_final_report(analysis, collection_summary, filtered_data)

        # Save reports
        await self._save_final_reports(final_report)

        return final_report

    def _load_input_data(self) -> List[Dict[str, Any]]:
        """Load YFinance input data"""
        input_source_path = "/workspaces/data/input_source"
        try:
            yfinance_files = [f for f in os.listdir(input_source_path)
                            if f.startswith("enriched_yfinance_")]
            if not yfinance_files:
                return []

            latest_file = sorted(yfinance_files)[-1]
            logger.info(f"Loading input data from {latest_file}")
            with open(f"{input_source_path}/{latest_file}", 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Could not load input data: {e}")
            return []

    def _load_collection_summary(self) -> Dict[str, Any]:
        """Load Polygon collection summary"""
        summary_path = f"{self.base_path}/collection_summary.json"
        try:
            with open(summary_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning("Collection summary not found")
            return {}

    def _load_filtered_data(self) -> Dict[str, Any]:
        """Load filtered tickers data"""
        filtered_path = f"{self.base_path}/filtered_tickers.json"
        try:
            with open(filtered_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning("Filtered tickers not found")
            return {}

    async def _comprehensive_failure_analysis(self, input_data: List[Dict[str, Any]],
                                           filtered_data: Dict[str, Any]) -> Dict[str, Any]:
        """Perform comprehensive failure analysis"""
        logger.info("Performing comprehensive failure analysis...")

        # Create lookups
        input_lookup = {item['ticker']: item for item in input_data}
        filtered_tickers = set(filtered_data.get('filtered_tickers', []))

        # Scan actual collections
        collected_tickers = self._scan_collected_tickers()

        # Determine failure categories
        expected_tickers = set(input_lookup.keys())
        successfully_collected = set(collected_tickers.keys())

        # True failures (excluding filtered)
        actual_failures = expected_tickers - successfully_collected - filtered_tickers

        logger.info(f"Analysis: {len(expected_tickers)} expected, {len(successfully_collected)} collected, "
                   f"{len(filtered_tickers)} filtered, {len(actual_failures)} failed")

        # Categorize failures
        failure_categories = await self._categorize_failures(actual_failures, input_lookup)

        return {
            'total_expected': len(expected_tickers),
            'successfully_collected': len(successfully_collected),
            'filtered_tickers': len(filtered_tickers),
            'actual_failures': len(actual_failures),
            'true_success_rate': (len(successfully_collected) / (len(expected_tickers) - len(filtered_tickers))) * 100
                                if (len(expected_tickers) - len(filtered_tickers)) > 0 else 0,
            'overall_success_rate': (len(successfully_collected) / len(expected_tickers)) * 100
                                  if len(expected_tickers) > 0 else 0,
            'failure_categories': failure_categories,
            'collected_stats': collected_tickers,
            'filtered_details': {
                'count': len(filtered_tickers),
                'reason': filtered_data.get('filter_reason', 'Unknown'),
                'tickers': list(filtered_tickers)
            }
        }

    def _scan_collected_tickers(self) -> Dict[str, Dict[str, Any]]:
        """Scan and analyze collected ticker data"""
        collected = {}

        if not os.path.exists(self.base_path):
            return collected

        for ticker_dir in os.listdir(self.base_path):
            ticker_path = os.path.join(self.base_path, ticker_dir)
            if not os.path.isdir(ticker_path) or ticker_dir.startswith('.'):
                continue

            # Skip non-ticker directories
            if ticker_dir in ['collection_summary.json', 'filtered_tickers.json']:
                continue

            stats = {
                'ticker': ticker_dir,
                'total_files': 0,
                'months': [],
                'date_range': []
            }

            # Scan 2025 directory
            year_path = os.path.join(ticker_path, '2025')
            if os.path.exists(year_path):
                for month in os.listdir(year_path):
                    month_path = os.path.join(year_path, month)
                    if os.path.isdir(month_path):
                        files = [f for f in os.listdir(month_path) if f.endswith('.json')]
                        if files:
                            stats['months'].append(month)
                            stats['total_files'] += len(files)
                            dates = [f.replace('.json', '') for f in files]
                            stats['date_range'].extend(dates)

            if stats['total_files'] > 0:
                stats['date_range'].sort()
                stats['months'].sort()
                collected[ticker_dir] = stats

        return collected

    async def _categorize_failures(self, failed_tickers: set, input_lookup: Dict[str, Any]) -> Dict[str, Any]:
        """Categorize failures with detailed analysis"""
        logger.info(f"Categorizing {len(failed_tickers)} failures...")

        categories = {
            'DELISTED_STOCKS': {'tickers': [], 'description': 'Stocks delisted or no longer trading'},
            'LOW_MARKET_CAP': {'tickers': [], 'description': 'Market cap too low for collection criteria'},
            'RECENT_IPOS': {'tickers': [], 'description': 'Recently IPO\'d stocks with insufficient history'},
            'API_ISSUES': {'tickers': [], 'description': 'API-related collection failures'},
            'DATA_QUALITY': {'tickers': [], 'description': 'Data quality issues preventing collection'},
            'UNKNOWN': {'tickers': [], 'description': 'Requires manual investigation'}
        }

        # Analyze each failed ticker
        for ticker in failed_tickers:
            ticker_info = input_lookup.get(ticker, {})
            market_cap = ticker_info.get('market_cap', 0)

            # Categorization logic
            if market_cap < 1000000000:  # < $1B (very small cap)
                categories['LOW_MARKET_CAP']['tickers'].append({
                    'ticker': ticker,
                    'market_cap': market_cap,
                    'reason': f'Market cap ${market_cap:,.0f} below $1B'
                })
            elif not ticker_info or market_cap == 0:
                categories['DATA_QUALITY']['tickers'].append({
                    'ticker': ticker,
                    'market_cap': market_cap,
                    'reason': 'No market data available'
                })
            else:
                # Check with API if available
                if self.polygon_api_key:
                    api_result = await self._check_ticker_api_status(ticker)
                    if 'delisted' in api_result.lower():
                        categories['DELISTED_STOCKS']['tickers'].append({
                            'ticker': ticker,
                            'market_cap': market_cap,
                            'reason': api_result
                        })
                    elif 'not found' in api_result.lower():
                        categories['API_ISSUES']['tickers'].append({
                            'ticker': ticker,
                            'market_cap': market_cap,
                            'reason': api_result
                        })
                    else:
                        categories['UNKNOWN']['tickers'].append({
                            'ticker': ticker,
                            'market_cap': market_cap,
                            'reason': f'API check: {api_result}'
                        })
                else:
                    categories['UNKNOWN']['tickers'].append({
                        'ticker': ticker,
                        'market_cap': market_cap,
                        'reason': 'Requires API investigation'
                    })

        # Calculate percentages
        total_failures = len(failed_tickers)
        for category, data in categories.items():
            count = len(data['tickers'])
            data['count'] = count
            data['percentage'] = (count / total_failures) * 100 if total_failures > 0 else 0

        return categories

    async def _check_ticker_api_status(self, ticker: str) -> str:
        """Check ticker status with Polygon API"""
        try:
            async with aiohttp.ClientSession() as session:
                url = f"https://api.polygon.io/v3/reference/tickers/{ticker}"
                params = {'apikey': self.polygon_api_key}

                async with session.get(url, params=params, timeout=5) as response:
                    if response.status == 404:
                        return "Ticker not found"
                    elif response.status == 429:
                        return "Rate limited"
                    elif response.status != 200:
                        return f"API error {response.status}"

                    data = await response.json()
                    if 'results' in data:
                        info = data['results']
                        if info.get('delisted_utc'):
                            return f"Delisted on {info['delisted_utc']}"
                        if not info.get('active', True):
                            return "Inactive ticker"
                        return "Active ticker"

                    return "No ticker info available"

        except Exception as e:
            return f"API check failed: {str(e)}"

    async def _create_final_report(self, analysis: Dict[str, Any],
                                 collection_summary: Dict[str, Any],
                                 filtered_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create the final comprehensive report"""

        report = {
            'report_metadata': {
                'title': 'Comprehensive Polygon.io Collection Failure Analysis',
                'generated_at': datetime.now().isoformat(),
                'analysis_period': '2025-06-01 to 2025-09-01',
                'report_version': '1.0',
                'analysis_type': 'Final Comprehensive Analysis'
            },

            'executive_summary': {
                'collection_performance': {
                    'total_tickers_processed': analysis['total_expected'],
                    'successfully_collected': analysis['successfully_collected'],
                    'filtered_out': analysis['filtered_tickers'],
                    'actual_failures': analysis['actual_failures'],
                    'overall_success_rate': f"{analysis['overall_success_rate']:.1f}%",
                    'true_success_rate': f"{analysis['true_success_rate']:.1f}%"
                },
                'key_findings': [
                    f"Collected data for {analysis['successfully_collected']:,} tickers successfully",
                    f"True success rate of {analysis['true_success_rate']:.1f}% (excluding filtered tickers)",
                    f"Only {analysis['actual_failures']} true collection failures",
                    f"{analysis['filtered_tickers']} tickers filtered by business rules"
                ]
            },

            'detailed_breakdown': {
                'successful_collections': {
                    'count': analysis['successfully_collected'],
                    'average_files_per_ticker': sum(
                        stats['total_files'] for stats in analysis['collected_stats'].values()
                    ) / len(analysis['collected_stats']) if analysis['collected_stats'] else 0,
                    'total_files_collected': sum(
                        stats['total_files'] for stats in analysis['collected_stats'].values()
                    )
                },
                'filtered_collections': analysis['filtered_details'],
                'failed_collections': {
                    'total_count': analysis['actual_failures'],
                    'categories': analysis['failure_categories']
                }
            },

            'failure_analysis': {
                'by_category': {},
                'top_failure_reasons': {},
                'remediation_priority': {}
            },

            'recommendations': {
                'immediate_actions': [],
                'process_improvements': [],
                'long_term_enhancements': []
            },

            'comparison_with_original': {
                'polygon_reported': {
                    'total': collection_summary.get('total_tickers', 0),
                    'successful': collection_summary.get('successful_collections', 0),
                    'failed': collection_summary.get('failed_collections', 0),
                    'filtered': collection_summary.get('filtered_collections', 0)
                },
                'actual_analysis': {
                    'total': analysis['total_expected'],
                    'successful': analysis['successfully_collected'],
                    'failed': analysis['actual_failures'],
                    'filtered': analysis['filtered_tickers']
                },
                'explanation': 'Original summary counted filtered tickers as failures'
            }
        }

        # Build failure analysis section
        for category, data in analysis['failure_categories'].items():
            if data['count'] > 0:
                report['failure_analysis']['by_category'][category] = {
                    'count': data['count'],
                    'percentage': data['percentage'],
                    'description': data['description'],
                    'sample_tickers': data['tickers'][:10]  # First 10 as sample
                }

        # Top failure reasons
        sorted_categories = sorted(
            analysis['failure_categories'].items(),
            key=lambda x: x[1]['percentage'],
            reverse=True
        )

        for category, data in sorted_categories[:5]:
            if data['count'] > 0:
                report['failure_analysis']['top_failure_reasons'][category] = f"{data['percentage']:.1f}%"

        # Generate recommendations
        report['recommendations'] = self._generate_final_recommendations(analysis)

        return report

    def _generate_final_recommendations(self, analysis: Dict[str, Any]) -> Dict[str, List[str]]:
        """Generate final actionable recommendations"""
        recommendations = {
            'immediate_actions': [],
            'process_improvements': [],
            'long_term_enhancements': []
        }

        # Analyze failure categories for recommendations
        categories = analysis['failure_categories']

        # Immediate actions
        unknown_count = categories['UNKNOWN']['count']
        if unknown_count > 10:
            recommendations['immediate_actions'].append(
                f"Investigate {unknown_count} unknown failures with API analysis"
            )

        api_issues = categories['API_ISSUES']['count']
        if api_issues > 5:
            recommendations['immediate_actions'].append(
                f"Review and retry {api_issues} API-related failures"
            )

        # Process improvements
        if categories['LOW_MARKET_CAP']['count'] > 0:
            recommendations['process_improvements'].append(
                "Update ticker filtering to exclude sub-$1B market cap stocks upfront"
            )

        if categories['DELISTED_STOCKS']['count'] > 0:
            recommendations['process_improvements'].append(
                "Implement delisting checks before collection attempts"
            )

        success_rate = analysis['true_success_rate']
        if success_rate < 95:
            recommendations['process_improvements'].append(
                f"Improve collection pipeline to achieve >95% success rate (current: {success_rate:.1f}%)"
            )

        # Long-term enhancements
        recommendations['long_term_enhancements'].extend([
            "Implement predictive failure detection based on historical patterns",
            "Create automated retry workflows for transient failures",
            "Add real-time collection monitoring and alerting",
            "Develop ticker universe maintenance automation"
        ])

        return recommendations

    async def _save_final_reports(self, report: Dict[str, Any]):
        """Save final comprehensive reports"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # 1. Executive Summary (JSON)
        exec_summary = {
            'report_type': 'Final Executive Summary',
            'generated_at': report['report_metadata']['generated_at'],
            'collection_performance': report['executive_summary']['collection_performance'],
            'key_findings': report['executive_summary']['key_findings'],
            'top_failure_reasons': report['failure_analysis']['top_failure_reasons'],
            'immediate_actions': report['recommendations']['immediate_actions']
        }

        exec_path = f"{self.error_records_path}/final_reports/executive_summary_final_{timestamp}.json"
        with open(exec_path, 'w') as f:
            json.dump(exec_summary, f, indent=2)

        # 2. Complete Analysis Report (JSON)
        complete_path = f"{self.error_records_path}/final_reports/complete_analysis_{timestamp}.json"
        with open(complete_path, 'w') as f:
            json.dump(report, f, indent=2)

        # 3. Failure Details by Category (JSON)
        for category, data in report['detailed_breakdown']['failed_collections']['categories'].items():
            if data['count'] > 0:
                category_path = f"{self.error_records_path}/final_reports/failures_{category.lower()}_{timestamp}.json"
                with open(category_path, 'w') as f:
                    json.dump({
                        'category': category,
                        'description': data['description'],
                        'total_count': data['count'],
                        'percentage_of_failures': data['percentage'],
                        'failed_tickers': data['tickers']
                    }, f, indent=2)

        # 4. Remediation Action Plan (JSON)
        action_plan = {
            'remediation_plan': 'Polygon.io Collection Improvement Action Plan',
            'generated_at': report['report_metadata']['generated_at'],
            'current_performance': report['executive_summary']['collection_performance'],
            'action_items': report['recommendations'],
            'implementation_timeline': {
                'immediate (1-3 days)': report['recommendations']['immediate_actions'],
                'short_term (1-4 weeks)': report['recommendations']['process_improvements'],
                'long_term (1-3 months)': report['recommendations']['long_term_enhancements']
            }
        }

        action_path = f"{self.error_records_path}/final_reports/action_plan_{timestamp}.json"
        with open(action_path, 'w') as f:
            json.dump(action_plan, f, indent=2)

        logger.info(f"Final reports saved:")
        logger.info(f"  Executive Summary: {exec_path}")
        logger.info(f"  Complete Analysis: {complete_path}")
        logger.info(f"  Action Plan: {action_path}")


async def main():
    """Main function"""
    generator = FinalFailureReportGenerator()

    try:
        report = await generator.generate_comprehensive_report()

        print("\n" + "="*80)
        print("FINAL COMPREHENSIVE POLYGON.IO FAILURE ANALYSIS")
        print("="*80)

        perf = report['executive_summary']['collection_performance']
        print(f"Total Tickers Processed: {perf['total_tickers_processed']:,}")
        print(f"Successfully Collected: {perf['successfully_collected']:,}")
        print(f"Filtered Out (Business Rules): {perf['filtered_out']:,}")
        print(f"Actual Collection Failures: {perf['actual_failures']:,}")
        print(f"Overall Success Rate: {perf['overall_success_rate']}")
        print(f"True Success Rate (Excluding Filtered): {perf['true_success_rate']}")

        print(f"\nTop Failure Categories:")
        for category, percentage in report['failure_analysis']['top_failure_reasons'].items():
            count = report['detailed_breakdown']['failed_collections']['categories'][category]['count']
            print(f"  {category}: {count} failures ({percentage})")

        print(f"\nKey Findings:")
        for finding in report['executive_summary']['key_findings']:
            print(f"  • {finding}")

        print(f"\nImmediate Actions Required:")
        for action in report['recommendations']['immediate_actions']:
            print(f"  • {action}")

        print(f"\nReports saved to: /workspaces/data/error_records/polygon_failures/final_reports/")
        print("="*80)

    except Exception as e:
        logger.error(f"Final analysis failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())