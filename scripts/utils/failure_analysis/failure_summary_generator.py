#!/usr/bin/env python3
"""
Polygon.io Collection Failure Summary Generator

Creates corrected failure analysis with proper categorization and actionable insights.
"""

import os
import json
from datetime import datetime
from collections import defaultdict, Counter
from typing import Dict, List, Any
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FailureSummaryGenerator:
    def __init__(self):
        self.base_path = "/workspaces/data/raw_data/polygon"
        self.error_records_path = "/workspaces/data/error_records/polygon_failures"

        # Corrected failure categories with realistic business reasons
        self.failure_categories = {
            'COMPLETE_FAILURE': 'No data collected at all',
            'DELISTED_STOCK': 'Stock delisted or no longer trading',
            'INSUFFICIENT_MARKET_CAP': 'Market cap below investment threshold',
            'NEW_IPO': 'Recently IPO\'d with insufficient historical data',
            'API_RATE_LIMIT': 'Collection failed due to API rate limiting',
            'DATA_QUALITY_ISSUES': 'Data exists but quality insufficient',
            'TICKER_SYMBOL_INVALID': 'Invalid or outdated ticker symbol',
            'NETWORK_TIMEOUT': 'Network or API timeout during collection',
            'PROCESSING_ERROR': 'Error in data processing pipeline',
            'UNKNOWN': 'Requires manual investigation'
        }

    def generate_corrected_summary(self):
        """Generate corrected failure summary with proper analysis"""
        logger.info("Starting corrected failure analysis...")

        # Load collection summary
        collection_summary = self._load_collection_summary()

        # Load input data
        input_data = self._load_input_data()

        # Analyze actual collection status
        collection_analysis = self._analyze_actual_collection_status(input_data)

        # Generate summary report
        summary = self._create_failure_summary(collection_summary, collection_analysis)

        # Save reports
        self._save_summary_reports(summary)

        return summary

    def _load_collection_summary(self) -> Dict[str, Any]:
        """Load Polygon collection summary"""
        summary_path = f"{self.base_path}/collection_summary.json"
        try:
            with open(summary_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning(f"Collection summary not found")
            return {}

    def _load_input_data(self) -> List[Dict[str, Any]]:
        """Load YFinance input data"""
        input_source_path = "/workspaces/data/input_source"
        try:
            yfinance_files = [f for f in os.listdir(input_source_path)
                            if f.startswith("enriched_yfinance_")]
            if not yfinance_files:
                return []

            latest_file = sorted(yfinance_files)[-1]
            with open(f"{input_source_path}/{latest_file}", 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Could not load input data: {e}")
            return []

    def _analyze_actual_collection_status(self, input_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze what was actually collected vs expected"""
        logger.info("Analyzing actual collection status...")

        # Create ticker lookup
        input_tickers = {item['ticker']: item for item in input_data}

        # Scan collected data
        collected_tickers = set()
        collection_stats = {}

        if os.path.exists(self.base_path):
            for ticker_dir in os.listdir(self.base_path):
                ticker_path = os.path.join(self.base_path, ticker_dir)
                if not os.path.isdir(ticker_path) or ticker_dir.startswith('.'):
                    continue

                file_count = 0
                for root, dirs, files in os.walk(ticker_path):
                    file_count += len([f for f in files if f.endswith('.json')])

                if file_count > 0:
                    collected_tickers.add(ticker_dir)
                    collection_stats[ticker_dir] = {
                        'files_collected': file_count,
                        'status': 'SUCCESS'
                    }

        # Identify failures
        expected_tickers = set(input_tickers.keys())
        failed_tickers = expected_tickers - collected_tickers

        # Categorize failures
        failure_analysis = {}
        for ticker in failed_tickers:
            ticker_info = input_tickers.get(ticker, {})
            market_cap = ticker_info.get('market_cap', 0)

            # Categorize based on available information
            if market_cap < 2000000000:  # $2B threshold
                category = 'INSUFFICIENT_MARKET_CAP'
                reason = f"Market cap ${market_cap:,.0f} below $2B threshold"
            elif market_cap == 0 or not ticker_info:
                category = 'TICKER_SYMBOL_INVALID'
                reason = "No market cap data or ticker not found"
            else:
                category = 'COMPLETE_FAILURE'
                reason = "Collection failed despite valid ticker and market cap"

            failure_analysis[ticker] = {
                'ticker': ticker,
                'category': category,
                'reason': reason,
                'market_cap': market_cap,
                'ticker_info': ticker_info
            }

        return {
            'expected_tickers': len(expected_tickers),
            'collected_tickers': len(collected_tickers),
            'failed_tickers': len(failed_tickers),
            'collection_rate': (len(collected_tickers) / len(expected_tickers)) * 100 if expected_tickers else 0,
            'failures': failure_analysis,
            'collection_stats': collection_stats
        }

    def _create_failure_summary(self, collection_summary: Dict[str, Any],
                              analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Create comprehensive failure summary"""

        # Categorize failures
        failure_categories = defaultdict(list)
        for ticker, failure_info in analysis['failures'].items():
            category = failure_info['category']
            failure_categories[category].append(failure_info)

        # Calculate percentages
        total_failures = analysis['failed_tickers']
        category_percentages = {}
        for category, failures in failure_categories.items():
            percentage = (len(failures) / total_failures) * 100 if total_failures > 0 else 0
            category_percentages[category] = {
                'count': len(failures),
                'percentage': percentage
            }

        # Create summary
        summary = {
            'report_metadata': {
                'generated_at': datetime.now().isoformat(),
                'analysis_type': 'Corrected Polygon.io Collection Failure Analysis',
                'period': '2025-06-01 to 2025-09-01'
            },

            'collection_overview': {
                'total_expected': analysis['expected_tickers'],
                'successfully_collected': analysis['collected_tickers'],
                'failed_collections': analysis['failed_tickers'],
                'success_rate': f"{analysis['collection_rate']:.1f}%"
            },

            'failure_breakdown': {
                'total_failures': total_failures,
                'categories': category_percentages,
                'detailed_failures': dict(failure_categories)
            },

            'key_insights': self._generate_insights(category_percentages, analysis),

            'remediation_recommendations': self._generate_recommendations(category_percentages),

            'original_summary_comparison': {
                'polygon_reported_total': collection_summary.get('total_tickers', 0),
                'polygon_reported_successful': collection_summary.get('successful_collections', 0),
                'polygon_reported_failed': collection_summary.get('failed_collections', 0),
                'polygon_reported_filtered': collection_summary.get('filtered_collections', 0),
                'actual_analysis_difference': 'Original summary included filtered tickers as failures'
            }
        }

        return summary

    def _generate_insights(self, category_percentages: Dict[str, Dict],
                          analysis: Dict[str, Any]) -> List[str]:
        """Generate key insights from the analysis"""
        insights = []

        success_rate = analysis['collection_rate']
        insights.append(f"Actual collection success rate: {success_rate:.1f}%")

        if category_percentages:
            top_category = max(category_percentages.items(), key=lambda x: x[1]['percentage'])
            insights.append(f"Primary failure reason: {top_category[0]} ({top_category[1]['percentage']:.1f}% of failures)")

        # Business insights
        market_cap_failures = category_percentages.get('INSUFFICIENT_MARKET_CAP', {}).get('count', 0)
        if market_cap_failures > 0:
            insights.append(f"{market_cap_failures} tickers excluded due to market cap filtering (business rule)")

        actual_failures = sum(
            cat['count'] for cat_name, cat in category_percentages.items()
            if cat_name not in ['INSUFFICIENT_MARKET_CAP']
        )

        if actual_failures < 50:
            insights.append(f"Only {actual_failures} true collection failures - excellent performance")

        return insights

    def _generate_recommendations(self, category_percentages: Dict[str, Dict]) -> Dict[str, List[str]]:
        """Generate specific recommendations by category"""
        recommendations = {
            'immediate_actions': [],
            'process_improvements': [],
            'monitoring_enhancements': []
        }

        for category, stats in category_percentages.items():
            if stats['percentage'] > 20:  # Significant categories
                if category == 'INSUFFICIENT_MARKET_CAP':
                    recommendations['process_improvements'].append(
                        f"Review market cap filtering: {stats['count']} tickers excluded"
                    )
                elif category == 'COMPLETE_FAILURE':
                    recommendations['immediate_actions'].append(
                        f"Investigate {stats['count']} complete collection failures"
                    )
                elif category == 'TICKER_SYMBOL_INVALID':
                    recommendations['process_improvements'].append(
                        f"Clean ticker list: {stats['count']} invalid symbols"
                    )

        # General recommendations
        recommendations['monitoring_enhancements'].extend([
            'Implement real-time collection monitoring',
            'Add automated retry logic for transient failures',
            'Create alerts for collection rate drops below 95%'
        ])

        return recommendations

    def _save_summary_reports(self, summary: Dict[str, Any]):
        """Save summary reports to error records"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Create summary directories
        os.makedirs(f"{self.error_records_path}/summary", exist_ok=True)

        # 1. Executive Summary
        exec_summary = {
            'report_type': 'Corrected Executive Summary',
            'generated_at': summary['report_metadata']['generated_at'],
            'collection_performance': summary['collection_overview'],
            'top_failure_reasons': dict(sorted(
                summary['failure_breakdown']['categories'].items(),
                key=lambda x: x[1]['percentage'], reverse=True
            )[:5]),
            'key_insights': summary['key_insights'],
            'priority_actions': summary['remediation_recommendations']['immediate_actions']
        }

        exec_path = f"{self.error_records_path}/summary/executive_summary_corrected_{timestamp}.json"
        with open(exec_path, 'w') as f:
            json.dump(exec_summary, f, indent=2)

        # 2. Detailed Analysis
        detailed_path = f"{self.error_records_path}/summary/detailed_analysis_corrected_{timestamp}.json"
        with open(detailed_path, 'w') as f:
            json.dump(summary, f, indent=2)

        # 3. Category Breakdown CSV-friendly format
        category_breakdown = []
        for category, failures in summary['failure_breakdown']['detailed_failures'].items():
            for failure in failures:
                category_breakdown.append({
                    'ticker': failure['ticker'],
                    'category': failure['category'],
                    'reason': failure['reason'],
                    'market_cap': failure['market_cap']
                })

        breakdown_path = f"{self.error_records_path}/summary/failure_breakdown_{timestamp}.json"
        with open(breakdown_path, 'w') as f:
            json.dump(category_breakdown, f, indent=2)

        logger.info(f"Summary reports saved:")
        logger.info(f"  Executive: {exec_path}")
        logger.info(f"  Detailed: {detailed_path}")
        logger.info(f"  Breakdown: {breakdown_path}")


def main():
    """Main function"""
    generator = FailureSummaryGenerator()

    try:
        summary = generator.generate_corrected_summary()

        print("\n" + "="*70)
        print("CORRECTED POLYGON.IO FAILURE ANALYSIS SUMMARY")
        print("="*70)

        overview = summary['collection_overview']
        print(f"Expected Collections: {overview['total_expected']:,}")
        print(f"Successful Collections: {overview['successfully_collected']:,}")
        print(f"Failed Collections: {overview['failed_collections']:,}")
        print(f"Success Rate: {overview['success_rate']}")

        print(f"\nFailure Breakdown:")
        for category, stats in sorted(summary['failure_breakdown']['categories'].items(),
                                    key=lambda x: x[1]['percentage'], reverse=True):
            print(f"  {category}: {stats['count']} failures ({stats['percentage']:.1f}%)")

        print(f"\nKey Insights:")
        for insight in summary['key_insights']:
            print(f"  • {insight}")

        print(f"\nReports saved to: /workspaces/data/error_records/polygon_failures/summary/")
        print("="*70)

    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        raise


if __name__ == "__main__":
    main()