#!/usr/bin/env python3
"""
Analyze Failed Tickers Without API Access

Uses available data (input data, market cap, etc.) to categorize failures
without requiring Polygon API access.
"""

import os
import json
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Any
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FailureAnalyzerNoAPI:
    def __init__(self):
        self.error_records_path = "/workspaces/data/error_records/polygon_failures"
        self.input_source_path = "/workspaces/data/input_source"

    def analyze_failures(self):
        """Analyze failures using available data"""
        logger.info("Analyzing failures without API access...")

        # Load data
        failed_tickers = self._load_failed_tickers()
        input_data = self._load_input_data()

        # Create lookup
        input_lookup = {item['ticker']: item for item in input_data}

        # Categorize based on available data
        categorized = self._categorize_without_api(failed_tickers, input_lookup)

        # Generate report
        report = self._create_analysis_report(categorized)

        # Save report
        self._save_report(report)

        return report

    def _load_failed_tickers(self) -> List[str]:
        """Load failed tickers list"""
        analysis_file = f"{self.error_records_path}/final_reports/complete_analysis_20250926_210449.json"

        with open(analysis_file, 'r') as f:
            analysis = json.load(f)

        unknown_failures = analysis['detailed_breakdown']['failed_collections']['categories']['UNKNOWN']['tickers']
        return [item['ticker'] for item in unknown_failures]

    def _load_input_data(self) -> List[Dict[str, Any]]:
        """Load YFinance input data"""
        try:
            yfinance_files = [f for f in os.listdir(self.input_source_path)
                            if f.startswith("enriched_yfinance_")]
            if not yfinance_files:
                return []

            latest_file = sorted(yfinance_files)[-1]
            with open(f"{self.input_source_path}/{latest_file}", 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Could not load input data: {e}")
            return []

    def _categorize_without_api(self, failed_tickers: List[str],
                               input_lookup: Dict[str, Any]) -> Dict[str, Any]:
        """Categorize failures based on available data"""
        logger.info(f"Categorizing {len(failed_tickers)} failures...")

        categories = {
            'VERY_LOW_MARKET_CAP': {
                'description': 'Market cap below $1B (micro-cap)',
                'tickers': [],
                'reason': 'Likely delisted or insufficient trading volume'
            },
            'LOW_MARKET_CAP': {
                'description': 'Market cap $1B-$2B (small-cap)',
                'tickers': [],
                'reason': 'Below collection threshold, marginal investment grade'
            },
            'NO_MARKET_DATA': {
                'description': 'No market cap or fundamental data available',
                'tickers': [],
                'reason': 'Likely delisted, invalid ticker, or data source issue'
            },
            'VALID_LARGE_CAP': {
                'description': 'Market cap >$2B with no obvious issues',
                'tickers': [],
                'reason': 'Requires API investigation - unexpected failure'
            },
            'NON_US_MARKET': {
                'description': 'Non-US market ticker',
                'tickers': [],
                'reason': 'Outside collection scope'
            },
            'INSUFFICIENT_DATA': {
                'description': 'Missing critical ticker information',
                'tickers': [],
                'reason': 'Data quality issues in input source'
            }
        }

        # Categorize each ticker
        for ticker in failed_tickers:
            ticker_info = input_lookup.get(ticker, {})

            if not ticker_info:
                categories['INSUFFICIENT_DATA']['tickers'].append({
                    'ticker': ticker,
                    'market_cap': 0,
                    'issue': 'Not found in input data'
                })
                continue

            market_cap = ticker_info.get('market_cap', 0)
            country = ticker_info.get('country', 'Unknown')
            exchange = ticker_info.get('exchange', 'Unknown')

            # Categorization logic
            if country and country.upper() not in ['UNITED STATES', 'US', 'USA']:
                categories['NON_US_MARKET']['tickers'].append({
                    'ticker': ticker,
                    'market_cap': market_cap,
                    'country': country,
                    'exchange': exchange
                })
            elif market_cap == 0 or market_cap is None:
                categories['NO_MARKET_DATA']['tickers'].append({
                    'ticker': ticker,
                    'market_cap': market_cap,
                    'issue': 'Missing or zero market cap'
                })
            elif market_cap < 1_000_000_000:  # < $1B
                categories['VERY_LOW_MARKET_CAP']['tickers'].append({
                    'ticker': ticker,
                    'market_cap': market_cap,
                    'market_cap_formatted': f'${market_cap:,.0f}'
                })
            elif market_cap < 2_000_000_000:  # $1B - $2B
                categories['LOW_MARKET_CAP']['tickers'].append({
                    'ticker': ticker,
                    'market_cap': market_cap,
                    'market_cap_formatted': f'${market_cap:,.0f}'
                })
            else:  # >= $2B
                categories['VALID_LARGE_CAP']['tickers'].append({
                    'ticker': ticker,
                    'market_cap': market_cap,
                    'market_cap_formatted': f'${market_cap:,.0f}',
                    'note': 'Unexpected failure - requires API investigation'
                })

        # Calculate counts and percentages
        total_failures = len(failed_tickers)
        for category, data in categories.items():
            count = len(data['tickers'])
            data['count'] = count
            data['percentage'] = (count / total_failures) * 100 if total_failures > 0 else 0

        return {
            'total_failures': total_failures,
            'categories': categories,
            'analysis_metadata': {
                'timestamp': datetime.now().isoformat(),
                'api_access': False,
                'analysis_method': 'Market cap and metadata-based categorization'
            }
        }

    def _create_analysis_report(self, categorized: Dict[str, Any]) -> Dict[str, Any]:
        """Create comprehensive analysis report"""

        # Sort categories by count
        sorted_categories = sorted(
            categorized['categories'].items(),
            key=lambda x: x[1]['count'],
            reverse=True
        )

        report = {
            'report_metadata': {
                'title': 'Failure Analysis Without API Access',
                'generated_at': datetime.now().isoformat(),
                'analysis_method': 'Market cap and metadata-based',
                'api_investigation_recommended': True
            },

            'executive_summary': {
                'total_failures_analyzed': categorized['total_failures'],
                'categories_identified': len([c for c in categorized['categories'].values() if c['count'] > 0]),
                'top_failure_reasons': {
                    cat: {'count': data['count'], 'percentage': data['percentage']}
                    for cat, data in sorted_categories[:5] if data['count'] > 0
                }
            },

            'detailed_breakdown': {
                cat: {
                    'count': data['count'],
                    'percentage': data['percentage'],
                    'description': data['description'],
                    'reason': data['reason'],
                    'sample_tickers': data['tickers'][:10],  # First 10
                    'all_tickers': [t['ticker'] if isinstance(t, dict) else t for t in data['tickers']]
                }
                for cat, data in sorted_categories if data['count'] > 0
            },

            'key_insights': self._generate_insights(categorized),

            'recommendations': self._generate_recommendations(categorized),

            'next_steps': [
                'Set up Polygon API key for detailed investigation',
                'Run investigate_failed_tickers.py with API access',
                'Filter out expected exclusions (very low market cap, non-US)',
                'Retry collection for valid large-cap failures',
                'Update input data to exclude invalid tickers'
            ]
        }

        return report

    def _generate_insights(self, categorized: Dict[str, Any]) -> List[str]:
        """Generate key insights"""
        insights = []
        categories = categorized['categories']
        total = categorized['total_failures']

        # Market cap analysis
        very_low_cap = categories['VERY_LOW_MARKET_CAP']['count']
        low_cap = categories['LOW_MARKET_CAP']['count']
        market_cap_related = very_low_cap + low_cap

        if market_cap_related > 0:
            pct = (market_cap_related / total) * 100
            insights.append(
                f"{market_cap_related} failures ({pct:.1f}%) are market cap-related (<$2B)"
            )

        # Data quality issues
        no_data = categories['NO_MARKET_DATA']['count']
        insufficient = categories['INSUFFICIENT_DATA']['count']
        data_issues = no_data + insufficient

        if data_issues > 0:
            pct = (data_issues / total) * 100
            insights.append(
                f"{data_issues} failures ({pct:.1f}%) have data quality issues"
            )

        # Valid large caps
        valid_large = categories['VALID_LARGE_CAP']['count']
        if valid_large > 0:
            pct = (valid_large / total) * 100
            insights.append(
                f"{valid_large} failures ({pct:.1f}%) are unexpected - large cap with good data"
            )

        # Non-US
        non_us = categories['NON_US_MARKET']['count']
        if non_us > 0:
            pct = (non_us / total) * 100
            insights.append(
                f"{non_us} failures ({pct:.1f}%) are non-US tickers (expected exclusions)"
            )

        # Expected vs unexpected
        expected = very_low_cap + low_cap + no_data + non_us
        unexpected = total - expected
        if unexpected > 0:
            pct = (unexpected / total) * 100
            insights.append(
                f"{unexpected} failures ({pct:.1f}%) are unexpected and require investigation"
            )

        return insights

    def _generate_recommendations(self, categorized: Dict[str, Any]) -> Dict[str, List[str]]:
        """Generate actionable recommendations"""
        categories = categorized['categories']

        recommendations = {
            'immediate_actions': [],
            'process_improvements': [],
            'api_investigation_required': []
        }

        # Immediate actions
        very_low_cap = categories['VERY_LOW_MARKET_CAP']['count']
        if very_low_cap > 10:
            recommendations['immediate_actions'].append(
                f"Filter {very_low_cap} micro-cap tickers (<$1B) from input data"
            )

        low_cap = categories['LOW_MARKET_CAP']['count']
        if low_cap > 10:
            recommendations['immediate_actions'].append(
                f"Review {low_cap} small-cap tickers ($1B-$2B) - adjust threshold or exclude"
            )

        no_data = categories['NO_MARKET_DATA']['count']
        if no_data > 0:
            recommendations['immediate_actions'].append(
                f"Clean {no_data} tickers with missing market data from input source"
            )

        # Process improvements
        non_us = categories['NON_US_MARKET']['count']
        if non_us > 0:
            recommendations['process_improvements'].append(
                f"Add geographic filter to exclude {non_us} non-US tickers upfront"
            )

        recommendations['process_improvements'].extend([
            'Implement pre-collection validation for market cap and data quality',
            'Add ticker status checks before collection attempts',
            'Create automated input data cleaning workflow'
        ])

        # API investigation
        valid_large = categories['VALID_LARGE_CAP']['count']
        insufficient = categories['INSUFFICIENT_DATA']['count']

        if valid_large > 0:
            recommendations['api_investigation_required'].append(
                f"Investigate {valid_large} large-cap failures with Polygon API"
            )

        if insufficient > 0:
            recommendations['api_investigation_required'].append(
                f"Cross-reference {insufficient} data-insufficient tickers with API"
            )

        return recommendations

    def _save_report(self, report: Dict[str, Any]):
        """Save analysis report"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        os.makedirs(f"{self.error_records_path}/no_api_analysis", exist_ok=True)

        # Save full report
        report_path = f"{self.error_records_path}/no_api_analysis/failure_analysis_{timestamp}.json"
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)

        # Save ticker lists by category
        for category, data in report['detailed_breakdown'].items():
            ticker_list_path = f"{self.error_records_path}/no_api_analysis/tickers_{category.lower()}_{timestamp}.json"
            with open(ticker_list_path, 'w') as f:
                json.dump({
                    'category': category,
                    'count': data['count'],
                    'tickers': data['all_tickers']
                }, f, indent=2)

        logger.info(f"Report saved to {report_path}")
        logger.info(f"Ticker lists saved for {len(report['detailed_breakdown'])} categories")


def main():
    """Main function"""
    analyzer = FailureAnalyzerNoAPI()

    try:
        report = analyzer.analyze_failures()

        print("\n" + "="*80)
        print("FAILURE ANALYSIS COMPLETE (WITHOUT API)")
        print("="*80)

        summary = report['executive_summary']
        print(f"Total Failures Analyzed: {summary['total_failures_analyzed']}")
        print(f"Categories Identified: {summary['categories_identified']}")

        print(f"\nTop Failure Categories:")
        for category, data in summary['top_failure_reasons'].items():
            print(f"  {category}: {data['count']} failures ({data['percentage']:.1f}%)")

        print(f"\nKey Insights:")
        for insight in report['key_insights']:
            print(f"  • {insight}")

        print(f"\nImmediate Actions:")
        for action in report['recommendations']['immediate_actions']:
            print(f"  • {action}")

        print(f"\nReports saved to: /workspaces/data/error_records/polygon_failures/no_api_analysis/")
        print("="*80)
        print("\nNOTE: For detailed investigation, run with POLYGON_API_KEY set:")
        print("  export POLYGON_API_KEY='your_api_key'")
        print("  python scripts/utils/failure_analysis/investigate_failed_tickers.py")
        print("="*80)

    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        raise


if __name__ == "__main__":
    main()