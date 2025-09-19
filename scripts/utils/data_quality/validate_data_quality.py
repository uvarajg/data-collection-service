#!/usr/bin/env python3
"""
Data Quality Validation Tool for AlgoAlchemist Data Collection Service

This script validates the quality of collected stock data by checking:
- Technical indicator coverage
- Fundamental data coverage
- Data completeness and structure
- Quality grades and scoring

Usage:
    python validate_data_quality.py [date]
    python validate_data_quality.py 2025-09-18
    python validate_data_quality.py --range 2025-09-15 2025-09-18
"""

import json
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

class DataQualityValidator:
    def __init__(self, base_path='/workspaces/data/historical/daily'):
        self.base_path = Path(base_path)

    def validate_single_date(self, target_date: str) -> Dict:
        """Validate data quality for a single date"""
        print(f"📋 Validating data quality for {target_date}")
        print("=" * 60)

        # Count files and coverage
        total_files = 0
        tech_files = 0
        fund_files = 0
        sample_data = []

        for ticker_dir in self.base_path.iterdir():
            if ticker_dir.is_dir():
                file_path = ticker_dir / '2025' / target_date.split('-')[1] / f'{target_date}.json'
                if file_path.exists():
                    total_files += 1
                    try:
                        with open(file_path, 'r') as f:
                            data = json.load(f)

                        # Check for technical indicators (multiple possible structures)
                        has_tech = ('technical_indicators' in data or 'technical' in data)
                        if has_tech:
                            tech_files += 1

                        # Check for fundamental data
                        has_fund = ('fundamentals' in data or 'fundamental' in data)
                        if has_fund:
                            fund_files += 1

                        # Collect sample for detailed analysis
                        if ticker_dir.name in ['AAPL', 'MSFT', 'NVDA', 'GOOGL', 'TSLA', 'AMZN', 'META']:
                            tech_count = 0
                            fund_count = 0

                            if 'technical_indicators' in data:
                                tech_count = len(data['technical_indicators'])
                            elif 'technical' in data:
                                tech_count = len(data['technical'])

                            if 'fundamentals' in data:
                                fund_count = len(data['fundamentals'])
                            elif 'fundamental' in data:
                                fund_count = len(data['fundamental'])

                            sample_data.append({
                                'ticker': ticker_dir.name,
                                'tech_count': tech_count,
                                'fund_count': fund_count,
                                'has_tech': has_tech,
                                'has_fund': has_fund
                            })

                    except Exception as e:
                        continue

        # Calculate metrics
        tech_coverage = (tech_files / total_files * 100) if total_files > 0 else 0
        fund_coverage = (fund_files / total_files * 100) if total_files > 0 else 0

        # Determine grade
        if tech_coverage >= 99 and fund_coverage >= 99:
            grade = "A+"
            quality_score = 99.9
        elif tech_coverage >= 95 and fund_coverage >= 90:
            grade = "A"
            quality_score = 95.0
        elif tech_coverage >= 85 and fund_coverage >= 80:
            grade = "B+"
            quality_score = 85.0
        elif tech_coverage >= 70 and fund_coverage >= 70:
            grade = "B"
            quality_score = 75.0
        else:
            grade = "C"
            quality_score = 60.0

        # Print results
        print(f"📁 Total Files: {total_files}")
        print(f"📈 Technical Coverage: {tech_files}/{total_files} ({tech_coverage:.1f}%)")
        print(f"📊 Fundamental Coverage: {fund_files}/{total_files} ({fund_coverage:.1f}%)")
        print(f"🎯 Quality Score: {quality_score:.1f}")
        print(f"📝 Grade: {grade}")

        print(f"\n📋 Sample Analysis (Major Stocks):")
        for stock in sample_data:
            tech_status = "✅" if stock['tech_count'] >= 8 else "⚠️" if stock['tech_count'] > 0 else "❌"
            fund_status = "✅" if stock['fund_count'] >= 5 else "⚠️" if stock['fund_count'] > 0 else "❌"
            print(f"   {tech_status} {stock['ticker']}: {stock['tech_count']} tech, {stock['fund_count']} fund")

        return {
            'date': target_date,
            'total_files': total_files,
            'tech_coverage': tech_coverage,
            'fund_coverage': fund_coverage,
            'quality_score': quality_score,
            'grade': grade,
            'sample_data': sample_data
        }

    def validate_date_range(self, start_date: str, end_date: str) -> List[Dict]:
        """Validate data quality for a date range"""
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')

        results = []
        current = start

        while current <= end:
            date_str = current.strftime('%Y-%m-%d')
            result = self.validate_single_date(date_str)
            results.append(result)
            print()  # Add spacing between dates
            current += timedelta(days=1)

        return results

    def print_summary_table(self, results: List[Dict]):
        """Print a summary table of validation results"""
        print("📊 DATA QUALITY SUMMARY TABLE")
        print("=" * 80)
        print(f"{'Date':<12} {'Technical':<12} {'Fundamental':<12} {'Quality':<10} {'Grade':<6}")
        print("-" * 80)

        for result in results:
            print(f"{result['date']:<12} {result['tech_coverage']:<11.1f}% "
                  f"{result['fund_coverage']:<11.1f}% {result['quality_score']:<9.1f} {result['grade']:<6}")

def main():
    validator = DataQualityValidator()

    if len(sys.argv) == 1:
        # Interactive mode
        date = input("Enter date to validate (YYYY-MM-DD) or 'range' for date range: ").strip()
        if date.lower() == 'range':
            start_date = input("Enter start date (YYYY-MM-DD): ").strip()
            end_date = input("Enter end date (YYYY-MM-DD): ").strip()
            results = validator.validate_date_range(start_date, end_date)
            validator.print_summary_table(results)
        else:
            validator.validate_single_date(date)

    elif len(sys.argv) == 2:
        # Single date mode
        target_date = sys.argv[1]
        validator.validate_single_date(target_date)

    elif len(sys.argv) == 4 and sys.argv[1] == '--range':
        # Date range mode
        start_date = sys.argv[2]
        end_date = sys.argv[3]
        results = validator.validate_date_range(start_date, end_date)
        validator.print_summary_table(results)

    else:
        print("Usage:")
        print("  python validate_data_quality.py [date]")
        print("  python validate_data_quality.py 2025-09-18")
        print("  python validate_data_quality.py --range 2025-09-15 2025-09-18")

if __name__ == "__main__":
    main()