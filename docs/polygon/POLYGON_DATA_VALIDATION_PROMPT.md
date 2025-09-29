# Polygon.io Data Validation Prompt

## Overview
This prompt guides you through validating historical data collected from Polygon.io API to ensure data quality, completeness, and consistency.

## Prerequisites
- Polygon.io data collection completed (using `collect_polygon_dates.py` or `historical_input_data_polygon.py`)
- Data stored in: `/workspaces/data/raw_data/polygon/`
- Python 3.11+ with pandas and json libraries

## Validation Steps

### 1. Quick Summary Check

First, review the collection summary to understand overall success rate:

```bash
# For date range collection (collect_polygon_dates.py)
cat /workspaces/data/raw_data/polygon/summary_2025-09-01_2025-09-14.json

# Expected output:
# {
#   "date_range": {"start": "2025-09-01", "end": "2025-09-14"},
#   "total_tickers": 2088,
#   "successful": 2084,
#   "failed": 4,
#   "success_rate": "99.8%",
#   "completed_at": "2025-09-26T04:02:52.839611"
# }
```

**Quality Criteria:**
- ✅ Success rate >= 99.0%
- ⚠️ Success rate 95.0% - 98.9% (Acceptable, review failures)
- ❌ Success rate < 95.0% (Needs investigation)

### 2. Validate Data Structure

Check that files follow the expected structure:

```bash
# Count total files collected
find /workspaces/data/raw_data/polygon -name "*.json" -type f | grep -v summary | wc -l

# Check sample file structure
cat /workspaces/data/raw_data/polygon/AAPL/2025/09/2025-09-05.json
```

**Expected Structure:**
```json
{
  "ticker": "AAPL",
  "date": "2025-09-05",
  "open": 239.995,
  "high": 241.32,
  "low": 238.4901,
  "close": 239.69,
  "volume": 54870397.0,
  "vwap": 239.6771,
  "transactions": 610786,
  "source": "polygon.io",
  "collected_at": "2025-09-26T04:02:24.136438"
}
```

**Validation Checklist:**
- ✅ All OHLCV fields present (open, high, low, close, volume)
- ✅ Additional metrics: vwap, transactions
- ✅ Metadata: source = "polygon.io", collected_at timestamp
- ✅ Proper data types (floats for prices, int for volume)

### 3. Data Completeness Validation Script

Create a validation script to check data completeness:

```python
#!/usr/bin/env python3
"""
Polygon.io Data Validation Script

Validates data collected from Polygon.io API for completeness and quality.
"""

import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List

class PolygonDataValidator:
    def __init__(self, base_path='/workspaces/data/raw_data/polygon'):
        self.base_path = Path(base_path)
        self.required_fields = ['ticker', 'date', 'open', 'high', 'low', 'close', 'volume']
        self.optional_fields = ['vwap', 'transactions']

    def validate_date_range(self, start_date: str, end_date: str) -> Dict:
        """Validate all files for a date range"""
        print(f"🔍 Validating Polygon.io data: {start_date} to {end_date}")
        print("=" * 70)

        # Generate list of expected trading days
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')

        trading_days = []
        current = start
        while current <= end:
            # Skip weekends (0=Monday, 5=Saturday, 6=Sunday)
            if current.weekday() < 5:
                trading_days.append(current.strftime('%Y-%m-%d'))
            current += timedelta(days=1)

        print(f"📅 Expected trading days: {len(trading_days)}")
        print(f"📅 Date range: {trading_days[0]} to {trading_days[-1]}")
        print()

        # Get all tickers
        tickers = sorted([d.name for d in self.base_path.iterdir()
                         if d.is_dir() and d.name != 'error_records'])

        print(f"📊 Total tickers found: {len(tickers)}")
        print()

        # Validation counters
        total_expected = len(tickers) * len(trading_days)
        files_found = 0
        files_valid = 0
        files_invalid = 0
        missing_files = 0

        validation_errors = []
        missing_tickers = []

        for ticker in tickers:
            ticker_files = 0
            ticker_valid = 0

            for date in trading_days:
                year = date.split('-')[0]
                month = date.split('-')[1]
                file_path = self.base_path / ticker / year / month / f"{date}.json"

                if not file_path.exists():
                    missing_files += 1
                    if ticker_files == 0:  # Only report first missing file per ticker
                        missing_tickers.append({
                            'ticker': ticker,
                            'first_missing_date': date
                        })
                    continue

                files_found += 1
                ticker_files += 1

                # Validate file content
                try:
                    with open(file_path, 'r') as f:
                        data = json.load(f)

                    # Check required fields
                    missing_fields = [f for f in self.required_fields if f not in data]
                    if missing_fields:
                        files_invalid += 1
                        validation_errors.append({
                            'ticker': ticker,
                            'date': date,
                            'error': f"Missing fields: {missing_fields}"
                        })
                        continue

                    # Validate data values
                    errors = []
                    if data['open'] <= 0:
                        errors.append('Invalid open price')
                    if data['high'] < data['open'] or data['high'] < data['close']:
                        errors.append('High price inconsistent')
                    if data['low'] > data['open'] or data['low'] > data['close']:
                        errors.append('Low price inconsistent')
                    if data['close'] <= 0:
                        errors.append('Invalid close price')
                    if data['volume'] < 0:
                        errors.append('Invalid volume')

                    if errors:
                        files_invalid += 1
                        validation_errors.append({
                            'ticker': ticker,
                            'date': date,
                            'error': '; '.join(errors)
                        })
                    else:
                        files_valid += 1
                        ticker_valid += 1

                except json.JSONDecodeError:
                    files_invalid += 1
                    validation_errors.append({
                        'ticker': ticker,
                        'date': date,
                        'error': 'Invalid JSON format'
                    })
                except Exception as e:
                    files_invalid += 1
                    validation_errors.append({
                        'ticker': ticker,
                        'date': date,
                        'error': str(e)
                    })

        # Calculate metrics
        success_rate = (files_valid / total_expected * 100) if total_expected > 0 else 0
        found_rate = (files_found / total_expected * 100) if total_expected > 0 else 0

        # Determine grade
        if success_rate >= 99.0:
            grade = "A+"
        elif success_rate >= 95.0:
            grade = "A"
        elif success_rate >= 90.0:
            grade = "B+"
        elif success_rate >= 85.0:
            grade = "B"
        elif success_rate >= 80.0:
            grade = "C"
        else:
            grade = "F"

        # Print results
        print("📈 VALIDATION RESULTS")
        print("=" * 70)
        print(f"Total Expected Files:    {total_expected:,}")
        print(f"Files Found:             {files_found:,} ({found_rate:.1f}%)")
        print(f"Files Valid:             {files_valid:,} ({success_rate:.1f}%)")
        print(f"Files Invalid:           {files_invalid:,}")
        print(f"Missing Files:           {missing_files:,}")
        print()
        print(f"📊 Quality Grade:        {grade}")
        print(f"📊 Success Rate:         {success_rate:.2f}%")
        print()

        # Report errors
        if validation_errors:
            print(f"⚠️  VALIDATION ERRORS ({len(validation_errors)})")
            print("=" * 70)
            for error in validation_errors[:20]:  # Show first 20 errors
                print(f"  {error['ticker']} | {error['date']}: {error['error']}")
            if len(validation_errors) > 20:
                print(f"  ... and {len(validation_errors) - 20} more errors")
            print()

        if missing_tickers:
            print(f"⚠️  TICKERS WITH MISSING DATA ({len(missing_tickers)})")
            print("=" * 70)
            for item in missing_tickers[:20]:  # Show first 20
                print(f"  {item['ticker']}: First missing date = {item['first_missing_date']}")
            if len(missing_tickers) > 20:
                print(f"  ... and {len(missing_tickers) - 20} more tickers")
            print()

        # Summary
        print("✅ VALIDATION SUMMARY")
        print("=" * 70)
        if success_rate >= 99.0:
            print("✓ Data quality is EXCELLENT (Grade A+)")
            print("✓ Ready for production use")
        elif success_rate >= 95.0:
            print("✓ Data quality is GOOD (Grade A)")
            print("✓ Minor issues detected, but acceptable for use")
        elif success_rate >= 90.0:
            print("⚠ Data quality is ACCEPTABLE (Grade B+)")
            print("⚠ Review and address validation errors")
        else:
            print("✗ Data quality is POOR (Grade < B)")
            print("✗ Significant issues detected - investigate before use")
        print()

        return {
            'total_expected': total_expected,
            'files_found': files_found,
            'files_valid': files_valid,
            'files_invalid': files_invalid,
            'missing_files': missing_files,
            'success_rate': success_rate,
            'grade': grade,
            'validation_errors': validation_errors[:50],  # Keep first 50 errors
            'missing_tickers': missing_tickers[:50]
        }


def main():
    """Main validation entry point"""
    import sys

    validator = PolygonDataValidator()

    if len(sys.argv) < 3:
        print("Usage: python validate_polygon_data.py START_DATE END_DATE")
        print("Example: python validate_polygon_data.py 2025-09-01 2025-09-14")
        sys.exit(1)

    start_date = sys.argv[1]
    end_date = sys.argv[2]

    results = validator.validate_date_range(start_date, end_date)

    # Save results
    output_file = Path('/workspaces/data/raw_data/polygon') / f'validation_report_{start_date}_{end_date}.json'
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)

    print(f"📝 Validation report saved: {output_file}")


if __name__ == '__main__':
    main()
```

**Usage:**
```bash
# Save the script
cd /workspaces/data-collection-service
nano scripts/polygon/validate_polygon_data.py
# Paste the script above

# Make it executable
chmod +x scripts/polygon/validate_polygon_data.py

# Run validation
python scripts/polygon/validate_polygon_data.py 2025-09-01 2025-09-14
```

### 4. Data Consistency Checks

Verify data consistency across multiple tickers:

```bash
# Check that high >= low for all files
python3 << 'EOF'
import json
from pathlib import Path

base_path = Path('/workspaces/data/raw_data/polygon')
errors = []

for ticker_dir in base_path.iterdir():
    if ticker_dir.is_dir() and ticker_dir.name != 'error_records':
        for json_file in ticker_dir.rglob('*.json'):
            try:
                with open(json_file) as f:
                    data = json.load(f)

                if data['high'] < data['low']:
                    errors.append(f"{data['ticker']} {data['date']}: High < Low")
                if data['high'] < data['open'] or data['high'] < data['close']:
                    errors.append(f"{data['ticker']} {data['date']}: High not highest")
                if data['low'] > data['open'] or data['low'] > data['close']:
                    errors.append(f"{data['ticker']} {data['date']}: Low not lowest")
            except:
                pass

if errors:
    print(f"❌ Found {len(errors)} consistency errors:")
    for e in errors[:20]:
        print(f"   {e}")
else:
    print("✅ All data passed consistency checks!")
EOF
```

### 5. Compare with Summary Report

Cross-check actual files with collection summary:

```bash
# Count actual JSON files (excluding summary files)
ACTUAL_FILES=$(find /workspaces/data/raw_data/polygon -name "*.json" -type f | grep -v summary | wc -l)

# Get expected count from summary
EXPECTED=$(cat /workspaces/data/raw_data/polygon/summary_*.json | python3 -c "import sys, json; data=json.load(sys.stdin); print(data['successful'] * len([d for d in __import__('datetime').date(2025,9,1).__class__(2025,9,1).__class__.__dict__ if not d.startswith('_')]))")

echo "📊 File Count Validation:"
echo "   Actual files found: $ACTUAL_FILES"
echo "   Expected from summary: Check manually"
```

### 6. Sample Data Quality Check

Manually inspect a few random files to ensure data looks reasonable:

```bash
# Random sample validation
echo "📋 Sample Data Validation:"
echo ""

# AAPL
echo "🍎 AAPL (Tech - High Volume):"
cat /workspaces/data/raw_data/polygon/AAPL/2025/09/2025-09-05.json | jq '{ticker, date, open, high, low, close, volume}'

echo ""

# TSLA
echo "🚗 TSLA (Volatile Stock):"
cat /workspaces/data/raw_data/polygon/TSLA/2025/09/2025-09-05.json | jq '{ticker, date, open, high, low, close, volume}'

echo ""

# A small cap stock
echo "📊 Small Cap Sample:"
ls /workspaces/data/raw_data/polygon | tail -1 | xargs -I {} cat /workspaces/data/raw_data/polygon/{}/2025/09/2025-09-05.json | jq '{ticker, date, open, high, low, close, volume}'
```

### 7. Date Range Completeness

Verify all expected trading days are present:

```bash
# Check date completeness for AAPL (as reference)
python3 << 'EOF'
from pathlib import Path
from datetime import datetime, timedelta

# Expected date range
start = datetime(2025, 9, 1)
end = datetime(2025, 9, 14)

# Generate trading days (excluding weekends)
expected_dates = []
current = start
while current <= end:
    if current.weekday() < 5:  # Monday=0, Friday=4
        expected_dates.append(current.strftime('%Y-%m-%d'))
    current += timedelta(days=1)

print(f"📅 Expected Trading Days ({len(expected_dates)}):")
for date in expected_dates:
    print(f"   {date}")

# Check AAPL files
aapl_path = Path('/workspaces/data/raw_data/polygon/AAPL/2025/09')
found_dates = sorted([f.stem for f in aapl_path.glob('*.json')])

print(f"\n📁 AAPL Files Found ({len(found_dates)}):")
for date in found_dates:
    print(f"   {date}")

missing = set(expected_dates) - set(found_dates)
if missing:
    print(f"\n⚠️  Missing dates: {missing}")
else:
    print(f"\n✅ All expected dates present!")
EOF
```

## Validation Criteria Summary

### Grade A+ (Excellent - Production Ready)
- ✅ Success rate >= 99.0%
- ✅ All OHLCV fields present
- ✅ Data consistency checks pass
- ✅ No significant missing dates
- ✅ Proper file structure and formatting

### Grade A (Good - Acceptable)
- ✅ Success rate >= 95.0%
- ✅ Minor missing data (< 5%)
- ⚠️ Few consistency issues (< 1%)

### Grade B (Fair - Needs Review)
- ⚠️ Success rate >= 85.0%
- ⚠️ Some missing data (5-15%)
- ⚠️ Multiple consistency issues

### Grade F (Poor - Needs Reprocessing)
- ❌ Success rate < 85.0%
- ❌ Significant missing data (> 15%)
- ❌ Many consistency issues

## Troubleshooting

### Common Issues

1. **Missing Files for Specific Tickers**
   - Check if ticker had no trading activity on those dates
   - Verify ticker still exists (delisted/merged stocks)
   - Review Polygon API response for those tickers

2. **Inconsistent OHLC Values**
   - May indicate split/reverse split events
   - Check Polygon API for corporate actions
   - Verify `adjusted=true` parameter was used

3. **Low Success Rate**
   - Check API key validity
   - Verify rate limiting wasn't too aggressive
   - Review network connectivity during collection
   - Check error logs for specific failures

## Next Steps After Validation

### If Grade A+ or A (Success Rate >= 95%)
1. ✅ Data is production ready
2. ✅ Proceed with technical indicator calculations (if needed)
3. ✅ Integrate with validation service
4. ✅ Use for backtesting and analysis

### If Grade B (Success Rate 85-95%)
1. ⚠️ Review specific validation errors
2. ⚠️ Re-collect failed tickers using script
3. ⚠️ Consider if missing data is acceptable for use case

### If Grade F (Success Rate < 85%)
1. ❌ Investigate root cause of failures
2. ❌ Review API key and rate limits
3. ❌ Check network and system logs
4. ❌ Re-run collection with adjustments

## Integration with Data Validation Service

Once Polygon data is validated, you can optionally run it through the existing data validation service:

```bash
# Run technical validation on Polygon data
python scripts/utils/data_quality/validate_data_quality.py --path /workspaces/data/raw_data/polygon --date 2025-09-05
```

**Note**: The standard validation service expects technical indicators and fundamentals, which Polygon data doesn't include by default. You may need to calculate these separately using the Alpaca-based scripts.

---

**Created**: September 2025
**Compatible With**: Polygon.io Stocks Starter Plan
**Data Format**: OHLCV JSON (simplified format)
**Python Version**: 3.11+