# Data Collection Service Run Prompt

## Purpose
Run the data collection service to collect historical daily data for all stocks in the latest enriched YFinance dataset. This prompt will ask for date input before execution.

## Interactive Date Input Process

### Step 1: Ask for Date Input
Please provide the date(s) for data collection:
- **Single date**: Enter one date (e.g., "2025-09-16") to use for both start and end
- **Date range**: Enter two dates separated by space (e.g., "2025-09-01 2025-09-16")
- **Format**: YYYY-MM-DD

### Step 2: Date Validation
The system will:
1. Parse the input date(s)
2. Validate the format
3. Confirm the date range with you before execution

## Execution Script

```python
# run_data_collection_with_dates.py
import asyncio
import json
from datetime import datetime
import sys
import os
sys.path.append('/workspaces/data-collection-service')

from src.services.data_coordinator import DataCollectionCoordinator

async def run_collection():
    # Get date input
    date_input = input("📅 Enter date(s) for collection (YYYY-MM-DD or YYYY-MM-DD YYYY-MM-DD): ").strip()

    # Parse dates
    dates = date_input.split()
    if len(dates) == 1:
        start_date = end_date = dates[0]
        print(f"✅ Using single date: {start_date}")
    elif len(dates) == 2:
        start_date, end_date = dates
        print(f"✅ Using date range: {start_date} to {end_date}")
    else:
        print("❌ Invalid input. Please provide 1 or 2 dates in YYYY-MM-DD format")
        return

    # Validate date format
    try:
        datetime.strptime(start_date, "%Y-%m-%d")
        datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError:
        print("❌ Invalid date format. Please use YYYY-MM-DD")
        return

    # Confirm with user
    print(f"\n🔄 Ready to collect data for:")
    print(f"   Start Date: {start_date}")
    print(f"   End Date: {end_date}")
    confirm = input("   Proceed? (y/n): ").strip().lower()

    if confirm != 'y':
        print("❌ Collection cancelled")
        return

    print("\n" + "="*60)
    print(f"🚀 STARTING DATA COLLECTION FOR {start_date} to {end_date}")
    print("="*60)

    # Initialize coordinator
    coordinator = DataCollectionCoordinator(
        use_yfinance_input=True,
        use_enriched_fundamentals=True
    )

    # Get all tickers from enriched data
    print("📊 Fetching all tickers from YFinance enriched data...")
    tickers = await coordinator.yfinance_input_service.fetch_active_tickers()
    print(f"✅ Found {len(tickers)} tickers")

    if len(tickers) > 10:
        print(f"📝 First 10 tickers: {tickers[:10]}")
        print(f"📝 Last 10 tickers: {tickers[-10:]}")

    print("\n🔧 Configuration:")
    print("   ✅ Using enriched YFinance data for fundamentals")
    print("   ✅ No market cap filtering (pre-filtered in enriched data)")
    print("   ✅ Technical indicator validation enabled")
    print("   ✅ Error rate monitoring enabled")

    print(f"\n⚙️  Starting collection for {len(tickers)} tickers...")

    # Run collection
    collection_result = await coordinator.collect_multiple_tickers(
        tickers=tickers,
        start_date=start_date,
        end_date=end_date
    )

    # Print summary
    print("\n" + "="*60)
    print("📊 COLLECTION SUMMARY")
    print("="*60)

    if collection_result and 'job_id' in collection_result:
        print(f"✅ Job ID: {collection_result['job_id']}")
        print(f"📈 Status: {collection_result.get('status', 'Unknown')}")

        if 'summary' in collection_result:
            summary = collection_result['summary']
            print(f"📊 Tickers Processed: {summary.get('total_tickers', 0)}")
            print(f"✅ Successful: {summary.get('successful_tickers', 0)}")
            print(f"⚠️  Partial: {summary.get('partial_tickers', 0)}")
            print(f"❌ Failed: {summary.get('failed_tickers', 0)}")

            if 'success_rate' in summary:
                print(f"📈 Success Rate: {summary['success_rate']:.1f}%")

    print("\n✅ Data collection complete!")
    print(f"📁 Data saved to: /workspaces/data/historical/daily/*/")

    # Show date-specific paths
    date_parts = start_date.split('-')
    year, month = date_parts[0], date_parts[1]
    print(f"📂 Check data at: /workspaces/data/historical/daily/*/{year}/{month}/")

if __name__ == "__main__":
    asyncio.run(run_collection())
```

## Command Options

### Option 1: Interactive Mode (Recommended)
```bash
python run_data_collection_with_dates.py
```
Then enter date(s) when prompted.

### Option 2: Direct Execution with Pre-set Dates
For automation or repeated runs, you can modify the script to accept command-line arguments:

```bash
# Single date
python run_data_collection_with_dates.py --date 2025-09-16

# Date range
python run_data_collection_with_dates.py --start 2025-09-01 --end 2025-09-16
```

## Expected Output

```
📅 Enter date(s) for collection (YYYY-MM-DD or YYYY-MM-DD YYYY-MM-DD): 2025-09-16
✅ Using single date: 2025-09-16

🔄 Ready to collect data for:
   Start Date: 2025-09-16
   End Date: 2025-09-16
   Proceed? (y/n): y

============================================================
🚀 STARTING DATA COLLECTION FOR 2025-09-16 to 2025-09-16
============================================================
📊 Fetching all tickers from YFinance enriched data...
✅ Found 1281 tickers
📝 First 10 tickers: ['TSM', 'ORCL', 'LLY', 'WMT', 'V', 'MA', 'XOM', 'JNJ', 'ABBV', 'HD']
📝 Last 10 tickers: ['CIMN', 'DRD', 'SID', 'BTE', 'CTRI', 'SVV']

🔧 Configuration:
   ✅ Using enriched YFinance data for fundamentals
   ✅ No market cap filtering (pre-filtered in enriched data)
   ✅ Technical indicator validation enabled
   ✅ Error rate monitoring enabled

⚙️  Starting collection for 1281 tickers...
[Progress updates...]

============================================================
📊 COLLECTION SUMMARY
============================================================
✅ Job ID: [UUID]
📈 Status: completed
📊 Tickers Processed: 1281
✅ Successful: 1281
⚠️  Partial: 0
❌ Failed: 0
📈 Success Rate: 100.0%

✅ Data collection complete!
📁 Data saved to: /workspaces/data/historical/daily/*/
📂 Check data at: /workspaces/data/historical/daily/*/2025/09/
```

## Monitoring Progress

During execution, you can monitor:
1. **Real-time progress**: Watch the ticker counter (e.g., "Processing ticker 500/1281")
2. **Log output**: Check for any errors or warnings
3. **File creation**: Monitor `/workspaces/data/historical/daily/` for new files

## Post-Collection Verification

After collection completes:

```bash
# Count files created for the date
find /workspaces/data/historical/daily -name "2025-09-16.json" | wc -l

# Check a sample file
python3 -c "
import json
sample = json.load(open('/workspaces/data/historical/daily/AAPL/2025/09/2025-09-16.json'))
print(f'Records in file: {len(sample)}')
print(f'First record date: {sample[0][\"date\"]}')
print(f'Data completeness: {sample[0][\"data_completeness_score\"]}')
"
```

## Important Notes

1. **Processing Time**: Expect ~15-25 minutes for all 1281 stocks for a single day
2. **Rate Limiting**: The system respects API rate limits automatically
3. **Error Handling**: Failed tickers are logged and can be retried
4. **Storage**: Each day's data creates ~1281 JSON files (~5-10MB total)
5. **Fundamentals**: Uses enriched data (<24 hours old) to avoid API calls

## Troubleshooting

If collection fails:
1. Check network connectivity
2. Verify enriched data exists and is <24 hours old
3. Check disk space in `/workspaces/data/`
4. Review logs for specific error messages
5. Retry failed tickers only (check job summary for list)