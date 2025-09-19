#!/usr/bin/env python3
"""
Run data collection for all stocks in enriched YFinance dataset for 2025-09-15
"""

import asyncio
import sys
import os

# Add the src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.services.data_collector import DataCollectionCoordinator

async def main():
    print("=" * 60)
    print("🚀 STARTING FULL DATA COLLECTION FOR 2025-09-15")
    print("=" * 60)

    # Initialize coordinator with enriched fundamentals
    coordinator = DataCollectionCoordinator(
        use_yfinance_input=True,
        use_enriched_fundamentals=True
    )

    # Define date range for September 15, 2025
    start_date = "2025-09-15"
    end_date = "2025-09-15"

    print(f"📅 Date range: {start_date} to {end_date}")

    # Fetch all tickers from enriched data (no limit)
    print("📊 Fetching all tickers from YFinance enriched data...")

    tickers = await coordinator.yfinance_input_service.fetch_active_tickers()

    print(f"✅ Found {len(tickers)} tickers")
    print(f"📝 First 10 tickers: {tickers[:10]}")
    print(f"📝 Last 10 tickers: {tickers[-10:]}")

    print("\n🔧 Configuration:")
    print("   ✅ Using enriched YFinance data for fundamentals")
    print("   ✅ No market cap filtering (pre-filtered in enriched data)")
    print("   ✅ Technical indicator validation enabled")
    print("   ✅ Error rate monitoring enabled")

    # Start collection
    print(f"\n⚙️  Starting collection for {len(tickers)} tickers...")

    collection_result = await coordinator.collect_multiple_tickers(
        tickers=tickers,
        start_date=start_date,
        end_date=end_date
    )

    # Print results
    print("\n" + "=" * 60)
    print("📊 COLLECTION RESULTS")
    print("=" * 60)

    print(f"✅ Job ID: {collection_result.job_id}")
    print(f"📁 Job status: {collection_result.job_status}")
    print(f"📈 Total tickers processed: {len(tickers)}")
    print(f"✅ Successful records: {collection_result.successful_records}")
    print(f"❌ Failed records: {collection_result.failed_records}")
    print(f"📊 Total records: {collection_result.total_records}")

    if collection_result.error_summary:
        print("\n⚠️  Error Summary:")
        error_count = 0
        for ticker, error in collection_result.error_summary.items():
            if error_count < 10:  # Show first 10 errors
                print(f"   - {ticker}: {error}")
                error_count += 1
        if error_count < len(collection_result.error_summary):
            print(f"   ... and {len(collection_result.error_summary) - error_count} more errors")

    # Check for successful data
    if collection_result.successful_records > 0:
        print(f"\n✅ Successfully collected {collection_result.successful_records} records")
        print(f"📁 Data saved in: /workspaces/data/historical/daily/*/2025/09/")
    else:
        print(f"\n⚠️  No successful records collected")

    # Check for error records
    print(f"\n📊 Records moved to error_records: {collection_result.failed_records}")
    if collection_result.failed_records > 0:
        print(f"📁 Error records saved in: /workspaces/data/error_records/")

    print("\n" + "=" * 60)
    print("✅ COLLECTION COMPLETE!")
    print("=" * 60)

if __name__ == "__main__":
    asyncio.run(main())