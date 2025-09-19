#!/usr/bin/env python3
"""
Run data collection for 2025-09-16 - Simple version
"""

import asyncio
import json
from datetime import datetime
import sys
import os

# Add paths
sys.path.append('/workspaces/data-collection-service')
os.chdir('/workspaces/data-collection-service')

# Import from working example
from src.services.data_collector import DataCollectionCoordinator

async def main():
    """Run data collection for 2025-09-16"""

    start_date = end_date = "2025-09-16"

    print("\n" + "="*60)
    print(f"🚀 STARTING DATA COLLECTION FOR {start_date}")
    print("="*60)

    # Initialize coordinator with enriched data
    coordinator = DataCollectionCoordinator(
        use_yfinance_input=True,
        use_enriched_fundamentals=True
    )

    # Get all tickers
    print("📊 Fetching all tickers from YFinance enriched data...")
    tickers = await coordinator.yfinance_input_service.fetch_active_tickers()
    print(f"✅ Found {len(tickers)} tickers")

    if len(tickers) > 10:
        print(f"📝 First 10 tickers: {tickers[:10]}")
        print(f"📝 Last 10 tickers: {tickers[-10:]}")

    print("\n🔧 Configuration:")
    print("   ✅ Using enriched YFinance data for fundamentals")
    print("   ✅ No market cap filtering")
    print("   ✅ Technical indicator validation enabled")

    print(f"\n⚙️  Starting collection for {len(tickers)} tickers...")

    # Run collection
    result = await coordinator.collect_multiple_tickers(
        tickers=tickers,
        start_date=start_date,
        end_date=end_date
    )

    # Summary
    print("\n" + "="*60)
    print("📊 COLLECTION SUMMARY")
    print("="*60)

    if result and 'job_id' in result:
        print(f"✅ Job ID: {result['job_id']}")
        print(f"📈 Status: {result.get('status', 'Unknown')}")

        if 'summary' in result:
            summary = result['summary']
            print(f"📊 Total: {summary.get('total_tickers', 0)}")
            print(f"✅ Successful: {summary.get('successful_tickers', 0)}")
            print(f"⚠️  Partial: {summary.get('partial_tickers', 0)}")
            print(f"❌ Failed: {summary.get('failed_tickers', 0)}")

            if 'success_rate' in summary:
                print(f"📈 Success Rate: {summary['success_rate']:.1f}%")

    print("\n✅ Complete!")
    print(f"📁 Data saved to: /workspaces/data/historical/daily/*/2025/09/2025-09-16.json")

if __name__ == "__main__":
    asyncio.run(main())