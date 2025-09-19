#!/usr/bin/env python3
"""
Run fresh data collection for top tickers from YFinance enriched data
with all improvements applied.
"""

import asyncio
import json
import structlog
from datetime import datetime, timedelta
from src.services.data_collector import DataCollectionCoordinator
from src.services.google_sheets_service import GoogleSheetsService
from src.services.yfinance_input_service import YFinanceInputService

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.dev.ConsoleRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()


async def run_fresh_collection(use_yfinance_input=True, ticker_limit=100):
    """Run fresh data collection for top tickers from YFinance enriched data or Google Sheets.

    Args:
        use_yfinance_input: If True, use YFinance enriched data. If False, use Google Sheets.
        ticker_limit: Maximum number of tickers to process
    """

    print("\n" + "="*60)
    print("🚀 STARTING FRESH DATA COLLECTION WITH ALL IMPROVEMENTS")
    print("="*60)

    # Calculate date range (today only: 2025-09-10)
    end_date = "2025-09-10"
    start_date = "2025-09-10"

    print(f"📅 Date range: {start_date} to {end_date}")

    # Initialize services
    coordinator = DataCollectionCoordinator(use_yfinance_input=use_yfinance_input)

    # Get tickers from the appropriate source
    if use_yfinance_input:
        yfinance_service = YFinanceInputService()
        print("📊 Fetching tickers from YFinance enriched data...")

        # Get summary stats first
        stats = await yfinance_service.get_summary_stats()
        if stats:
            print(f"📈 Data source summary:")
            print(f"   Total stocks available: {stats.get('total_stocks', 0)}")
            print(f"   Mega Cap (>$200B): {stats.get('mega_cap_count', 0)}")
            print(f"   Large Cap ($10B-$200B): {stats.get('large_cap_count', 0)}")
            print(f"   Mid Cap ($2B-$10B): {stats.get('mid_cap_count', 0)}")
            print(f"   Data file: {stats.get('file_path', 'N/A')}")

        # Fetch tickers with market cap filter
        tickers = await yfinance_service.fetch_active_tickers(
            limit=ticker_limit,
            min_market_cap=2_000_000_000  # $2B minimum
        )
        print(f"✅ Found {len(tickers)} tickers with market cap > $2B")
    else:
        sheets_service = GoogleSheetsService()
        print("📊 Fetching tickers from Google Sheets...")
        tickers = await sheets_service.fetch_active_tickers()
        print(f"✅ Found {len(tickers)} tickers")
    
    # Display tickers
    print("\n📝 Tickers to collect:")
    for i in range(0, len(tickers), 10):
        print(f"   {', '.join(tickers[i:i+10])}")
    
    print("\n🔧 Improvements applied:")
    print("   ✅ Fixed dividend yield calculation")
    print("   ✅ Chronological data ordering enforced")
    print("   ✅ Robust file writing with JSON validation")
    print("   ✅ Technical indicator validation with bounds checking")
    print("   ✅ Data completeness scoring")
    print("   ✅ Error rate monitoring with alerts")
    print("   ✅ Invalid records moved to error_records")
    
    print("\n⚙️  Starting collection...")
    
    # Run the collection
    job_result = await coordinator.collect_multiple_tickers(
        tickers=tickers,
        start_date=start_date,
        end_date=end_date
    )
    
    # Save job metadata
    job_id = getattr(job_result, 'job_id', 'unknown')
    job_file = f"/workspaces/data/jobs/{job_id}/metadata.json"
    
    print(f"\n✅ Collection started!")
    print(f"📋 Job ID: {job_id}")
    print(f"📁 Job file: {job_file}")
    print(f"🎯 Status: {getattr(job_result, 'status', 'unknown')}")
    
    # Monitor progress
    print("\n📊 Initial statistics:")
    print(f"   - Tickers to process: {len(tickers)}")
    print(f"   - Date range: Single day (2025-09-10)")
    print(f"   - Expected records: ~{len(tickers)} (1 trading day)")
    
    return job_result


async def monitor_job(job_id):
    """Monitor the job progress."""
    
    import time
    import os
    
    job_file = f"/workspaces/data/jobs/{job_id}/metadata.json"
    
    print(f"\n⏳ Monitoring job: {job_id}")
    print("Press Ctrl+C to stop monitoring (collection will continue)")
    
    last_status = None
    while True:
        try:
            if os.path.exists(job_file):
                with open(job_file, 'r') as f:
                    job_data = json.load(f)
                
                status = job_data.get("job_status", "unknown")
                processed = job_data.get("tickers_processed", 0)
                successful = job_data.get("successful_tickers", 0)
                failed = job_data.get("failed_tickers", 0)
                records = job_data.get("successful_records", 0)
                
                if status != last_status or processed % 10 == 0:
                    print(f"\r📊 Progress: {processed}/{len(job_data.get('tickers', []))} tickers | "
                          f"✅ {successful} successful | ❌ {failed} failed | "
                          f"📝 {records} records | Status: {status}", end="")
                    last_status = status
                
                if status in ["completed", "failed"]:
                    print(f"\n\n🎉 Job {status}!")
                    print(f"Final stats:")
                    print(f"  - Successful tickers: {successful}")
                    print(f"  - Failed tickers: {failed}")
                    print(f"  - Total records: {records}")
                    print(f"  - Error records: {job_data.get('failed_records', 0)}")
                    break
            
            await asyncio.sleep(5)
            
        except KeyboardInterrupt:
            print("\n\n⚠️  Monitoring stopped. Collection continues in background.")
            break
        except Exception as e:
            print(f"\nError monitoring: {e}")
            await asyncio.sleep(5)


async def main():
    """Main function to run the collection."""

    # Configuration - use YFinance input by default
    use_yfinance_input = True  # Set to False to use Google Sheets
    ticker_limit = 100  # Number of tickers to process

    # Start the collection
    job_result = await run_fresh_collection(
        use_yfinance_input=use_yfinance_input,
        ticker_limit=ticker_limit
    )

    # Monitor if job started successfully
    if job_result and hasattr(job_result, 'job_id'):
        await monitor_job(job_result.job_id)
    else:
        print("\n❌ Failed to start collection")
        print(f"Result: {job_result}")


if __name__ == "__main__":
    asyncio.run(main())