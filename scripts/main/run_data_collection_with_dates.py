#!/usr/bin/env python3
"""
Interactive Data Collection Service Runner
Prompts for date input and runs collection for all stocks in enriched dataset
"""

import asyncio
import json
from datetime import datetime
import sys
import os

# Add project root to path
sys.path.append('/workspaces/data-collection-service')

from src.services.data_collector import DataCollectionCoordinator

async def run_collection(date_input=None, automated=False):
    """Run data collection with date input (interactive or command line)"""

    print("\n" + "="*60)
    print("📊 DATA COLLECTION SERVICE - INTERACTIVE RUNNER")
    print("="*60)

    # Get date input from parameter or interactive prompt
    if date_input is None:
        print("\n📅 Date Input Options:")
        print("   • Single date: Enter one date (e.g., '2025-09-16')")
        print("   • Date range: Enter two dates (e.g., '2025-09-01 2025-09-16')")
        print("   • Format: YYYY-MM-DD\n")

        date_input = input("Enter date(s) for collection: ").strip()

    if not date_input:
        print("❌ No date provided. Exiting...")
        return

    # Parse dates
    dates = date_input.split()

    if len(dates) == 1:
        start_date = end_date = dates[0]
        print(f"\n✅ Using single date: {start_date}")
    elif len(dates) == 2:
        start_date, end_date = dates
        print(f"\n✅ Using date range: {start_date} to {end_date}")
    else:
        print("❌ Invalid input. Please provide 1 or 2 dates in YYYY-MM-DD format")
        return

    # Validate date format
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        # Ensure start <= end
        if start_dt > end_dt:
            print("❌ Start date must be before or equal to end date")
            return

    except ValueError as e:
        print(f"❌ Invalid date format: {e}")
        print("   Please use YYYY-MM-DD format")
        return

    # Calculate number of days
    days_count = (end_dt - start_dt).days + 1

    # Confirm with user
    print("\n" + "-"*60)
    print("🔄 READY TO COLLECT DATA")
    print("-"*60)
    print(f"   📅 Start Date: {start_date}")
    print(f"   📅 End Date:   {end_date}")
    print(f"   📊 Days:       {days_count} day(s)")
    print("-"*60)

    if automated:
        print("🤖 Running in automated mode - proceeding without confirmation")
    else:
        confirm = input("\nProceed with collection? (y/n): ").strip().lower()

        if confirm != 'y':
            print("❌ Collection cancelled by user")
            return

    print("\n" + "="*60)
    print(f"🚀 STARTING DATA COLLECTION")
    print("="*60)

    # Initialize coordinator with enriched data
    print("\n⚙️  Initializing Data Collection Coordinator...")
    coordinator = DataCollectionCoordinator(
        use_yfinance_input=True,
        use_enriched_fundamentals=True
    )

    # Get all tickers from enriched data
    print("📊 Fetching all tickers from YFinance enriched data...")
    try:
        tickers = await coordinator.yfinance_input_service.fetch_active_tickers()
        print(f"✅ Found {len(tickers)} tickers")

        if not tickers:
            print("❌ No tickers found in enriched data")
            return

        # Show sample tickers
        if len(tickers) > 20:
            print(f"\n📝 Sample tickers:")
            print(f"   First 10: {tickers[:10]}")
            print(f"   Last 10:  {tickers[-10:]}")
        else:
            print(f"📝 Tickers: {tickers}")

    except Exception as e:
        print(f"❌ Error fetching tickers: {e}")
        return

    # Show configuration
    print("\n🔧 Configuration:")
    print("   ✅ Using enriched YFinance data for fundamentals")
    print("   ✅ No market cap filtering (pre-filtered in enriched data)")
    print("   ✅ Technical indicator validation enabled")
    print("   ✅ Error rate monitoring enabled")
    print("   ✅ Sequential processing with rate limiting")

    estimated_time = len(tickers) * days_count * 0.5 / 60  # Rough estimate: 0.5 sec per ticker-day
    print(f"\n⏱️  Estimated time: ~{estimated_time:.1f} minutes")
    print(f"⚙️  Starting collection for {len(tickers)} tickers over {days_count} day(s)...")

    # Track start time
    start_time = datetime.now()

    try:
        # Run collection
        collection_result = await coordinator.collect_multiple_tickers(
            tickers=tickers,
            start_date=start_date,
            end_date=end_date
        )

        # Calculate elapsed time
        elapsed = (datetime.now() - start_time).total_seconds()

        # Print summary
        print("\n" + "="*60)
        print("📊 COLLECTION SUMMARY")
        print("="*60)

        if collection_result and 'job_id' in collection_result:
            print(f"✅ Job ID: {collection_result['job_id']}")
            print(f"📈 Status: {collection_result.get('status', 'Unknown')}")

            if 'summary' in collection_result:
                summary = collection_result['summary']
                total = summary.get('total_tickers', 0)
                successful = summary.get('successful_tickers', 0)
                partial = summary.get('partial_tickers', 0)
                failed = summary.get('failed_tickers', 0)

                print(f"\n📊 Tickers Processed: {total}")
                print(f"   ✅ Successful: {successful}")
                print(f"   ⚠️  Partial:    {partial}")
                print(f"   ❌ Failed:     {failed}")

                if 'success_rate' in summary:
                    print(f"\n📈 Success Rate: {summary['success_rate']:.1f}%")

                # Show failed tickers if any
                if failed > 0 and 'failed_ticker_list' in summary:
                    print(f"\n❌ Failed tickers: {summary['failed_ticker_list'][:10]}")
                    if len(summary['failed_ticker_list']) > 10:
                        print(f"   ... and {len(summary['failed_ticker_list']) - 10} more")

        else:
            print("⚠️  No job summary available")

        print(f"\n⏱️  Total time: {elapsed/60:.1f} minutes")
        print(f"⚡ Processing speed: {len(tickers)*days_count/elapsed:.1f} ticker-days/second")

        # Show where data is stored
        print("\n💾 DATA STORAGE LOCATIONS:")
        print(f"📁 Base path: /workspaces/data/historical/daily/")

        # Parse dates for path info
        date_parts = start_date.split('-')
        year, month = date_parts[0], date_parts[1]
        print(f"📂 Date-specific path: /workspaces/data/historical/daily/*/{year}/{month}/")

        if start_date == end_date:
            print(f"📄 Files created: ~{len(tickers)} files named '{start_date}.json'")
        else:
            print(f"📄 Files created: ~{len(tickers) * days_count} files")

        print("\n✅ Data collection complete!")

        # Offer to verify a sample
        print("\n" + "-"*60)
        verify = input("Would you like to verify a sample file? (y/n): ").strip().lower()

        if verify == 'y':
            sample_ticker = tickers[0] if tickers else 'AAPL'
            sample_path = f"/workspaces/data/historical/daily/{sample_ticker}/{year}/{month}/{start_date}.json"

            if os.path.exists(sample_path):
                with open(sample_path, 'r') as f:
                    sample_data = json.load(f)

                print(f"\n📋 Sample file: {sample_path}")
                print(f"   Records: {len(sample_data)}")
                if sample_data:
                    print(f"   Date: {sample_data[0].get('date', 'N/A')}")
                    print(f"   Completeness: {sample_data[0].get('data_completeness_score', 'N/A')}")
                    print(f"   Has fundamentals: {'market_cap' in sample_data[0]}")
                    print(f"   Has indicators: {'rsi' in sample_data[0]}")
            else:
                print(f"⚠️  Sample file not found: {sample_path}")

    except KeyboardInterrupt:
        print("\n\n⚠️  Collection interrupted by user")
        print("   Partial data may have been saved")

    except Exception as e:
        print(f"\n❌ Error during collection: {e}")
        import traceback
        traceback.print_exc()

    finally:
        print("\n" + "="*60)
        print("🏁 COLLECTION RUNNER FINISHED")
        print("="*60)


def main():
    """Main entry point"""
    print("\n🚀 Starting Data Collection Service Runner...")

    # Check if enriched data exists
    import glob
    enriched_files = glob.glob('/workspaces/data/input_source/enriched_yfinance_*.json')

    if not enriched_files:
        print("❌ No enriched YFinance data found!")
        print("   Please run: python collect_us_market_stocks.py")
        return

    # Get latest enriched file
    latest_file = max(enriched_files)
    file_time = os.path.getmtime(latest_file)
    age_hours = (datetime.now().timestamp() - file_time) / 3600

    print(f"📊 Using enriched data: {os.path.basename(latest_file)}")
    print(f"   Age: {age_hours:.1f} hours")

    if age_hours > 24:
        print("⚠️  Warning: Enriched data is >24 hours old")
        print("   Consider running: python collect_us_market_stocks.py")

    # Check for command line arguments
    date_input = None
    automated = False

    if len(sys.argv) >= 3 and sys.argv[1] == '--date':
        date_input = sys.argv[2]
        print(f"📅 Command line date provided: {date_input}")

    # Check for automated flag
    if '--automated' in sys.argv:
        automated = True
        print("🤖 Automated mode enabled")

    # Run async collection
    asyncio.run(run_collection(date_input, automated))


if __name__ == "__main__":
    main()