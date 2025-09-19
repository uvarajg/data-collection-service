#!/usr/bin/env python3
"""
Automated data collection script for daily pipeline
This is designed to be called from daily_pipeline_automation.py
"""

import asyncio
import json
import sys
import os
from datetime import datetime
from pathlib import Path

# Change to the data collection service directory to ensure proper imports
os.chdir('/workspaces/data-collection-service')
sys.path.insert(0, '/workspaces/data-collection-service')

# Now import with the proper context
from src.services.data_collector import DataCollectionCoordinator

async def run_automated_collection(target_date=None):
    """
    Run data collection for the specified date

    Args:
        target_date: Date string in YYYY-MM-DD format. If None, uses yesterday.

    Returns:
        Dictionary with collection results
    """
    from datetime import date, timedelta

    # If no date provided, use yesterday
    if target_date is None:
        yesterday = date.today() - timedelta(days=1)
        target_date = yesterday.strftime('%Y-%m-%d')

    print(f"📅 Running automated collection for: {target_date}")

    try:
        # Initialize coordinator
        coordinator = DataCollectionCoordinator(
            use_yfinance_input=True,
            use_enriched_fundamentals=True
        )

        # Get all tickers from enriched data
        print("📊 Fetching tickers from enriched YFinance data...")
        tickers = await coordinator.yfinance_input_service.fetch_active_tickers()
        print(f"✅ Found {len(tickers)} tickers")

        if len(tickers) > 10:
            print(f"📝 First 10 tickers: {tickers[:10]}")

        # Run collection
        print(f"⚙️  Starting collection for {len(tickers)} tickers...")
        collection_result = await coordinator.collect_multiple_tickers(
            tickers=tickers,
            start_date=target_date,
            end_date=target_date
        )

        # Process and return results
        if collection_result and hasattr(collection_result, 'job_id'):
            summary = {
                'status': 'success',
                'job_id': collection_result.job_id,
                'target_date': target_date,
                'total_tickers': len(tickers),
                'successful_tickers': getattr(collection_result, 'successful_count', 0),
                'failed_tickers': getattr(collection_result, 'failed_count', 0)
            }

            if summary['total_tickers'] > 0:
                summary['success_rate'] = round(
                    (summary['successful_tickers'] / summary['total_tickers']) * 100, 2
                )

            print(f"✅ Collection completed successfully")
            print(f"📊 Success rate: {summary.get('success_rate', 0)}%")

            return summary

        else:
            return {
                'status': 'failed',
                'target_date': target_date,
                'error': 'No valid collection result returned'
            }

    except Exception as e:
        print(f"❌ Collection failed: {e}")
        return {
            'status': 'error',
            'target_date': target_date,
            'error': str(e)
        }

def main():
    """Main entry point for command line execution"""
    # Check for command line argument
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
    else:
        from datetime import date, timedelta
        # Use previous business day
        today = date.today()
        if today.weekday() == 0:  # Monday
            target_date = (today - timedelta(days=3)).strftime('%Y-%m-%d')
        else:
            target_date = (today - timedelta(days=1)).strftime('%Y-%m-%d')

    print("=" * 60)
    print(f"🚀 AUTOMATED DATA COLLECTION FOR {target_date}")
    print("=" * 60)

    # Run the collection
    result = asyncio.run(run_automated_collection(target_date))

    # Output result as JSON for the pipeline to parse
    print("\n" + "=" * 60)
    print("📊 COLLECTION RESULT:")
    print("=" * 60)
    print(json.dumps(result, indent=2))

    # Exit with appropriate code
    sys.exit(0 if result.get('status') == 'success' else 1)

if __name__ == "__main__":
    main()