#!/usr/bin/env python3
"""
Demo script showing how to use the data collection service with YFinance input source.
"""

import asyncio
from datetime import datetime, timedelta
from src.services.data_collector import DataCollectionCoordinator
from src.services.yfinance_input_service import YFinanceInputService


async def demo_collection():
    """Demonstrate the data collection with YFinance input source."""

    print("\n" + "=" * 70)
    print("🚀 DATA COLLECTION SERVICE - YFinance Input Source Demo")
    print("=" * 70)

    # Initialize the YFinance input service
    yfinance_input = YFinanceInputService()

    # Show available data
    print("\n📊 Available Data Source:")
    stats = await yfinance_input.get_summary_stats()
    if stats:
        print(f"   File: {stats.get('file_path', 'N/A')}")
        print(f"   Total stocks: {stats.get('total_stocks', 0)}")
        print(f"   Mega Cap (>$200B): {stats.get('mega_cap_count', 0)}")
        print(f"   Large Cap ($10B-$200B): {stats.get('large_cap_count', 0)}")
        print(f"   Mid Cap ($2B-$10B): {stats.get('mid_cap_count', 0)}")

    # Fetch top 10 tickers for demo
    print("\n🎯 Fetching top 10 stocks by market cap...")
    tickers = await yfinance_input.fetch_active_tickers(limit=10)
    print(f"   Selected tickers: {', '.join(tickers)}")

    # Show metadata for selected tickers
    print("\n📋 Stock Details:")
    for ticker in tickers[:5]:  # Show first 5 for brevity
        metadata = await yfinance_input.get_ticker_metadata(ticker)
        if metadata:
            market_cap_b = metadata.get('market_cap', 0) / 1_000_000_000
            print(f"   {ticker:6} - ${market_cap_b:,.1f}B - {metadata.get('sector', 'N/A'):20} - {metadata.get('name', 'N/A')[:30]}")

    # Initialize the data collection coordinator
    print("\n🔧 Initializing Data Collection Service...")
    coordinator = DataCollectionCoordinator(use_yfinance_input=True)

    # Set date range for collection (last 30 days)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)

    print(f"\n📅 Collection Configuration:")
    print(f"   Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print(f"   Tickers to collect: {len(tickers)}")
    print(f"   Input source: YFinance enriched data")

    # Example: Collect data for the first ticker
    print(f"\n💫 Example: Collecting data for {tickers[0]}...")

    try:
        result = await coordinator.collect_ticker_data(
            ticker=tickers[0],
            start_date=start_date.strftime('%Y-%m-%d'),
            end_date=end_date.strftime('%Y-%m-%d'),
            include_technical_indicators=True,
            include_fundamentals=True
        )

        print(f"\n✅ Collection Result for {tickers[0]}:")
        print(f"   Status: {result.get('status', 'unknown')}")
        print(f"   Records collected: {result.get('records_collected', 0)}")
        print(f"   Records saved: {result.get('records_saved', 0)}")
        print(f"   Technical indicators: {result.get('technical_indicators_calculated', 0)}")
        print(f"   Fundamentals added: {result.get('fundamentals_added', 0)}")

        if result.get('error_message'):
            print(f"   ⚠️ Error: {result['error_message']}")

    except Exception as e:
        print(f"\n❌ Error during collection: {e}")

    # Demonstrate batch collection
    print(f"\n🚀 Ready to collect data for all {len(tickers)} tickers")
    print("   Use run_fresh_collection.py for full batch collection")

    # Show configuration example
    print("\n📝 Configuration Example:")
    print("   To use YFinance input source in your code:")
    print("   ```python")
    print("   # Initialize coordinator with YFinance input")
    print("   coordinator = DataCollectionCoordinator(use_yfinance_input=True)")
    print("   ")
    print("   # Or in run_fresh_collection.py, set:")
    print("   use_yfinance_input = True")
    print("   ticker_limit = 100  # Number of top stocks to collect")
    print("   ```")

    print("\n" + "=" * 70)
    print("✅ Demo completed successfully!")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(demo_collection())