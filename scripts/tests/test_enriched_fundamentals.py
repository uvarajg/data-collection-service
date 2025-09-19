#!/usr/bin/env python3
"""
Test script to verify the enriched fundamentals service integration
"""

import asyncio
import sys
import os

# Add the src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.services.data_collector import DataCollectionCoordinator
from src.services.enriched_fundamentals_service import EnrichedFundamentalsService

async def main():
    print("Testing Enriched Fundamentals Service Integration")
    print("=" * 50)

    # Test 1: Direct EnrichedFundamentalsService
    print("\n1. Testing EnrichedFundamentalsService directly")
    fundamentals_service = EnrichedFundamentalsService()

    # Check service stats
    stats = fundamentals_service.get_stats()
    print(f"Service Stats: {stats}")

    if not stats['loaded']:
        print("❌ No enriched data loaded")
        return

    # Test a few tickers
    test_tickers = ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'AMZN']

    for ticker in test_tickers[:3]:  # Test first 3 only
        try:
            fundamentals = await fundamentals_service.get_fundamentals(ticker)
            if fundamentals:
                print(f"✅ {ticker}: Found fundamentals (Market Cap: ${fundamentals.market_cap:.0f}M, P/E: {fundamentals.pe_ratio})")
            else:
                print(f"❌ {ticker}: No fundamentals found")
        except Exception as e:
            print(f"❌ {ticker}: Error - {e}")

    # Test 2: DataCollectionCoordinator with enriched fundamentals
    print("\n2. Testing DataCollectionCoordinator with enriched fundamentals")
    collector = DataCollectionCoordinator(use_enriched_fundamentals=True)

    # Test fetching ticker list (should return all tickers without filters)
    print("Fetching ticker list...")

    if collector.use_yfinance_input and collector.yfinance_input_service:
        tickers = await collector.yfinance_input_service.fetch_active_tickers()
        print(f"✅ Fetched {len(tickers)} tickers from enriched data")

        if tickers:
            print(f"Sample tickers: {tickers[:10]}")

        # Test getting metadata for a ticker
        if tickers:
            test_ticker = tickers[0]
            metadata = await collector.yfinance_input_service.get_ticker_metadata(test_ticker)
            if metadata:
                print(f"✅ {test_ticker} metadata: {metadata}")
    else:
        print("❌ YFinance input service not configured")

    # Test 3: Small collection run
    print("\n3. Testing small data collection run")

    # Get a few tickers for testing
    if collector.use_yfinance_input and collector.yfinance_input_service:
        small_ticker_list = await collector.yfinance_input_service.fetch_active_tickers(limit=3)

        if small_ticker_list:
            print(f"Testing collection for: {small_ticker_list}")

            # Simulate the collection process without saving
            for ticker in small_ticker_list:
                try:
                    # Test fundamentals retrieval
                    fundamentals = await collector.fundamentals_service.get_fundamentals(ticker)

                    if fundamentals:
                        print(f"✅ {ticker}: Retrieved fundamentals successfully")
                        print(f"   Market Cap: ${fundamentals.market_cap:.0f}M")
                        print(f"   P/E Ratio: {fundamentals.pe_ratio}")
                        print(f"   ROE: {fundamentals.roe_percent}%")
                    else:
                        print(f"⚠️  {ticker}: No fundamentals available")

                except Exception as e:
                    print(f"❌ {ticker}: Error retrieving fundamentals - {e}")
        else:
            print("❌ No tickers available for testing")

    print("\n" + "=" * 50)
    print("Test completed!")

if __name__ == "__main__":
    asyncio.run(main())