#!/usr/bin/env python3
"""
Test script to verify YFinance input service is working correctly.
"""

import asyncio
import json
from src.services.yfinance_input_service import YFinanceInputService


async def test_yfinance_input():
    """Test the YFinance input service."""

    print("=" * 60)
    print("🧪 TESTING YFINANCE INPUT SERVICE")
    print("=" * 60)

    # Initialize the service
    service = YFinanceInputService()

    # Test 1: Find the latest file
    print("\n📁 Test 1: Finding latest enriched file...")
    latest_file = service.get_latest_enriched_file()
    if latest_file:
        print(f"✅ Found file: {latest_file}")
    else:
        print("❌ No enriched file found")
        return

    # Test 2: Validate connection
    print("\n🔌 Test 2: Validating connection...")
    is_valid = await service.validate_connection()
    print(f"{'✅' if is_valid else '❌'} Connection valid: {is_valid}")

    # Test 3: Get summary statistics
    print("\n📊 Test 3: Getting summary statistics...")
    stats = await service.get_summary_stats()
    if stats:
        print(f"✅ Summary stats retrieved:")
        print(f"   Total stocks: {stats.get('total_stocks', 0)}")
        print(f"   Mega Cap (>$200B): {stats.get('mega_cap_count', 0)}")
        print(f"   Large Cap ($10B-$200B): {stats.get('large_cap_count', 0)}")
        print(f"   Mid Cap ($2B-$10B): {stats.get('mid_cap_count', 0)}")

        # Show top 5 by market cap
        top_stocks = stats.get('top_10_by_market_cap', [])[:5]
        if top_stocks:
            print(f"\n   Top 5 stocks by market cap:")
            for i, stock in enumerate(top_stocks, 1):
                market_cap_b = stock.get('market_cap', 0) / 1_000_000_000
                print(f"   {i}. {stock.get('ticker', 'N/A'):6} - ${market_cap_b:,.1f}B - {stock.get('name', 'N/A')[:40]}")
    else:
        print("❌ Failed to get summary stats")

    # Test 4: Fetch tickers with various filters
    print("\n🎯 Test 4: Fetching tickers with filters...")

    # Test 4a: Top 10 stocks
    print("\n   4a. Top 10 stocks by market cap:")
    tickers = await service.fetch_active_tickers(limit=10)
    print(f"   ✅ Found {len(tickers)} tickers: {', '.join(tickers)}")

    # Test 4b: Stocks with >$100B market cap
    print("\n   4b. Stocks with market cap > $100B:")
    tickers = await service.fetch_active_tickers(min_market_cap=100_000_000_000)
    print(f"   ✅ Found {len(tickers)} tickers")
    if tickers:
        print(f"   First 10: {', '.join(tickers[:10])}")

    # Test 4c: Mid-cap stocks ($2B-$10B)
    print("\n   4c. Mid-cap stocks ($2B-$10B):")
    tickers = await service.fetch_active_tickers(
        min_market_cap=2_000_000_000,
        max_market_cap=10_000_000_000,
        limit=10
    )
    print(f"   ✅ Found {len(tickers)} tickers: {', '.join(tickers)}")

    # Test 4d: Tech sector stocks
    print("\n   4d. Technology sector stocks (top 10):")
    tickers = await service.fetch_active_tickers(
        sectors=['Technology'],
        limit=10
    )
    print(f"   ✅ Found {len(tickers)} tickers: {', '.join(tickers)}")

    # Test 5: Get metadata for a specific ticker
    print("\n📋 Test 5: Getting metadata for specific ticker (AAPL)...")
    metadata = await service.get_ticker_metadata('AAPL')
    if metadata:
        print(f"✅ Metadata retrieved:")
        for key, value in metadata.items():
            if key == 'market_cap':
                value = f"${value/1_000_000_000:,.1f}B" if value else 'N/A'
            print(f"   {key}: {value}")
    else:
        print("❌ No metadata found for AAPL")

    print("\n" + "=" * 60)
    print("✅ All tests completed!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(test_yfinance_input())