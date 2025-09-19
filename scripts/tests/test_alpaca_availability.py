#!/usr/bin/env python3
"""
Quick test for Alpaca API data availability for 2025-09-16
"""

import os
import sys
from datetime import datetime
sys.path.append('/workspaces/data-collection-service/src')

from services.alpaca_service import AlpacaService

def test_alpaca_availability():
    """Test if Alpaca API can now access 2025-09-16 data"""

    print("=" * 60)
    print("🔍 TESTING ALPACA API AVAILABILITY FOR 2025-09-16")
    print("=" * 60)

    # Initialize Alpaca service
    alpaca_service = AlpacaService()

    # Test with a few representative tickers
    test_tickers = ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'NVDA']

    results = {}

    for ticker in test_tickers:
        print(f"\n📊 Testing {ticker}...")

        try:
            # Test current day data (2025-09-16)
            import asyncio
            bars = asyncio.run(alpaca_service.get_daily_bars(
                ticker=ticker,
                start_date="2025-09-16",
                end_date="2025-09-16"
            ))

            if bars and len(bars) > 0:
                print(f"✅ {ticker}: SUCCESS - Retrieved {len(bars)} bars")
                results[ticker] = {
                    'status': 'success',
                    'bars_count': len(bars),
                    'data': bars[0] if bars else None
                }
            else:
                print(f"⚠️  {ticker}: NO DATA - Empty response")
                results[ticker] = {
                    'status': 'no_data',
                    'bars_count': 0,
                    'data': None
                }

        except Exception as e:
            error_msg = str(e)
            print(f"❌ {ticker}: ERROR - {error_msg}")
            results[ticker] = {
                'status': 'error',
                'error': error_msg,
                'bars_count': 0
            }

    # Summary
    print("\n" + "=" * 60)
    print("📋 SUMMARY RESULTS")
    print("=" * 60)

    success_count = sum(1 for r in results.values() if r['status'] == 'success')
    error_count = sum(1 for r in results.values() if r['status'] == 'error')
    no_data_count = sum(1 for r in results.values() if r['status'] == 'no_data')

    print(f"✅ Successful: {success_count}/{len(test_tickers)}")
    print(f"❌ Errors: {error_count}/{len(test_tickers)}")
    print(f"⚠️  No Data: {no_data_count}/{len(test_tickers)}")

    # Check if we still get the SIP error
    sip_errors = [ticker for ticker, result in results.items()
                  if result['status'] == 'error' and 'SIP' in result.get('error', '')]

    if sip_errors:
        print(f"\n🚨 SIP Restriction Still Active:")
        for ticker in sip_errors:
            print(f"   • {ticker}: {results[ticker]['error']}")

    # Current time info
    current_time = datetime.now()
    print(f"\n🕐 Test Time: {current_time}")
    print(f"🗓️  Target Date: 2025-09-16")

    return results

if __name__ == "__main__":
    results = test_alpaca_availability()