#!/usr/bin/env python3
"""
Collect YFinance data for filtered stocks in smaller batches
This can be run multiple times to handle interruptions
"""

import json
import yfinance as yf
from datetime import datetime
import sys
import os

def load_filtered_stocks():
    """Load the most recent Set C filtered stocks"""
    input_dir = "/workspaces/data/input_source"

    # Find the most recent filtered file or use enriched data
    import glob
    files = glob.glob(f"{input_dir}/enriched_yfinance_*.json")
    files += glob.glob(f"{input_dir}/enriched_yfinance_*.json.gz")
    # Fallback to old naming
    files += glob.glob(f"{input_dir}/set_c_filtered_2b_*.json")

    if not files:
        print("❌ No filtered stock files found!")
        return None

    latest_file = max(files)
    print(f"📂 Loading stocks from: {latest_file}")

    with open(latest_file, 'r') as f:
        stocks = json.load(f)

    return stocks

def get_yfinance_data(ticker):
    """Get comprehensive YFinance data for a single ticker"""
    try:
        yf_ticker = yf.Ticker(ticker)
        info = yf_ticker.info

        # Return a simplified version with key metrics
        return {
            'ticker': ticker,
            'yf_fetched': True,
            'yf_timestamp': datetime.now().isoformat(),

            # Basic info
            'name': info.get('longName', ''),
            'sector': info.get('sector', ''),
            'industry': info.get('industry', ''),
            'country': info.get('country', ''),
            'website': info.get('website', ''),

            # Market data
            'market_cap': info.get('marketCap', 0),
            'enterprise_value': info.get('enterpriseValue', 0),
            'current_price': info.get('currentPrice', info.get('regularMarketPrice', 0)),
            'pe_ratio': info.get('trailingPE', 0),
            'forward_pe': info.get('forwardPE', 0),
            'peg_ratio': info.get('pegRatio', 0),
            'price_to_book': info.get('priceToBook', 0),

            # Financials
            'revenue': info.get('totalRevenue', 0),
            'profit_margins': info.get('profitMargins', 0),
            'gross_margins': info.get('grossMargins', 0),
            'ebitda': info.get('ebitda', 0),
            'earnings_growth': info.get('earningsGrowth', 0),

            # Dividends
            'dividend_yield': info.get('dividendYield', 0),
            'payout_ratio': info.get('payoutRatio', 0),

            # Other metrics
            'beta': info.get('beta', 0),
            'return_on_equity': info.get('returnOnEquity', 0),
            'debt_to_equity': info.get('debtToEquity', 0),
            'current_ratio': info.get('currentRatio', 0),

            # Volume
            'volume': info.get('volume', 0),
            'average_volume': info.get('averageVolume', 0),

            # 52-week range
            '52_week_low': info.get('fiftyTwoWeekLow', 0),
            '52_week_high': info.get('fiftyTwoWeekHigh', 0),
        }
    except Exception as e:
        return {
            'ticker': ticker,
            'yf_fetched': False,
            'error': str(e)[:100]
        }

def main():
    print("\n" + "="*60)
    print("YFinance Data Collector - Batch Processing")
    print("="*60)

    # Load filtered stocks
    stocks = load_filtered_stocks()
    if not stocks:
        return

    print(f"📊 Total stocks to process: {len(stocks)}")

    # Process first 100 stocks as a test
    batch_size = 100
    test_batch = stocks[:batch_size]

    print(f"\n🚀 Processing first {batch_size} stocks as test...")

    results = []
    success_count = 0

    for i, stock in enumerate(test_batch, 1):
        ticker = stock['ticker']
        print(f"  [{i}/{batch_size}] Processing {ticker}...", end='')

        yf_data = get_yfinance_data(ticker)

        # Merge original data with YFinance data
        enriched = {**stock, **yf_data}
        results.append(enriched)

        if yf_data.get('yf_fetched'):
            success_count += 1
            print(" ✅")
        else:
            print(f" ❌ {yf_data.get('error', 'Unknown error')[:30]}")

    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"/workspaces/data/input_source/enriched_test_batch_{timestamp}.json"

    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)

    print(f"\n" + "="*60)
    print(f"✅ Test batch complete!")
    print(f"   • Processed: {len(test_batch)} stocks")
    print(f"   • Successful: {success_count} ({success_count/len(test_batch)*100:.1f}%)")
    print(f"   • Failed: {len(test_batch) - success_count}")
    print(f"   • Saved to: {output_file}")
    print("="*60)

if __name__ == "__main__":
    main()