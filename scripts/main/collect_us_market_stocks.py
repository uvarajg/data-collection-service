#!/usr/bin/env python3
"""
US Market Stock Data Collector
Fetches stock data from GitHub repositories and enriches with YFinance data

Steps:
1. Download raw data from GitHub (Set A)
2. Extract required fields (Set B)
3. Filter by market cap > $2B (Set C)
4. Query YFinance for comprehensive data (Set D)
5. Save all datasets to /data/input_source
"""

import json
import os
import requests
import yfinance as yf
from datetime import datetime
from typing import Dict, List, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep
import traceback

class USMarketStockCollector:
    def __init__(self):
        self.base_path = "/workspaces/data/input_source"
        # Using raw content URLs to fetch directly
        self.github_sources = {
            "amex": "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/amex/amex_full_tickers.json",
            "nasdaq": "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/nasdaq/nasdaq_full_tickers.json",
            "nyse": "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/nyse/nyse_full_tickers.json"
        }
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    def step1_download_raw_data(self) -> Dict[str, List[Dict]]:
        """Step 1: Download and save raw stock data from GitHub (Set A)"""
        print("=" * 80)
        print("STEP 1: Downloading raw stock data from GitHub repositories...")
        print("=" * 80)

        raw_data = {}

        for exchange, url in self.github_sources.items():
            print(f"\n📥 Downloading {exchange.upper()} data from GitHub...")
            try:
                response = requests.get(url, timeout=30)
                response.raise_for_status()

                data = response.json()
                raw_data[exchange] = data

                # We don't save individual exchange files anymore
                print(f"   ✅ Downloaded {len(data)} stocks from {exchange.upper()}")

            except Exception as e:
                print(f"   ❌ Error downloading {exchange}: {e}")
                raw_data[exchange] = []

        # Save combined raw data
        all_stocks = []
        for exchange, stocks in raw_data.items():
            for stock in stocks:
                stock['exchange'] = exchange.upper()
                all_stocks.append(stock)

        combined_filename = f"{self.base_path}/raw_combined_{self.timestamp}.json"
        with open(combined_filename, 'w') as f:
            json.dump(all_stocks, f, indent=2)

        print(f"\n📊 Total stocks downloaded: {len(all_stocks)}")
        print(f"💾 Raw data saved to: {combined_filename}")

        return raw_data

    def step2_extract_required_fields(self, raw_data: Dict[str, List[Dict]]) -> List[Dict]:
        """Step 2: Extract ticker, market cap, country, industry, sector (Set B)"""
        print("\n" + "=" * 80)
        print("STEP 2: Extracting required fields from raw data...")
        print("=" * 80)

        extracted_data = []

        for exchange, stocks in raw_data.items():
            print(f"\n🔍 Processing {exchange.upper()} stocks...")

            for stock in stocks:
                try:
                    # Extract and clean market cap
                    market_cap_str = stock.get('marketCap', '0')
                    market_cap = self._parse_market_cap(market_cap_str)

                    extracted = {
                        'ticker': stock.get('symbol', '').upper(),
                        'name': stock.get('name', ''),
                        'exchange': exchange.upper(),
                        'market_cap': market_cap,
                        'market_cap_str': market_cap_str,
                        'country': stock.get('country', 'USA'),
                        'industry': stock.get('industry', ''),
                        'sector': stock.get('sector', ''),
                        'ipo_year': stock.get('ipoyear', ''),
                        'last_sale': stock.get('lastsale', ''),
                        'net_change': stock.get('netchange', ''),
                        'percent_change': stock.get('pctchange', ''),
                        'volume': stock.get('volume', '')
                    }

                    extracted_data.append(extracted)

                except Exception as e:
                    print(f"   ⚠️ Error processing {stock.get('symbol', 'UNKNOWN')}: {e}")

        # We don't save extracted data anymore (intermediate step)
        print(f"\n📊 Total stocks extracted: {len(extracted_data)}")

        return extracted_data

    def step3_filter_by_market_cap(self, extracted_data: List[Dict]) -> List[Dict]:
        """Step 3: Filter stocks with market cap > $2B (Set C)"""
        print("\n" + "=" * 80)
        print("STEP 3: Filtering stocks with market cap > $2B...")
        print("=" * 80)

        min_market_cap = 2_000_000_000  # $2 billion

        filtered_data = [
            stock for stock in extracted_data
            if stock['market_cap'] >= min_market_cap
        ]

        # Sort by market cap descending
        filtered_data.sort(key=lambda x: x['market_cap'], reverse=True)

        # We don't save filtered data anymore (intermediate step)

        # Print summary by market cap category
        mega_cap = [s for s in filtered_data if s['market_cap'] >= 200_000_000_000]
        large_cap = [s for s in filtered_data if 10_000_000_000 <= s['market_cap'] < 200_000_000_000]
        mid_cap = [s for s in filtered_data if 2_000_000_000 <= s['market_cap'] < 10_000_000_000]

        print(f"\n📊 Market Cap Distribution:")
        print(f"   🏆 Mega Cap (>$200B): {len(mega_cap)} stocks")
        print(f"   💎 Large Cap ($10B-$200B): {len(large_cap)} stocks")
        print(f"   📈 Mid Cap ($2B-$10B): {len(mid_cap)} stocks")
        print(f"   ─────────────────────────────")
        print(f"   📊 Total (>$2B): {len(filtered_data)} stocks")

        # Show top 10 by market cap
        print(f"\n🏆 Top 10 by Market Cap:")
        for i, stock in enumerate(filtered_data[:10], 1):
            market_cap_b = stock['market_cap'] / 1_000_000_000
            print(f"   {i:2}. {stock['ticker']:6} - ${market_cap_b:,.1f}B - {stock['name'][:40]}")

        return filtered_data

    def step4_query_yfinance(self, filtered_data: List[Dict]) -> List[Dict]:
        """Step 4: Query YFinance for comprehensive data (Set D)"""
        print("\n" + "=" * 80)
        print("STEP 4: Querying YFinance for comprehensive data...")
        print("=" * 80)

        enriched_data = []
        failed_tickers = []

        # Process in batches with progress tracking
        batch_size = 50
        total_stocks = len(filtered_data)

        print(f"\n🚀 Processing {total_stocks} stocks in batches of {batch_size}...")

        for batch_start in range(0, total_stocks, batch_size):
            batch_end = min(batch_start + batch_size, total_stocks)
            batch = filtered_data[batch_start:batch_end]
            batch_num = (batch_start // batch_size) + 1
            total_batches = (total_stocks + batch_size - 1) // batch_size

            print(f"\n📦 Batch {batch_num}/{total_batches} ({batch_start+1}-{batch_end}/{total_stocks})...")

            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = {
                    executor.submit(self._get_yfinance_data, stock): stock
                    for stock in batch
                }

                for future in as_completed(futures):
                    original_stock = futures[future]
                    try:
                        yf_data = future.result()
                        if yf_data:
                            # Merge original data with YFinance data
                            enriched = {**original_stock, **yf_data}
                            enriched_data.append(enriched)
                            print(f"   ✅ {original_stock['ticker']}")
                        else:
                            failed_tickers.append(original_stock['ticker'])
                            print(f"   ❌ {original_stock['ticker']} - No data")
                    except Exception as e:
                        failed_tickers.append(original_stock['ticker'])
                        print(f"   ❌ {original_stock['ticker']} - Error: {e}")

            # Save intermediate results every 10 batches
            if batch_num % 10 == 0:
                intermediate_file = f"{self.base_path}/enriched_intermediate_batch_{batch_num}_{self.timestamp}.json"
                with open(intermediate_file, 'w') as f:
                    json.dump(enriched_data, f, indent=2)
                print(f"   💾 Intermediate save: {len(enriched_data)} stocks")

            # Small delay between batches
            if batch_end < total_stocks:
                sleep(1)

        # Save final enriched data
        filename = f"{self.base_path}/enriched_yfinance_{self.timestamp}.json"
        with open(filename, 'w') as f:
            json.dump(enriched_data, f, indent=2)

        # Save failed tickers for retry
        if failed_tickers:
            failed_file = f"{self.base_path}/failed_tickers_{self.timestamp}.json"
            with open(failed_file, 'w') as f:
                json.dump(failed_tickers, f, indent=2)
            print(f"\n⚠️ Failed tickers saved to: {failed_file}")

        print(f"\n" + "=" * 80)
        print(f"📊 YFinance Data Collection Summary:")
        print(f"   ✅ Successful: {len(enriched_data)} stocks")
        print(f"   ❌ Failed: {len(failed_tickers)} stocks")
        print(f"   📈 Success Rate: {len(enriched_data)/total_stocks*100:.1f}%")
        print(f"\n💾 Enriched data saved to: {filename}")

        return enriched_data

    def _parse_market_cap(self, market_cap_str: str) -> float:
        """Parse market cap string to float"""
        if not market_cap_str or market_cap_str == 'N/A':
            return 0

        # Remove $ and commas
        market_cap_str = str(market_cap_str).replace('$', '').replace(',', '')

        # Handle B (billion), M (million), K (thousand) suffixes
        multiplier = 1
        if market_cap_str.endswith('B'):
            multiplier = 1_000_000_000
            market_cap_str = market_cap_str[:-1]
        elif market_cap_str.endswith('M'):
            multiplier = 1_000_000
            market_cap_str = market_cap_str[:-1]
        elif market_cap_str.endswith('K'):
            multiplier = 1_000
            market_cap_str = market_cap_str[:-1]

        try:
            return float(market_cap_str) * multiplier
        except:
            return 0

    def _get_yfinance_data(self, stock: Dict) -> Dict:
        """Get comprehensive data from YFinance for a single stock"""
        ticker = stock['ticker']

        try:
            # Create ticker object
            yf_ticker = yf.Ticker(ticker)

            # Get all available info
            info = yf_ticker.info

            # Extract key metrics
            yf_data = {
                'yf_ticker': ticker,
                'yf_fetched_at': datetime.now().isoformat(),

                # Basic Info
                'yf_long_name': info.get('longName', ''),
                'yf_short_name': info.get('shortName', ''),
                'yf_sector': info.get('sector', ''),
                'yf_industry': info.get('industry', ''),
                'yf_country': info.get('country', ''),
                'yf_website': info.get('website', ''),
                'yf_business_summary': info.get('longBusinessSummary', ''),

                # Market Data
                'yf_market_cap': info.get('marketCap', 0),
                'yf_enterprise_value': info.get('enterpriseValue', 0),
                'yf_current_price': info.get('currentPrice', info.get('regularMarketPrice', 0)),
                'yf_previous_close': info.get('previousClose', 0),
                'yf_open': info.get('open', 0),
                'yf_day_low': info.get('dayLow', 0),
                'yf_day_high': info.get('dayHigh', 0),
                'yf_52_week_low': info.get('fiftyTwoWeekLow', 0),
                'yf_52_week_high': info.get('fiftyTwoWeekHigh', 0),
                'yf_50_day_average': info.get('fiftyDayAverage', 0),
                'yf_200_day_average': info.get('twoHundredDayAverage', 0),
                'yf_volume': info.get('volume', 0),
                'yf_average_volume': info.get('averageVolume', 0),
                'yf_average_volume_10days': info.get('averageVolume10days', 0),

                # Valuation Metrics
                'yf_pe_ratio': info.get('trailingPE', 0),
                'yf_forward_pe': info.get('forwardPE', 0),
                'yf_peg_ratio': info.get('pegRatio', 0),
                'yf_price_to_book': info.get('priceToBook', 0),
                'yf_price_to_sales': info.get('priceToSalesTrailing12Months', 0),
                'yf_ev_to_revenue': info.get('enterpriseToRevenue', 0),
                'yf_ev_to_ebitda': info.get('enterpriseToEbitda', 0),

                # Financial Metrics
                'yf_trailing_eps': info.get('trailingEps', 0),
                'yf_forward_eps': info.get('forwardEps', 0),
                'yf_revenue': info.get('totalRevenue', 0),
                'yf_revenue_per_share': info.get('revenuePerShare', 0),
                'yf_revenue_growth': info.get('revenueGrowth', 0),
                'yf_gross_profits': info.get('grossProfits', 0),
                'yf_gross_margins': info.get('grossMargins', 0),
                'yf_operating_margins': info.get('operatingMargins', 0),
                'yf_profit_margins': info.get('profitMargins', 0),
                'yf_ebitda': info.get('ebitda', 0),
                'yf_ebitda_margins': info.get('ebitdaMargins', 0),
                'yf_net_income': info.get('netIncomeToCommon', 0),
                'yf_earnings_growth': info.get('earningsGrowth', 0),
                'yf_earnings_quarterly_growth': info.get('earningsQuarterlyGrowth', 0),

                # Balance Sheet
                'yf_total_cash': info.get('totalCash', 0),
                'yf_total_cash_per_share': info.get('totalCashPerShare', 0),
                'yf_total_debt': info.get('totalDebt', 0),
                'yf_debt_to_equity': info.get('debtToEquity', 0),
                'yf_current_ratio': info.get('currentRatio', 0),
                'yf_quick_ratio': info.get('quickRatio', 0),
                'yf_book_value': info.get('bookValue', 0),

                # Dividends
                'yf_dividend_rate': info.get('dividendRate', 0),
                'yf_dividend_yield': info.get('dividendYield', 0),
                'yf_trailing_annual_dividend_rate': info.get('trailingAnnualDividendRate', 0),
                'yf_trailing_annual_dividend_yield': info.get('trailingAnnualDividendYield', 0),
                'yf_five_year_avg_dividend_yield': info.get('fiveYearAvgDividendYield', 0),
                'yf_payout_ratio': info.get('payoutRatio', 0),
                'yf_ex_dividend_date': info.get('exDividendDate', ''),

                # Ownership
                'yf_shares_outstanding': info.get('sharesOutstanding', 0),
                'yf_float_shares': info.get('floatShares', 0),
                'yf_shares_short': info.get('sharesShort', 0),
                'yf_short_ratio': info.get('shortRatio', 0),
                'yf_short_percent_of_float': info.get('shortPercentOfFloat', 0),
                'yf_held_percent_insiders': info.get('heldPercentInsiders', 0),
                'yf_held_percent_institutions': info.get('heldPercentInstitutions', 0),

                # Analyst Recommendations
                'yf_recommendation_key': info.get('recommendationKey', ''),
                'yf_recommendation_mean': info.get('recommendationMean', 0),
                'yf_number_of_analyst_opinions': info.get('numberOfAnalystOpinions', 0),
                'yf_target_high_price': info.get('targetHighPrice', 0),
                'yf_target_low_price': info.get('targetLowPrice', 0),
                'yf_target_mean_price': info.get('targetMeanPrice', 0),
                'yf_target_median_price': info.get('targetMedianPrice', 0),

                # Other Metrics
                'yf_beta': info.get('beta', 0),
                'yf_return_on_assets': info.get('returnOnAssets', 0),
                'yf_return_on_equity': info.get('returnOnEquity', 0),
                'yf_free_cashflow': info.get('freeCashflow', 0),
                'yf_operating_cashflow': info.get('operatingCashflow', 0),
            }

            return yf_data

        except Exception as e:
            print(f"      Error fetching {ticker}: {str(e)[:50]}")
            return None


    def run_all_steps(self):
        """Run all steps 1-7"""
        print("\n" + "🚀" * 40)
        print("   US MARKET STOCK DATA COLLECTOR")
        print("   Fetching all US stocks with market cap > $2B")
        print("🚀" * 40)

        start_time = datetime.now()

        # Step 1: Download raw data
        raw_data = self.step1_download_raw_data()

        # Step 2: Extract required fields
        extracted_data = self.step2_extract_required_fields(raw_data)

        # Step 3: Filter by market cap
        filtered_data = self.step3_filter_by_market_cap(extracted_data)

        # Step 4: Query YFinance
        enriched_data = self.step4_query_yfinance(filtered_data)

        # Save final summary
        summary = {
            'timestamp': self.timestamp,
            'total_raw_stocks': sum(len(stocks) for stocks in raw_data.values()),
            'total_extracted': len(extracted_data),
            'total_filtered_2b': len(filtered_data),
            'total_enriched': len(enriched_data),
            'execution_time': str(datetime.now() - start_time),
            'data_files': {
                'raw_combined': f"raw_combined_{self.timestamp}.json",
                'enriched': f"enriched_yfinance_{self.timestamp}.json"
            }
        }

        summary_file = f"{self.base_path}/input_source_data_job_summary_{self.timestamp}.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)

        print("\n" + "=" * 80)
        print("✅ DATA COLLECTION COMPLETE!")
        print("=" * 80)
        print(f"📊 Summary:")
        print(f"   • Raw stocks downloaded: {summary['total_raw_stocks']}")
        print(f"   • Stocks extracted: {summary['total_extracted']}")
        print(f"   • Stocks > $2B: {summary['total_filtered_2b']}")
        print(f"   • Stocks enriched with YFinance: {summary['total_enriched']}")
        print(f"   • Total execution time: {summary['execution_time']}")
        print(f"\n💾 Summary saved to: {summary_file}")
        print(f"📁 All data saved in: {self.base_path}/")

        return enriched_data


if __name__ == "__main__":
    collector = USMarketStockCollector()
    collector.run_all_steps()