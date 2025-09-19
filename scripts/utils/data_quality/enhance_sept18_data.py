#!/usr/bin/env python3
"""
Enhance September 18 data by adding technical indicators and fundamental data
"""

import json
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, List, Tuple

# Add src to path for imports
sys.path.append('/workspaces/data-collection-service/src')

class DataEnhancer:
    def __init__(self, target_date='2025-09-18'):
        self.target_date = target_date
        self.base_path = Path('/workspaces/data/historical/daily')
        self.fundamentals_file = '/workspaces/data/input_source/enriched_yfinance_20250919_093058.json'
        self.fundamentals_data = {}
        self.stats = {
            'total_files': 0,
            'enhanced': 0,
            'technical_added': 0,
            'fundamentals_added': 0,
            'errors': 0
        }

    def load_fundamentals(self):
        """Load fundamental data from enriched source"""
        print("📚 Loading fundamental data from enriched source...")
        with open(self.fundamentals_file, 'r') as f:
            data = json.load(f)

        # Convert list to dict keyed by ticker
        for item in data:
            ticker = item.get('ticker')
            if ticker:
                self.fundamentals_data[ticker] = {
                    'market_cap': item.get('market_cap'),
                    'pe_ratio': item.get('pe_ratio'),
                    'forward_pe': item.get('forward_pe'),
                    'peg_ratio': item.get('peg_ratio'),
                    'price_to_book': item.get('price_to_book'),
                    'profit_margins': item.get('profit_margins'),
                    'return_on_equity': item.get('return_on_equity'),
                    'revenue': item.get('revenue'),
                    'debt_to_equity': item.get('debt_to_equity'),
                    'current_ratio': item.get('current_ratio'),
                    'dividend_yield': item.get('dividend_yield'),
                    'sector': item.get('sector'),
                    'industry': item.get('industry'),
                    'beta': item.get('beta'),
                    'trailing_eps': item.get('trailing_eps'),
                    'book_value': item.get('book_value')
                }

        print(f"✅ Loaded fundamentals for {len(self.fundamentals_data)} tickers")

    def calculate_technical_indicators(self, ticker: str, current_data: Dict) -> Dict:
        """Calculate technical indicators for a ticker"""
        try:
            # Fetch historical data (need 50+ days for indicators)
            end_date = datetime.strptime(self.target_date, '%Y-%m-%d') + timedelta(days=1)
            start_date = end_date - timedelta(days=70)

            stock = yf.Ticker(ticker)
            hist = stock.history(start=start_date.strftime('%Y-%m-%d'),
                               end=end_date.strftime('%Y-%m-%d'))

            if hist.empty or len(hist) < 20:
                return {}

            # Ensure we have the current day's data
            close_prices = hist['Close'].values
            high_prices = hist['High'].values
            low_prices = hist['Low'].values
            volumes = hist['Volume'].values

            # Calculate indicators
            indicators = {}

            # Simple Moving Averages
            if len(close_prices) >= 20:
                indicators['sma_20'] = float(np.mean(close_prices[-20:]))
            if len(close_prices) >= 50:
                indicators['sma_50'] = float(np.mean(close_prices[-50:]))

            # Exponential Moving Averages
            if len(close_prices) >= 12:
                ema_12 = self._calculate_ema(close_prices, 12)
                indicators['ema_12'] = float(ema_12[-1])
            if len(close_prices) >= 26:
                ema_26 = self._calculate_ema(close_prices, 26)
                indicators['ema_26'] = float(ema_26[-1])

                # MACD
                if len(close_prices) >= 26:
                    macd_line = ema_12[-26:] - ema_26
                    signal_line = self._calculate_ema(macd_line, 9)
                    indicators['macd'] = float(macd_line[-1])
                    indicators['macd_signal'] = float(signal_line[-1])
                    indicators['macd_histogram'] = float(macd_line[-1] - signal_line[-1])

            # RSI (14-day)
            if len(close_prices) >= 15:
                indicators['rsi'] = float(self._calculate_rsi(close_prices, 14))

            # Bollinger Bands
            if len(close_prices) >= 20:
                sma_20 = np.mean(close_prices[-20:])
                std_20 = np.std(close_prices[-20:])
                indicators['bb_upper'] = float(sma_20 + (2 * std_20))
                indicators['bb_middle'] = float(sma_20)
                indicators['bb_lower'] = float(sma_20 - (2 * std_20))

            # Volume indicators
            if len(volumes) >= 20:
                indicators['volume_sma_20'] = float(np.mean(volumes[-20:]))
                current_volume = float(current_data.get('volume', 0))
                if indicators['volume_sma_20'] > 0:
                    indicators['volume_ratio'] = current_volume / indicators['volume_sma_20']

            # Price position indicators
            current_close = float(current_data.get('close', 0))
            if current_close > 0:
                # Percent from 52-week high/low
                if len(close_prices) >= 252:  # ~1 year of trading days
                    yearly_high = float(np.max(close_prices[-252:]))
                    yearly_low = float(np.min(close_prices[-252:]))
                    indicators['percent_from_52w_high'] = ((current_close - yearly_high) / yearly_high) * 100
                    indicators['percent_from_52w_low'] = ((current_close - yearly_low) / yearly_low) * 100

            return indicators

        except Exception as e:
            print(f"❌ Error calculating indicators for {ticker}: {str(e)[:50]}")
            return {}

    def _calculate_ema(self, prices, period):
        """Calculate Exponential Moving Average"""
        ema = np.zeros_like(prices)
        ema[0] = prices[0]
        multiplier = 2 / (period + 1)

        for i in range(1, len(prices)):
            ema[i] = (prices[i] * multiplier) + (ema[i-1] * (1 - multiplier))

        return ema

    def _calculate_rsi(self, prices, period=14):
        """Calculate Relative Strength Index"""
        deltas = np.diff(prices)
        gains = deltas.copy()
        losses = deltas.copy()
        gains[gains < 0] = 0
        losses[losses > 0] = 0
        losses = abs(losses)

        avg_gain = np.mean(gains[:period])
        avg_loss = np.mean(losses[:period])

        for i in range(period, len(gains)):
            avg_gain = ((avg_gain * (period - 1)) + gains[i]) / period
            avg_loss = ((avg_loss * (period - 1)) + losses[i]) / period

        if avg_loss == 0:
            return 100

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

        return rsi

    def enhance_single_file(self, file_path: Path, ticker: str) -> bool:
        """Enhance a single data file"""
        try:
            # Load existing data
            with open(file_path, 'r') as f:
                data = json.load(f)

            original_keys = set(data.keys())

            # Add technical indicators
            indicators = self.calculate_technical_indicators(ticker, data)
            if indicators:
                data['technical_indicators'] = indicators
                self.stats['technical_added'] += 1

            # Add fundamental data
            if ticker in self.fundamentals_data:
                # Filter out None values
                fundamentals = {k: v for k, v in self.fundamentals_data[ticker].items() if v is not None}
                if fundamentals:
                    data['fundamentals'] = fundamentals
                    self.stats['fundamentals_added'] += 1

            # Only update if we added new data
            if set(data.keys()) != original_keys:
                # Add metadata
                data['enhanced_timestamp'] = datetime.now().isoformat()
                data['data_version'] = '2.0'

                # Save enhanced data
                with open(file_path, 'w') as f:
                    json.dump(data, f, indent=2)

                self.stats['enhanced'] += 1
                return True

            return False

        except Exception as e:
            print(f"❌ Error enhancing {ticker}: {str(e)[:50]}")
            self.stats['errors'] += 1
            return False

    def process_batch(self, batch: List[Tuple[Path, str]]) -> int:
        """Process a batch of files"""
        enhanced_count = 0
        for file_path, ticker in batch:
            if self.enhance_single_file(file_path, ticker):
                enhanced_count += 1
        return enhanced_count

    def run(self):
        """Run the enhancement process"""
        print("="*80)
        print("🔧 SEPTEMBER 18 DATA ENHANCEMENT PROCESS")
        print("="*80)
        print(f"📅 Target Date: {self.target_date}")
        print(f"📁 Base Path: {self.base_path}")
        print("="*80)

        # Load fundamentals
        self.load_fundamentals()

        # Find all Sept 18 files
        print(f"\n🔍 Scanning for {self.target_date} files...")
        files_to_process = []

        for ticker_dir in self.base_path.iterdir():
            if ticker_dir.is_dir():
                ticker = ticker_dir.name
                file_path = ticker_dir / '2025' / '09' / f'{self.target_date}.json'
                if file_path.exists():
                    files_to_process.append((file_path, ticker))

        self.stats['total_files'] = len(files_to_process)
        print(f"✅ Found {len(files_to_process)} files to enhance")

        # Process files in batches
        print(f"\n🚀 Starting enhancement process...")
        batch_size = 50
        start_time = datetime.now()

        for i in range(0, len(files_to_process), batch_size):
            batch = files_to_process[i:i+batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(files_to_process) + batch_size - 1) // batch_size

            print(f"\n📦 Processing batch {batch_num}/{total_batches} ({i+1}-{min(i+batch_size, len(files_to_process))})...")

            # Process batch with progress tracking
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = {executor.submit(self.enhance_single_file, fp, t): (fp, t)
                          for fp, t in batch}

                for future in as_completed(futures):
                    future.result()  # Get result to trigger any exceptions

            # Progress update
            elapsed = (datetime.now() - start_time).total_seconds()
            progress_pct = ((i + len(batch)) / len(files_to_process)) * 100
            print(f"   ✅ Progress: {progress_pct:.1f}% | Enhanced: {self.stats['enhanced']} | "
                  f"Technical: {self.stats['technical_added']} | Fundamentals: {self.stats['fundamentals_added']}")

        # Final summary
        print("\n" + "="*80)
        print("✅ ENHANCEMENT COMPLETE!")
        print("="*80)
        print(f"📊 Total Files: {self.stats['total_files']}")
        print(f"✅ Enhanced: {self.stats['enhanced']}")
        print(f"📈 Technical Indicators Added: {self.stats['technical_added']}")
        print(f"💰 Fundamentals Added: {self.stats['fundamentals_added']}")
        print(f"❌ Errors: {self.stats['errors']}")
        print(f"⏱️  Total Time: {(datetime.now() - start_time).total_seconds():.1f} seconds")
        print("="*80)

        # Validate a sample
        self.validate_sample()

    def validate_sample(self):
        """Validate a sample of enhanced files"""
        print("\n📋 Validation Sample:")
        sample_tickers = ['AAPL', 'MSFT', 'GOOGL', 'NVDA', 'TSLA']

        for ticker in sample_tickers:
            file_path = self.base_path / ticker / '2025' / '09' / f'{self.target_date}.json'
            if file_path.exists():
                with open(file_path, 'r') as f:
                    data = json.load(f)

                has_technical = 'technical_indicators' in data
                has_fundamentals = 'fundamentals' in data

                status = "✅" if (has_technical and has_fundamentals) else "⚠️"
                print(f"   {status} {ticker}: Technical={'✅' if has_technical else '❌'}, "
                      f"Fundamentals={'✅' if has_fundamentals else '❌'}")

if __name__ == "__main__":
    enhancer = DataEnhancer(target_date='2025-09-18')
    enhancer.run()