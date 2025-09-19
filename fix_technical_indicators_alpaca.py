#!/usr/bin/env python3
"""
Fix technical indicators for Sept 18 data using Alpaca API
"""

import json
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from typing import Dict, Any, Optional
import time

# Add src to path
sys.path.append('/workspaces/data-collection-service/src')

# Import Alpaca service
from services.alpaca_service import AlpacaService

class TechnicalIndicatorFixer:
    def __init__(self, target_date='2025-09-18'):
        self.target_date = target_date
        self.base_path = Path('/workspaces/data/historical/daily')
        self.alpaca_service = AlpacaService()
        self.stats = {
            'total_files': 0,
            'technical_added': 0,
            'already_has_technical': 0,
            'errors': 0,
            'api_calls': 0
        }

    def calculate_technical_indicators_pandas(self, df: pd.DataFrame) -> Dict[str, float]:
        """Calculate technical indicators using pandas (more reliable)"""
        try:
            if df.empty or len(df) < 20:
                return {}

            indicators = {}
            close_prices = df['close'].values
            high_prices = df['high'].values
            low_prices = df['low'].values
            volumes = df['volume'].values

            # Simple Moving Averages
            if len(close_prices) >= 20:
                indicators['sma_20'] = float(df['close'].rolling(window=20).mean().iloc[-1])

            if len(close_prices) >= 50:
                indicators['sma_50'] = float(df['close'].rolling(window=50).mean().iloc[-1])

            # Exponential Moving Averages
            if len(close_prices) >= 12:
                ema_12 = df['close'].ewm(span=12, adjust=False).mean()
                indicators['ema_12'] = float(ema_12.iloc[-1])

            if len(close_prices) >= 26:
                ema_26 = df['close'].ewm(span=26, adjust=False).mean()
                indicators['ema_26'] = float(ema_26.iloc[-1])

                # MACD
                macd_line = ema_12 - ema_26
                signal_line = macd_line.ewm(span=9, adjust=False).mean()
                indicators['macd'] = float(macd_line.iloc[-1])
                indicators['macd_signal'] = float(signal_line.iloc[-1])
                indicators['macd_histogram'] = float(macd_line.iloc[-1] - signal_line.iloc[-1])

            # RSI (14-day)
            if len(close_prices) >= 15:
                delta = df['close'].diff()
                gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
                loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
                rs = gain / loss
                rsi = 100 - (100 / (1 + rs))
                indicators['rsi'] = float(rsi.iloc[-1])

            # Bollinger Bands
            if len(close_prices) >= 20:
                sma_20 = df['close'].rolling(window=20).mean()
                std_20 = df['close'].rolling(window=20).std()
                indicators['bb_upper'] = float(sma_20.iloc[-1] + (2 * std_20.iloc[-1]))
                indicators['bb_middle'] = float(sma_20.iloc[-1])
                indicators['bb_lower'] = float(sma_20.iloc[-1] - (2 * std_20.iloc[-1]))

            # Volume indicators
            if len(volumes) >= 20:
                indicators['volume_sma_20'] = float(df['volume'].rolling(window=20).mean().iloc[-1])
                current_volume = float(volumes[-1])
                if indicators['volume_sma_20'] > 0:
                    indicators['volume_ratio'] = current_volume / indicators['volume_sma_20']

            # Additional indicators
            if len(close_prices) >= 20:
                # ATR (Average True Range)
                high_low = df['high'] - df['low']
                high_close = np.abs(df['high'] - df['close'].shift())
                low_close = np.abs(df['low'] - df['close'].shift())
                true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
                atr = true_range.rolling(window=14).mean()
                if not atr.empty:
                    indicators['atr'] = float(atr.iloc[-1])

                # Price position
                current_close = float(close_prices[-1])
                if len(close_prices) >= 252:  # ~1 year
                    yearly_high = float(np.max(close_prices[-252:]))
                    yearly_low = float(np.min(close_prices[-252:]))
                    if yearly_high != yearly_low:
                        indicators['percent_from_52w_high'] = ((current_close - yearly_high) / yearly_high) * 100
                        indicators['percent_from_52w_low'] = ((current_close - yearly_low) / yearly_low) * 100

            return indicators

        except Exception as e:
            print(f"❌ Error in pandas calculation: {str(e)[:100]}")
            return {}

    def get_historical_data_alpaca(self, ticker: str) -> Optional[pd.DataFrame]:
        """Get historical data from Alpaca API"""
        try:
            # Calculate date range (need 70 days for indicators)
            end_date = datetime.strptime(self.target_date, '%Y-%m-%d')
            start_date = end_date - timedelta(days=70)

            # Get data from Alpaca
            self.stats['api_calls'] += 1
            bars = self.alpaca_service.get_historical_bars(
                ticker=ticker,
                start_date=start_date.strftime('%Y-%m-%d'),
                end_date=end_date.strftime('%Y-%m-%d'),
                timeframe='1Day'
            )

            if not bars or bars.empty:
                return None

            # Ensure we have the columns we need
            bars = bars.rename(columns={
                'open': 'open',
                'high': 'high',
                'low': 'low',
                'close': 'close',
                'volume': 'volume'
            })

            return bars

        except Exception as e:
            if 'rate limit' in str(e).lower():
                print(f"⏳ Rate limit hit, waiting 60 seconds...")
                time.sleep(60)
                return self.get_historical_data_alpaca(ticker)  # Retry
            print(f"❌ Alpaca error for {ticker}: {str(e)[:100]}")
            return None

    def fix_single_file(self, file_path: Path, ticker: str) -> bool:
        """Fix technical indicators for a single file"""
        try:
            # Load existing data
            with open(file_path, 'r') as f:
                data = json.load(f)

            # Check if already has technical indicators
            if 'technical_indicators' in data and len(data.get('technical_indicators', {})) > 5:
                self.stats['already_has_technical'] += 1
                return False

            # Get historical data from Alpaca
            hist_df = self.get_historical_data_alpaca(ticker)
            if hist_df is None or hist_df.empty:
                # Fallback: try getting from Alpaca's technical indicators directly
                indicators = self.get_alpaca_technical_indicators(ticker)
            else:
                # Calculate indicators using pandas
                indicators = self.calculate_technical_indicators_pandas(hist_df)

            if indicators and len(indicators) > 0:
                # Update the data
                data['technical_indicators'] = indicators
                data['technical_enhanced_timestamp'] = datetime.now().isoformat()
                data['technical_source'] = 'alpaca'

                # Save the updated file
                with open(file_path, 'w') as f:
                    json.dump(data, f, indent=2)

                self.stats['technical_added'] += 1
                return True

            return False

        except Exception as e:
            print(f"❌ Error fixing {ticker}: {str(e)[:100]}")
            self.stats['errors'] += 1
            return False

    def get_alpaca_technical_indicators(self, ticker: str) -> Dict[str, float]:
        """Get pre-calculated technical indicators from Alpaca if available"""
        try:
            # Try to get recent technical data from Alpaca
            end_date = datetime.strptime(self.target_date, '%Y-%m-%d')

            # Get SMA values
            indicators = {}

            # Note: Alpaca doesn't provide pre-calculated indicators in the basic API
            # We'll need to calculate them from the price data
            return {}

        except Exception as e:
            return {}

    def run(self):
        """Run the technical indicator fix process"""
        print("="*80)
        print("🔧 FIXING TECHNICAL INDICATORS USING ALPACA")
        print("="*80)
        print(f"📅 Target Date: {self.target_date}")
        print(f"📁 Base Path: {self.base_path}")
        print("="*80)

        # Test Alpaca connection first
        print("\n🔌 Testing Alpaca connection...")
        test_ticker = 'AAPL'
        test_data = self.get_historical_data_alpaca(test_ticker)
        if test_data is not None and not test_data.empty:
            print(f"✅ Alpaca connection successful! Got {len(test_data)} days of data for {test_ticker}")
        else:
            print("❌ Warning: Alpaca connection issue, but continuing...")

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
        print(f"✅ Found {len(files_to_process)} files to process")

        # Process files in batches to manage rate limits
        print(f"\n🚀 Starting technical indicator calculation...")
        batch_size = 10  # Smaller batches for rate limit management
        start_time = datetime.now()

        for i in range(0, len(files_to_process), batch_size):
            batch = files_to_process[i:i+batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(files_to_process) + batch_size - 1) // batch_size

            print(f"\n📦 Processing batch {batch_num}/{total_batches} ({i+1}-{min(i+batch_size, len(files_to_process))})...")

            for file_path, ticker in batch:
                success = self.fix_single_file(file_path, ticker)
                if success and (self.stats['technical_added'] % 10 == 0):
                    print(f"   ✅ Added technical indicators to {self.stats['technical_added']} files")

                # Rate limit management (Alpaca has 200 requests/minute limit)
                if self.stats['api_calls'] % 180 == 0 and self.stats['api_calls'] > 0:
                    print("   ⏳ Rate limit pause (60 seconds)...")
                    time.sleep(60)

            # Progress update
            progress_pct = ((i + len(batch)) / len(files_to_process)) * 100
            print(f"   📊 Progress: {progress_pct:.1f}% | Technical Added: {self.stats['technical_added']} | "
                  f"Already Had: {self.stats['already_has_technical']} | Errors: {self.stats['errors']}")

        # Final summary
        elapsed = (datetime.now() - start_time).total_seconds()
        print("\n" + "="*80)
        print("✅ TECHNICAL INDICATOR FIX COMPLETE!")
        print("="*80)
        print(f"📊 Total Files: {self.stats['total_files']}")
        print(f"✅ Technical Indicators Added: {self.stats['technical_added']}")
        print(f"⏭️  Already Had Indicators: {self.stats['already_has_technical']}")
        print(f"❌ Errors: {self.stats['errors']}")
        print(f"📡 API Calls Made: {self.stats['api_calls']}")
        print(f"⏱️  Total Time: {elapsed:.1f} seconds")
        print("="*80)

        # Validate sample files
        self.validate_sample()

    def validate_sample(self):
        """Validate a sample of fixed files"""
        print("\n📋 Validation Sample:")
        sample_tickers = ['AAPL', 'MSFT', 'GOOGL', 'NVDA', 'TSLA', 'AMZN', 'META']

        for ticker in sample_tickers:
            file_path = self.base_path / ticker / '2025' / '09' / f'{self.target_date}.json'
            if file_path.exists():
                with open(file_path, 'r') as f:
                    data = json.load(f)

                has_technical = 'technical_indicators' in data
                has_fundamentals = 'fundamentals' in data

                if has_technical:
                    num_indicators = len(data['technical_indicators'])
                    status = "✅" if num_indicators >= 5 else "⚠️"
                    print(f"   {status} {ticker}: Technical={num_indicators} indicators, "
                          f"Fundamentals={'✅' if has_fundamentals else '❌'}")
                else:
                    print(f"   ❌ {ticker}: No technical indicators, "
                          f"Fundamentals={'✅' if has_fundamentals else '❌'}")

if __name__ == "__main__":
    fixer = TechnicalIndicatorFixer(target_date='2025-09-18')
    fixer.run()