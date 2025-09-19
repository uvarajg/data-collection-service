#!/usr/bin/env python3
"""
Fix technical indicators for Sept 18 data using Alpaca API (simplified version)
"""

import json
import os
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from typing import Dict, Optional
import time
import alpaca_trade_api as tradeapi
from alpaca_trade_api.rest import TimeFrame
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class TechnicalIndicatorFixer:
    def __init__(self, target_date=None):
        self.target_date = target_date
        self.base_path = Path('/workspaces/data/historical/daily')

        # Initialize Alpaca client
        self.api = tradeapi.REST(
            key_id=os.getenv('ALPACA_API_KEY_ID'),
            secret_key=os.getenv('ALPACA_API_SECRET_KEY'),
            base_url='https://paper-api.alpaca.markets'  # Use paper trading endpoint
        )

        self.stats = {
            'total_files': 0,
            'technical_added': 0,
            'already_has_technical': 0,
            'errors': 0,
            'api_calls': 0
        }

    def calculate_technical_indicators_pandas(self, df: pd.DataFrame) -> Dict[str, float]:
        """Calculate technical indicators using pandas (reliable method)"""
        try:
            if df.empty or len(df) < 20:
                return {}

            indicators = {}

            # Ensure we have the right columns
            if 'close' not in df.columns:
                return {}

            # Simple Moving Averages
            if len(df) >= 20:
                indicators['sma_20'] = float(df['close'].rolling(window=20).mean().iloc[-1])

            if len(df) >= 50:
                indicators['sma_50'] = float(df['close'].rolling(window=50).mean().iloc[-1])

            # Exponential Moving Averages
            if len(df) >= 12:
                ema_12 = df['close'].ewm(span=12, adjust=False).mean()
                indicators['ema_12'] = float(ema_12.iloc[-1])

            if len(df) >= 26:
                ema_26 = df['close'].ewm(span=26, adjust=False).mean()
                indicators['ema_26'] = float(ema_26.iloc[-1])

                # MACD
                if len(ema_12) >= 26:
                    macd_line = ema_12 - ema_26
                    signal_line = macd_line.ewm(span=9, adjust=False).mean()
                    indicators['macd'] = float(macd_line.iloc[-1])
                    indicators['macd_signal'] = float(signal_line.iloc[-1])
                    indicators['macd_histogram'] = float(macd_line.iloc[-1] - signal_line.iloc[-1])

            # RSI (14-day)
            if len(df) >= 15:
                delta = df['close'].diff()
                gain = delta.where(delta > 0, 0).rolling(window=14).mean()
                loss = -delta.where(delta < 0, 0).rolling(window=14).mean()

                # Avoid division by zero
                rs = gain / loss.replace(0, 1e-10)
                rsi = 100 - (100 / (1 + rs))
                indicators['rsi'] = float(rsi.iloc[-1]) if not pd.isna(rsi.iloc[-1]) else 50.0

            # Bollinger Bands
            if len(df) >= 20:
                sma_20 = df['close'].rolling(window=20).mean()
                std_20 = df['close'].rolling(window=20).std()
                indicators['bb_upper'] = float(sma_20.iloc[-1] + (2 * std_20.iloc[-1]))
                indicators['bb_middle'] = float(sma_20.iloc[-1])
                indicators['bb_lower'] = float(sma_20.iloc[-1] - (2 * std_20.iloc[-1]))

            # Volume indicators
            if 'volume' in df.columns and len(df) >= 20:
                indicators['volume_sma_20'] = float(df['volume'].rolling(window=20).mean().iloc[-1])
                current_volume = float(df['volume'].iloc[-1])
                if indicators['volume_sma_20'] > 0:
                    indicators['volume_ratio'] = current_volume / indicators['volume_sma_20']

            # ATR (Average True Range)
            if len(df) >= 15 and all(col in df.columns for col in ['high', 'low', 'close']):
                high_low = df['high'] - df['low']
                high_close = np.abs(df['high'] - df['close'].shift())
                low_close = np.abs(df['low'] - df['close'].shift())

                ranges = pd.concat([high_low, high_close, low_close], axis=1)
                true_range = ranges.max(axis=1)
                atr = true_range.rolling(window=14).mean()

                if not atr.empty:
                    indicators['atr'] = float(atr.iloc[-1])

            return indicators

        except Exception as e:
            print(f"❌ Error in indicator calculation: {str(e)[:100]}")
            return {}

    def get_historical_data_alpaca(self, ticker: str) -> Optional[pd.DataFrame]:
        """Get historical data from Alpaca API"""
        try:
            # Calculate date range (70 days for indicators)
            end_date = datetime.strptime(self.target_date, '%Y-%m-%d')
            start_date = end_date - timedelta(days=70)

            # Get bars from Alpaca
            self.stats['api_calls'] += 1

            bars = self.api.get_bars(
                ticker,
                TimeFrame.Day,
                start=start_date.strftime('%Y-%m-%d'),
                end=end_date.strftime('%Y-%m-%d'),
                adjustment='raw'
            ).df

            if bars.empty:
                return None

            # Reset index to have clean dataframe
            bars = bars.reset_index()

            return bars

        except Exception as e:
            if 'rate limit' in str(e).lower():
                print(f"⏳ Rate limit hit for {ticker}, waiting 60 seconds...")
                time.sleep(60)
                return self.get_historical_data_alpaca(ticker)  # Retry
            elif 'not found' in str(e).lower() or '404' in str(e):
                # Ticker not available in Alpaca
                return None
            else:
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
                return False

            # Calculate indicators
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

    def run(self):
        """Run the technical indicator fix process"""
        print("="*80)
        print("🔧 FIXING TECHNICAL INDICATORS USING ALPACA")
        print("="*80)
        print(f"📅 Target Date: {self.target_date}")
        print(f"📁 Base Path: {self.base_path}")
        print("="*80)

        # Check API credentials
        if not os.getenv('ALPACA_API_KEY_ID') or not os.getenv('ALPACA_API_SECRET_KEY'):
            print("❌ ERROR: Alpaca API credentials not found in environment!")
            print("Please set ALPACA_API_KEY_ID and ALPACA_API_SECRET_KEY in .env file")
            return

        # Test Alpaca connection
        print("\n🔌 Testing Alpaca connection...")
        try:
            account = self.api.get_account()
            print(f"✅ Alpaca connection successful! Account status: {account.status}")
        except Exception as e:
            print(f"❌ Alpaca connection error: {str(e)}")
            print("Continuing anyway...")

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

        # Process files
        print(f"\n🚀 Starting technical indicator calculation...")
        batch_size = 5  # Small batches for rate limits
        start_time = datetime.now()

        # Process high-priority stocks first
        priority_tickers = ['AAPL', 'MSFT', 'NVDA', 'GOOGL', 'AMZN', 'META', 'TSLA', 'BRK.B',
                           'JPM', 'V', 'JNJ', 'WMT', 'PG', 'MA', 'HD']

        priority_files = [(f, t) for f, t in files_to_process if t in priority_tickers]
        other_files = [(f, t) for f, t in files_to_process if t not in priority_tickers]

        # Reorder: priority first
        files_to_process = priority_files + other_files

        for i in range(0, len(files_to_process), batch_size):
            batch = files_to_process[i:i+batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(files_to_process) + batch_size - 1) // batch_size

            print(f"\n📦 Batch {batch_num}/{total_batches} ({i+1}-{min(i+batch_size, len(files_to_process))})...")

            for file_path, ticker in batch:
                print(f"   Processing {ticker}...", end=' ')
                success = self.fix_single_file(file_path, ticker)
                if success:
                    print("✅")
                else:
                    print("⏭️")

                # Rate limit: Alpaca allows 200 requests/minute
                if self.stats['api_calls'] % 190 == 0 and self.stats['api_calls'] > 0:
                    print("   ⏳ Rate limit pause (60 seconds)...")
                    time.sleep(60)
                elif self.stats['api_calls'] % 10 == 0:
                    time.sleep(1)  # Small pause every 10 requests

            # Progress update
            progress_pct = ((i + len(batch)) / len(files_to_process)) * 100
            print(f"   📊 Progress: {progress_pct:.1f}% | Added: {self.stats['technical_added']} | "
                  f"Skipped: {self.stats['already_has_technical']} | Errors: {self.stats['errors']}")

        # Final summary
        elapsed = (datetime.now() - start_time).total_seconds()
        print("\n" + "="*80)
        print("✅ TECHNICAL INDICATOR FIX COMPLETE!")
        print("="*80)
        print(f"📊 Total Files: {self.stats['total_files']}")
        print(f"✅ Technical Indicators Added: {self.stats['technical_added']}")
        print(f"⏭️  Already Had Indicators: {self.stats['already_has_technical']}")
        print(f"❌ Errors: {self.stats['errors']}")
        print(f"📡 API Calls: {self.stats['api_calls']}")
        print(f"⏱️  Time: {elapsed:.1f} seconds")
        print("="*80)

        # Validate sample
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
                    indicators = data['technical_indicators']
                    num = len(indicators)
                    # Show some key indicators
                    rsi = indicators.get('rsi', 'N/A')
                    sma_20 = indicators.get('sma_20', 'N/A')

                    status = "✅" if num >= 5 else "⚠️"
                    print(f"   {status} {ticker}: {num} indicators (RSI={rsi:.1f if isinstance(rsi, float) else rsi}, "
                          f"SMA20={sma_20:.2f if isinstance(sma_20, float) else sma_20})")
                else:
                    print(f"   ❌ {ticker}: No technical indicators")

if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        target_date = sys.argv[1]
    else:
        target_date = input("Enter target date (YYYY-MM-DD): ").strip()

    if not target_date:
        print("❌ Error: Target date is required")
        sys.exit(1)

    fixer = TechnicalIndicatorFixer(target_date=target_date)
    fixer.run()