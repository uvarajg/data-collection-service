#!/usr/bin/env python3
"""
Test version of Historical Input Data Polygon

This script tests the Polygon.io data collection with limited tickers and date range
to verify functionality before running the full collection.

Test Parameters:
- Limited tickers: AAPL, MSFT, GOOGL, TSLA, NVDA
- Date range: Last 30 days
- Same technical indicator and fundamental calculations
"""

import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass

import aiohttp
import numpy as np
import pandas as pd
import ta
from dotenv import load_dotenv

# Load environment variables from the data collection service
load_dotenv('/workspaces/data-collection-service/.env')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/workspaces/data/raw_data/polygon/test_collection.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class PolygonConfig:
    """Configuration for Polygon.io API"""
    api_key: str
    base_url: str = "https://api.polygon.io"
    rate_limit: int = 5  # requests per minute for Starter plan
    timeout: int = 30
    max_retries: int = 3
    retry_delay: int = 60  # seconds

@dataclass
class StockDataPoint:
    """Data structure matching the existing validation service format"""
    record_id: str
    ticker: str
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    adjusted_close: Optional[float] = None

    # Technical indicators (calculated locally)
    rsi_14: Optional[float] = None
    macd_line: Optional[float] = None
    macd_signal: Optional[float] = None
    macd_histogram: Optional[float] = None
    sma_50: Optional[float] = None
    sma_200: Optional[float] = None
    ema_12: Optional[float] = None
    ema_26: Optional[float] = None
    bb_upper: Optional[float] = None
    bb_middle: Optional[float] = None
    bb_lower: Optional[float] = None
    atr_14: Optional[float] = None
    volatility: Optional[float] = None

    # Fundamental data (calculated from market data)
    market_cap: Optional[float] = None
    pe_ratio: Optional[float] = None
    debt_to_equity: Optional[float] = None
    roe_percent: Optional[float] = None
    current_ratio: Optional[float] = None
    operating_margin_percent: Optional[float] = None
    revenue_growth_percent: Optional[float] = None
    profit_margin_percent: Optional[float] = None
    dividend_yield_percent: Optional[float] = None
    book_value: Optional[float] = None

    # Metadata
    collection_timestamp: str = ""
    data_source: str = "polygon.io"
    processing_status: str = "collected"

class TechnicalIndicatorsCalculator:
    """Technical indicators calculator using the same logic as the existing service"""

    def __init__(self):
        self.logger = logging.getLogger(__name__ + ".technical")

    def calculate_indicators(self, records: List[StockDataPoint]) -> List[StockDataPoint]:
        """Calculate technical indicators for a list of stock data records"""
        if not records or len(records) < 50:
            self.logger.warning(f"Insufficient data for technical indicators: {len(records)} records")
            return records

        # Sort records by date to ensure proper chronological order
        records = sorted(records, key=lambda r: r.date)

        # Extract OHLCV data into numpy arrays
        close_prices = np.array([r.close for r in records])
        high_prices = np.array([r.high for r in records])
        low_prices = np.array([r.low for r in records])
        volume_data = np.array([r.volume for r in records])

        # Calculate all technical indicators
        indicators = self._calculate_all_indicators(
            close_prices, high_prices, low_prices, volume_data
        )

        # Populate indicators back into records
        for i, record in enumerate(records):
            record.rsi_14 = self._safe_get_indicator(indicators['rsi_14'], i)
            record.macd_line = self._safe_get_indicator(indicators['macd_line'], i)
            record.macd_signal = self._safe_get_indicator(indicators['macd_signal'], i)
            record.macd_histogram = self._safe_get_indicator(indicators['macd_histogram'], i)
            record.sma_50 = self._safe_get_indicator(indicators['sma_50'], i)
            record.sma_200 = self._safe_get_indicator(indicators['sma_200'], i)
            record.ema_12 = self._safe_get_indicator(indicators['ema_12'], i)
            record.ema_26 = self._safe_get_indicator(indicators['ema_26'], i)
            record.bb_upper = self._safe_get_indicator(indicators['bb_upper'], i)
            record.bb_middle = self._safe_get_indicator(indicators['bb_middle'], i)
            record.bb_lower = self._safe_get_indicator(indicators['bb_lower'], i)
            record.atr_14 = self._safe_get_indicator(indicators['atr_14'], i)
            record.volatility = self._safe_get_indicator(indicators['volatility'], i)
            record.processing_status = "indicators_calculated"

        self.logger.info(f"Technical indicators calculated for {len(records)} records")
        return records

    def _calculate_all_indicators(
        self,
        close: np.ndarray,
        high: np.ndarray,
        low: np.ndarray,
        volume: np.ndarray
    ) -> Dict[str, np.ndarray]:
        """Calculate all technical indicators using TA library"""

        indicators = {}

        try:
            # Create DataFrame for TA library
            df = pd.DataFrame({
                'close': close,
                'high': high,
                'low': low,
                'volume': volume
            })

            # RSI (14-period)
            indicators['rsi_14'] = ta.momentum.RSIIndicator(df['close'], window=14).rsi().values

            # MACD (12, 26, 9)
            macd = ta.trend.MACD(df['close'], window_fast=12, window_slow=26, window_sign=9)
            indicators['macd_line'] = macd.macd().values
            indicators['macd_signal'] = macd.macd_signal().values
            indicators['macd_histogram'] = macd.macd_diff().values

            # Simple Moving Averages
            indicators['sma_50'] = ta.trend.SMAIndicator(df['close'], window=50).sma_indicator().values
            indicators['sma_200'] = ta.trend.SMAIndicator(df['close'], window=200).sma_indicator().values

            # Exponential Moving Averages
            indicators['ema_12'] = ta.trend.EMAIndicator(df['close'], window=12).ema_indicator().values
            indicators['ema_26'] = ta.trend.EMAIndicator(df['close'], window=26).ema_indicator().values

            # Bollinger Bands (20-period, 2 standard deviations)
            bb = ta.volatility.BollingerBands(df['close'], window=20, window_dev=2)
            indicators['bb_upper'] = bb.bollinger_hband().values
            indicators['bb_middle'] = bb.bollinger_mavg().values
            indicators['bb_lower'] = bb.bollinger_lband().values

            # Average True Range (14-period)
            indicators['atr_14'] = ta.volatility.AverageTrueRange(df['high'], df['low'], df['close'], window=14).average_true_range().values

            # Volatility calculation (20-period standard deviation)
            indicators['volatility'] = df['close'].rolling(window=20).std().values

        except Exception as e:
            self.logger.error(f"Error calculating technical indicators: {e}")
            # Return empty arrays if calculation fails
            array_length = len(close)
            for key in ['rsi_14', 'macd_line', 'macd_signal', 'macd_histogram',
                       'sma_50', 'sma_200', 'ema_12', 'ema_26',
                       'bb_upper', 'bb_middle', 'bb_lower', 'atr_14', 'volatility']:
                indicators[key] = np.full(array_length, np.nan)

        return indicators

    def _safe_get_indicator(self, indicator_array: np.ndarray, index: int) -> Optional[float]:
        """Safely get indicator value, handling NaN and out-of-bounds"""
        try:
            if index < len(indicator_array):
                value = indicator_array[index]
                if np.isnan(value) or np.isinf(value):
                    return None
                return float(value)
            return None
        except (IndexError, ValueError, TypeError):
            return None

class PolygonDataCollector:
    """Test data collector for Polygon.io API"""

    def __init__(self, config: PolygonConfig):
        self.config = config
        self.session = None
        self.last_request_time = 0
        self.request_interval = 12  # 12 seconds between requests (5 per minute)
        self.technical_calculator = TechnicalIndicatorsCalculator()
        self.logger = logging.getLogger(__name__ + ".collector")

        # Create output directories
        self.base_path = Path("/workspaces/data/raw_data/polygon")
        self.error_path = self.base_path / "error_records"
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.error_path.mkdir(parents=True, exist_ok=True)

    async def __aenter__(self):
        """Async context manager entry"""
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.config.timeout)
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()

    async def collect_test_data(self, ticker: str, days: int = 250, min_market_cap: float = 2_000_000_000) -> bool:
        """
        Collect test data for a specific ticker (need 250 days for technical indicators)
        Only stores data if market cap > $2B

        Args:
            ticker: Stock symbol
            days: Number of days to collect (need 250+ for indicators)
            min_market_cap: Minimum market cap threshold (default $2B)
        """
        try:
            # Calculate date range (need enough history for technical indicators)
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=days)

            self.logger.info(f"Collecting {days} days of test data for {ticker}: {start_date} to {end_date}")

            # FIRST: Get financial data to check market cap
            financials_data = await self._get_financials_data(ticker)

            # Check market cap filter
            market_cap = None
            if financials_data and 'marketCapitalization' in financials_data:
                market_cap = financials_data['marketCapitalization']

            if market_cap is None:
                self.logger.warning(f"No market cap data available for {ticker}, skipping")
                await self._log_error(ticker, "market_cap_missing", "No market cap data available")
                return False

            if market_cap < min_market_cap:
                self.logger.info(f"Skipping {ticker}: Market cap ${market_cap:,.0f} < ${min_market_cap:,.0f} threshold")
                await self._log_error(ticker, "market_cap_filter", f"Market cap ${market_cap:,.0f} below ${min_market_cap:,.0f} threshold")
                return False

            self.logger.info(f"✅ {ticker} passes market cap filter: ${market_cap:,.0f} > ${min_market_cap:,.0f}")

            # NOW collect OHLCV data (only if market cap passes)
            ohlcv_data = await self._get_ohlcv_data(ticker, start_date, end_date)
            if not ohlcv_data:
                await self._log_error(ticker, "ohlcv_collection", "No OHLCV data received")
                return False

            self.logger.info(f"Retrieved {len(ohlcv_data)} days of data for {ticker}")

            # Convert to StockDataPoint objects
            records = []
            for daily_data in ohlcv_data:
                record = StockDataPoint(
                    record_id=f"{ticker}_{daily_data['t']}_{int(time.time())}",
                    ticker=ticker,
                    date=daily_data['t'],
                    open=daily_data['o'],
                    high=daily_data['h'],
                    low=daily_data['l'],
                    close=daily_data['c'],
                    volume=daily_data['v'],
                    adjusted_close=daily_data.get('ac'),
                    collection_timestamp=datetime.now().isoformat(),
                    data_source="polygon.io"
                )

                # Extract fundamental ratios from financial data
                if financials_data:
                    # Market Cap
                    if 'marketCapitalization' in financials_data:
                        record.market_cap = financials_data['marketCapitalization']

                    # P/E Ratio
                    if 'priceToEarningsRatio' in financials_data:
                        record.pe_ratio = financials_data['priceToEarningsRatio']

                    # Book Value per Share
                    if 'bookValuePerShare' in financials_data:
                        record.book_value = financials_data['bookValuePerShare']

                    # Dividend Yield (convert to percentage)
                    if 'dividendYield' in financials_data:
                        record.dividend_yield_percent = financials_data['dividendYield'] * 100

                    # Debt to Equity Ratio
                    if 'debtToEquityRatio' in financials_data:
                        record.debt_to_equity = financials_data['debtToEquityRatio']

                    # ROE (convert to percentage)
                    if 'returnOnAverageEquity' in financials_data:
                        record.roe_percent = financials_data['returnOnAverageEquity'] * 100

                    # Current Ratio
                    if 'currentRatio' in financials_data:
                        record.current_ratio = financials_data['currentRatio']

                    # Profit Margin (convert to percentage)
                    if 'profitMargin' in financials_data:
                        record.profit_margin_percent = financials_data['profitMargin'] * 100

                    # Operating Margin - calculate from operating income and revenues
                    if 'operatingIncome' in financials_data and 'revenues' in financials_data:
                        revenues = financials_data['revenues']
                        if revenues and revenues != 0:
                            record.operating_margin_percent = (financials_data['operatingIncome'] / revenues) * 100

                records.append(record)

            # Calculate technical indicators for all records
            records = self.technical_calculator.calculate_indicators(records)

            # Store only the last 30 days of data with indicators
            recent_records = records[-30:] if len(records) > 30 else records
            await self._store_data(ticker, recent_records)

            self.logger.info(f"Successfully collected and stored {len(recent_records)} records for {ticker}")
            return True

        except Exception as e:
            self.logger.error(f"Error collecting data for {ticker}: {e}")
            await self._log_error(ticker, "collection_error", str(e))
            return False

    async def _get_ohlcv_data(self, ticker: str, start_date, end_date) -> List[Dict]:
        """Get OHLCV data from Polygon API with rate limiting and retries"""

        url = f"{self.config.base_url}/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}"
        params = {
            'apikey': self.config.api_key,
            'adjusted': 'true',
            'sort': 'asc',
            'limit': 50000
        }

        for attempt in range(self.config.max_retries):
            try:
                # Simple rate limiting
                current_time = time.time()
                time_since_last_request = current_time - self.last_request_time
                if time_since_last_request < self.request_interval:
                    wait_time = self.request_interval - time_since_last_request
                    self.logger.info(f"Rate limiting: waiting {wait_time:.1f} seconds")
                    await asyncio.sleep(wait_time)

                self.logger.info(f"Making API request for {ticker} (attempt {attempt + 1})")
                self.last_request_time = time.time()

                async with self.session.get(url, params=params) as response:
                        self.logger.info(f"API response status for {ticker}: {response.status}")

                        if response.status == 200:
                            data = await response.json()
                            self.logger.info(f"API response data keys: {list(data.keys())}")

                            if data.get('status') in ['OK', 'DELAYED'] and 'results' in data:
                                # Convert timestamp to date format
                                results = []
                                for item in data['results']:
                                    results.append({
                                        't': datetime.fromtimestamp(item['t'] / 1000).strftime('%Y-%m-%d'),
                                        'o': item['o'],
                                        'h': item['h'],
                                        'l': item['l'],
                                        'c': item['c'],
                                        'v': item['v'],
                                        'ac': item.get('c')  # Use close as adjusted close if not available
                                    })
                                self.logger.info(f"Successfully processed {len(results)} records for {ticker}")
                                return results
                            else:
                                self.logger.warning(f"No data for {ticker}: {data}")
                                return []

                        elif response.status == 429:  # Rate limited
                            self.logger.warning(f"Rate limited for {ticker}, waiting {self.config.retry_delay}s")
                            await asyncio.sleep(self.config.retry_delay)
                            continue

                        else:
                            response_text = await response.text()
                            self.logger.error(f"API error for {ticker}: {response.status} - {response_text}")
                            return []

            except Exception as e:
                self.logger.error(f"Request error for {ticker} (attempt {attempt + 1}): {e}")
                if attempt < self.config.max_retries - 1:
                    await asyncio.sleep(self.config.retry_delay)

        return []

    async def _get_financials_data(self, ticker: str) -> Dict[str, Any]:
        """Get comprehensive financial data from Polygon legacy financials endpoint"""
        self.logger.info(f"Attempting to get financial data for {ticker}")

        # Simple rate limiting
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        if time_since_last_request < self.request_interval:
            wait_time = self.request_interval - time_since_last_request
            self.logger.info(f"Financial data rate limiting: waiting {wait_time:.1f} seconds")
            await asyncio.sleep(wait_time)

        self.last_request_time = time.time()

        url = f"{self.config.base_url}/v2/reference/financials/{ticker}"
        params = {
            'apikey': self.config.api_key,
            'limit': 4,
            'type': 'Y'  # Annual data
        }

        try:
            async with self.session.get(url, params=params) as response:
                self.logger.info(f"Financial data API response status for {ticker}: {response.status}")

                if response.status == 200:
                    data = await response.json()
                    if data.get('status') in ['OK', 'DELAYED'] and 'results' in data:
                        results = data['results']
                        if results:
                            # Get the most recent financial data
                            latest = results[0]
                            self.logger.info(f"Retrieved financial data for {ticker} (period: {latest.get('reportPeriod', 'Unknown')})")
                            return latest
                else:
                    response_text = await response.text()
                    self.logger.warning(f"Could not get financial data for {ticker}: {response.status} - {response_text}")
        except Exception as e:
            self.logger.warning(f"Could not get financial data for {ticker}: {e}")

        return {}

    async def _store_data(self, ticker: str, records: List[StockDataPoint]):
        """Store collected data in JSON format matching existing structure"""

        for record in records:
            # Create directory structure: /ticker/year/month/
            date_obj = datetime.strptime(record.date, '%Y-%m-%d')
            ticker_path = self.base_path / ticker / str(date_obj.year) / f"{date_obj.month:02d}"
            ticker_path.mkdir(parents=True, exist_ok=True)

            # Create JSON file for the date
            file_path = ticker_path / f"{record.date}.json"

            # Convert record to JSON format matching existing structure
            data_dict = {
                "record_id": record.record_id,
                "ticker": record.ticker,
                "date": record.date,
                "basic_data": {
                    "open": record.open,
                    "high": record.high,
                    "low": record.low,
                    "close": record.close,
                    "volume": record.volume,
                    "adjusted_close": record.adjusted_close
                },
                "technical_indicators": {
                    "rsi_14": record.rsi_14,
                    "macd_line": record.macd_line,
                    "macd_signal": record.macd_signal,
                    "macd_histogram": record.macd_histogram,
                    "sma_50": record.sma_50,
                    "sma_200": record.sma_200,
                    "ema_12": record.ema_12,
                    "ema_26": record.ema_26,
                    "bb_upper": record.bb_upper,
                    "bb_middle": record.bb_middle,
                    "bb_lower": record.bb_lower,
                    "atr_14": record.atr_14,
                    "volatility": record.volatility
                },
                "fundamental_data": {
                    "market_cap": record.market_cap,
                    "pe_ratio": record.pe_ratio,
                    "debt_to_equity": record.debt_to_equity,
                    "roe_percent": record.roe_percent,
                    "current_ratio": record.current_ratio,
                    "operating_margin_percent": record.operating_margin_percent,
                    "revenue_growth_percent": record.revenue_growth_percent,
                    "profit_margin_percent": record.profit_margin_percent,
                    "dividend_yield_percent": record.dividend_yield_percent,
                    "book_value": record.book_value
                },
                "metadata": {
                    "collection_timestamp": record.collection_timestamp,
                    "data_source": record.data_source,
                    "processing_status": record.processing_status,
                    "technical_indicators_calculated": True,
                    "fundamental_data_calculated": True
                }
            }

            # Write to file
            with open(file_path, 'w') as f:
                json.dump(data_dict, f, indent=2, default=str)

        self.logger.info(f"Stored {len(records)} records for {ticker}")

    async def _log_error(self, ticker: str, error_type: str, error_message: str):
        """Log detailed error information for retry purposes"""

        error_record = {
            "ticker": ticker,
            "error_type": error_type,
            "error_message": error_message,
            "timestamp": datetime.now().isoformat(),
            "retry_count": 0,
            "retry_after": (datetime.now() + timedelta(hours=1)).isoformat(),
            "collection_attempted": True,
            "success": False
        }

        # Create error file
        error_file = self.error_path / f"{ticker}_{error_type}_{int(time.time())}.json"
        with open(error_file, 'w') as f:
            json.dump(error_record, f, indent=2)

        self.logger.error(f"Error logged for {ticker}: {error_type} - {error_message}")

async def main():
    """Test execution function"""

    # Configuration
    api_key = os.getenv('POLYGON_API_KEY')
    if not api_key:
        logger.error("POLYGON_API_KEY environment variable not set")
        sys.exit(1)

    logger.info(f"Using API key: {api_key[:10]}...")

    config = PolygonConfig(api_key=api_key)

    # Test tickers
    test_tickers = ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'NVDA']

    logger.info(f"Starting test collection for {len(test_tickers)} tickers")

    # Create collector and process test tickers
    async with PolygonDataCollector(config) as collector:
        successful = 0
        failed = 0

        for ticker in test_tickers:
            logger.info(f"Processing test ticker: {ticker}")
            result = await collector.collect_test_data(ticker, days=250)  # Need enough history for indicators

            if result:
                successful += 1
                logger.info(f"✅ Successfully collected data for {ticker}")
            else:
                failed += 1
                logger.error(f"❌ Failed to collect data for {ticker}")

            # Brief pause between tickers
            await asyncio.sleep(5)

    # Final summary
    logger.info(f"Test completed: {successful} successful, {failed} failed out of {len(test_tickers)} tickers")

    # Generate test summary report
    summary = {
        "test_date": datetime.now().isoformat(),
        "test_tickers": test_tickers,
        "successful_collections": successful,
        "failed_collections": failed,
        "success_rate": (successful / len(test_tickers)) * 100 if test_tickers else 0,
        "data_source": "polygon.io",
        "test_mode": True,
        "storage_path": "/workspaces/data/raw_data/polygon"
    }

    summary_path = Path("/workspaces/data/raw_data/polygon") / "test_summary.json"
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)

    logger.info(f"Test summary saved to: {summary_path}")

if __name__ == "__main__":
    asyncio.run(main())