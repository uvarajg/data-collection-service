#!/usr/bin/env python3
"""
Historical Input Data Polygon

This script collects 5 years of historical data from Polygon.io API for all US stocks.
Uses the same technical indicator calculation logic as the existing data collection service
to maintain consistency and avoid discrepancies.

Requirements:
- Polygon.io Stocks Starter plan with paid API key
- Store data in /workspaces/data/raw_data/polygon in JSON format
- Use existing technical indicator calculation code
- Create new fundamental ratio calculations
- Comprehensive error handling with detailed logging

Author: Claude Code
Date: September 2025
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
from decimal import Decimal

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
        logging.FileHandler('/workspaces/data/raw_data/polygon/collection.log'),
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
    """
    Technical indicators calculator using the same logic as the existing service.
    Replicates /workspaces/data-collection-service/src/services/technical_indicators.py
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__ + ".technical")

    def calculate_indicators(self, records: List[StockDataPoint]) -> List[StockDataPoint]:
        """
        Calculate technical indicators for a list of stock data records.
        Uses identical logic from existing TechnicalIndicatorsService.
        """
        if not records or len(records) < 50:
            self.logger.warning(f"Insufficient data for technical indicators: {len(records)} records")
            return records

        # Sort records by date to ensure proper chronological order
        records = sorted(records, key=lambda r: r.date)

        # Verify chronological order
        dates = [r.date for r in records]
        for i in range(1, len(dates)):
            if dates[i] < dates[i-1]:
                raise ValueError(f"Data sequence error: {dates[i-1]} followed by {dates[i]}")

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
        """Calculate all technical indicators using TA library - identical to existing service"""

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

class FundamentalCalculator:
    """
    Calculate fundamental ratios from available market data.
    Creates new calculations as existing service doesn't have comprehensive fundamental data.
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__ + ".fundamental")

    def calculate_fundamentals(self, record: StockDataPoint, market_data: Dict[str, Any]) -> StockDataPoint:
        """
        Calculate fundamental ratios from market data.

        Args:
            record: Stock data point to enhance
            market_data: Additional market data from Polygon API

        Returns:
            Enhanced record with fundamental data
        """
        try:
            # Market Cap (from Polygon market_status endpoint)
            if 'market_cap' in market_data:
                record.market_cap = market_data['market_cap']

            # P/E Ratio calculation
            if 'earnings_per_share' in market_data and market_data['earnings_per_share']:
                record.pe_ratio = record.close / market_data['earnings_per_share']

            # Book Value calculation
            if 'book_value_per_share' in market_data:
                record.book_value = market_data['book_value_per_share']

            # Dividend Yield calculation
            if 'annual_dividend' in market_data and record.close:
                record.dividend_yield_percent = (market_data['annual_dividend'] / record.close) * 100

            # Debt to Equity ratio
            if 'total_debt' in market_data and 'total_equity' in market_data:
                if market_data['total_equity'] != 0:
                    record.debt_to_equity = market_data['total_debt'] / market_data['total_equity']

            # ROE Percentage
            if 'net_income' in market_data and 'total_equity' in market_data:
                if market_data['total_equity'] != 0:
                    record.roe_percent = (market_data['net_income'] / market_data['total_equity']) * 100

            # Current Ratio
            if 'current_assets' in market_data and 'current_liabilities' in market_data:
                if market_data['current_liabilities'] != 0:
                    record.current_ratio = market_data['current_assets'] / market_data['current_liabilities']

            # Operating Margin Percentage
            if 'operating_income' in market_data and 'revenue' in market_data:
                if market_data['revenue'] != 0:
                    record.operating_margin_percent = (market_data['operating_income'] / market_data['revenue']) * 100

            # Profit Margin Percentage
            if 'net_income' in market_data and 'revenue' in market_data:
                if market_data['revenue'] != 0:
                    record.profit_margin_percent = (market_data['net_income'] / market_data['revenue']) * 100

            # Revenue Growth Percentage (requires historical data)
            if 'revenue_growth_rate' in market_data:
                record.revenue_growth_percent = market_data['revenue_growth_rate'] * 100

            self.logger.debug(f"Fundamental ratios calculated for {record.ticker}")

        except Exception as e:
            self.logger.warning(f"Error calculating fundamentals for {record.ticker}: {e}")

        return record

class PolygonDataCollector:
    """
    Main data collector for Polygon.io API with comprehensive error handling.
    """

    def __init__(self, config: PolygonConfig):
        self.config = config
        self.session = None
        # Simple rate limiting with timestamps (5 requests per minute)
        self.request_times = []
        self.rate_limit_period = 60  # seconds
        self.max_requests_per_period = config.rate_limit
        self.technical_calculator = TechnicalIndicatorsCalculator()
        self.fundamental_calculator = FundamentalCalculator()
        self.logger = logging.getLogger(__name__ + ".collector")

        # Create output directories
        self.base_path = Path("/workspaces/data/raw_data/polygon")
        self.error_path = self.base_path / "error_records"
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.error_path.mkdir(parents=True, exist_ok=True)

    async def _rate_limit(self):
        """Simple rate limiter implementation"""
        now = time.time()
        # Remove timestamps older than rate_limit_period
        self.request_times = [t for t in self.request_times if now - t < self.rate_limit_period]

        # If we've hit the limit, wait
        if len(self.request_times) >= self.max_requests_per_period:
            sleep_time = self.rate_limit_period - (now - self.request_times[0]) + 1
            if sleep_time > 0:
                self.logger.info(f"Rate limit reached, sleeping for {sleep_time:.1f} seconds")
                await asyncio.sleep(sleep_time)
                # Clean up old timestamps after sleeping
                now = time.time()
                self.request_times = [t for t in self.request_times if now - t < self.rate_limit_period]

        # Record this request
        self.request_times.append(time.time())

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

    async def collect_historical_data(self, ticker: str, years: int = 5, min_market_cap: float = 2_000_000_000) -> bool:
        """
        Collect 5 years of historical data for a specific ticker.
        Only stores data if market cap > $2B.

        Args:
            ticker: Stock symbol
            years: Number of years of historical data
            min_market_cap: Minimum market cap threshold (default $2B)

        Returns:
            True if successful (and meets market cap requirement), False otherwise
        """
        try:
            # FIRST: Get financial data to check market cap
            self.logger.info(f"Checking market cap for {ticker}...")
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
                self.logger.info(f"❌ Skipping {ticker}: Market cap ${market_cap:,.0f} < ${min_market_cap:,.0f} threshold")
                await self._log_error(ticker, "market_cap_filter", f"Market cap ${market_cap:,.0f} below ${min_market_cap:,.0f} threshold")
                return False

            self.logger.info(f"✅ {ticker} passes market cap filter: ${market_cap:,.0f} > ${min_market_cap:,.0f}")

            # Calculate date range
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=years * 365)

            self.logger.info(f"Collecting {years} years of data for {ticker}: {start_date} to {end_date}")

            # NOW collect OHLCV data (only if market cap passes)
            ohlcv_data = await self._get_ohlcv_data(ticker, start_date, end_date)
            if not ohlcv_data:
                await self._log_error(ticker, "ohlcv_collection", "No OHLCV data received")
                return False

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

            # Store data
            await self._store_data(ticker, records)

            self.logger.info(f"Successfully collected {len(records)} records for {ticker}")
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
            'limit': 50000  # Maximum allowed
        }

        for attempt in range(self.config.max_retries):
            try:
                await self._rate_limit()
                async with self.session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get('status') == 'OK' and 'results' in data:
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
                            return results
                        else:
                            self.logger.warning(f"No data for {ticker}: {data}")
                            return []
                    elif response.status == 429:  # Rate limited
                        self.logger.warning(f"Rate limited for {ticker}, waiting {self.config.retry_delay}s")
                        await asyncio.sleep(self.config.retry_delay)
                        continue
                    else:
                        self.logger.error(f"API error for {ticker}: {response.status}")
                        return []

            except Exception as e:
                self.logger.error(f"Request error for {ticker} (attempt {attempt + 1}): {e}")
                if attempt < self.config.max_retries - 1:
                    await asyncio.sleep(self.config.retry_delay)

        return []

    async def _get_financials_data(self, ticker: str) -> Dict[str, Any]:
        """Get comprehensive financial data from Polygon legacy financials endpoint"""

        url = f"{self.config.base_url}/v2/reference/financials/{ticker}"
        params = {
            'apikey': self.config.api_key,
            'limit': 4,
            'type': 'Y'  # Annual data
        }

        try:
            await self._rate_limit()
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get('status') in ['OK', 'DELAYED'] and 'results' in data:
                        results = data['results']
                        if results:
                            # Get the most recent financial data
                            latest = results[0]
                            self.logger.debug(f"Retrieved financial data for {ticker} (period: {latest.get('reportPeriod', 'Unknown')})")
                            return latest
                else:
                    self.logger.warning(f"Could not get financial data for {ticker}: {response.status}")
        except Exception as e:
            self.logger.warning(f"Could not get financial data for {ticker}: {e}")

        return {}

    async def _get_market_data(self, ticker: str) -> Dict[str, Any]:
        """Get additional market data for fundamental calculations"""

        # Try to get ticker details
        url = f"{self.config.base_url}/v3/reference/tickers/{ticker}"
        params = {'apikey': self.config.api_key}

        try:
            await self._rate_limit()
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get('status') == 'OK' and 'results' in data:
                        result = data['results']
                        return {
                            'market_cap': result.get('market_cap'),
                            'weighted_shares_outstanding': result.get('weighted_shares_outstanding'),
                            'description': result.get('description'),
                            'homepage_url': result.get('homepage_url'),
                            'total_employees': result.get('total_employees')
                        }
        except Exception as e:
            self.logger.warning(f"Could not get market data for {ticker}: {e}")

        return {}

    async def _store_data(self, ticker: str, records: List[StockDataPoint]):
        """Store collected data in JSON format matching existing structure"""

        # Group records by date for individual file storage
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

async def get_all_us_stocks(api_key: str) -> List[str]:
    """
    Get list of all US stocks from Polygon API.
    Returns list of ticker symbols.
    """
    tickers = []
    url = "https://api.polygon.io/v3/reference/tickers"

    async with aiohttp.ClientSession() as session:
        params = {
            'apikey': api_key,
            'market': 'stocks',
            'exchange': 'XNYS,XNAS',  # NYSE and NASDAQ
            'active': 'true',
            'limit': 1000
        }

        next_url = None
        page = 1

        while True:
            try:
                if next_url:
                    current_url = f"https://api.polygon.io{next_url}&apikey={api_key}"
                    async with session.get(current_url) as response:
                        data = await response.json()
                else:
                    async with session.get(url, params=params) as response:
                        data = await response.json()

                if data.get('status') == 'OK' and 'results' in data:
                    for ticker_info in data['results']:
                        tickers.append(ticker_info['ticker'])

                    logger.info(f"Retrieved page {page}: {len(data['results'])} tickers (total: {len(tickers)})")

                    # Check for next page
                    if 'next_url' in data:
                        next_url = data['next_url']
                        page += 1
                        await asyncio.sleep(12)  # Rate limiting for free tier
                    else:
                        break
                else:
                    logger.error(f"Error retrieving tickers: {data}")
                    break

            except Exception as e:
                logger.error(f"Error fetching tickers page {page}: {e}")
                break

    logger.info(f"Retrieved {len(tickers)} US stock tickers")
    return tickers

async def main():
    """Main execution function"""

    # Configuration
    api_key = os.getenv('POLYGON_API_KEY')
    if not api_key:
        logger.error("POLYGON_API_KEY environment variable not set")
        sys.exit(1)

    config = PolygonConfig(api_key=api_key)

    # Get list of all US stocks
    logger.info("Retrieving list of all US stocks...")
    tickers = await get_all_us_stocks(api_key)

    if not tickers:
        logger.error("No tickers retrieved. Exiting.")
        sys.exit(1)

    logger.info(f"Starting collection for {len(tickers)} tickers")

    # Create collector and process tickers
    async with PolygonDataCollector(config) as collector:
        successful = 0
        failed = 0

        # Process tickers in batches to avoid overwhelming the API
        batch_size = 10
        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i + batch_size]

            logger.info(f"Processing batch {i//batch_size + 1}/{(len(tickers) + batch_size - 1)//batch_size}")

            # Process batch concurrently
            tasks = [collector.collect_historical_data(ticker) for ticker in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Count results
            for result in results:
                if isinstance(result, Exception):
                    failed += 1
                    logger.error(f"Batch processing exception: {result}")
                elif result:
                    successful += 1
                else:
                    failed += 1

            # Brief pause between batches
            await asyncio.sleep(15)

            # Progress report
            total_processed = successful + failed
            logger.info(f"Progress: {total_processed}/{len(tickers)} ({successful} successful, {failed} failed)")

    # Final summary
    logger.info(f"Collection completed: {successful} successful, {failed} failed out of {len(tickers)} tickers")

    # Generate summary report
    summary = {
        "collection_date": datetime.now().isoformat(),
        "total_tickers": len(tickers),
        "successful_collections": successful,
        "failed_collections": failed,
        "success_rate": (successful / len(tickers)) * 100 if tickers else 0,
        "data_source": "polygon.io",
        "years_collected": 5,
        "storage_path": "/workspaces/data/raw_data/polygon"
    }

    summary_path = Path("/workspaces/data/raw_data/polygon") / "collection_summary.json"
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)

    logger.info(f"Collection summary saved to: {summary_path}")

if __name__ == "__main__":
    asyncio.run(main())