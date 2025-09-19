"""
Fundamentals service that uses enriched YFinance JSON data instead of making fresh API calls.
Falls back to fresh API only if data is older than 24 hours or not found.
"""

import json
import os
import glob
import structlog
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from pathlib import Path
import yfinance as yf

from ..models.data_models import FundamentalData
from ..utils.retry_decorator import yfinance_retry, YFINANCE_RATE_LIMITER

logger = structlog.get_logger()


class EnrichedFundamentalsService:
    """
    Fundamentals service that primarily uses enriched YFinance JSON data
    to avoid unnecessary API calls. Falls back to fresh API only when needed.
    """

    def __init__(self, base_path: str = "/workspaces/data/input_source"):
        self.base_path = base_path
        self.enriched_data = None
        self.enriched_data_timestamp = None
        self.logger = logger.bind(service="enriched_fundamentals")

        # Load enriched data on initialization
        self._load_enriched_data()

    def _load_enriched_data(self) -> bool:
        """Load the latest enriched YFinance data file."""
        try:
            # Find the latest enriched file
            pattern = os.path.join(self.base_path, "enriched_yfinance_*.json")
            files = glob.glob(pattern)

            if not files:
                # Check for compressed files
                pattern_gz = os.path.join(self.base_path, "enriched_yfinance_*.json.gz")
                compressed_files = glob.glob(pattern_gz)

                if compressed_files:
                    import gzip
                    latest_file = max(compressed_files)

                    with gzip.open(latest_file, 'rt') as f:
                        self.enriched_data = json.load(f)

                    # Extract timestamp from filename
                    self._extract_file_timestamp(latest_file)
                    self.logger.info(f"Loaded compressed enriched data",
                                   file=latest_file,
                                   stocks_count=len(self.enriched_data))
                    return True

                self.logger.warning("No enriched YFinance files found")
                return False

            # Get the most recent file
            latest_file = max(files)

            # Load the data
            with open(latest_file, 'r') as f:
                self.enriched_data = json.load(f)

            # Extract timestamp from filename
            self._extract_file_timestamp(latest_file)

            self.logger.info(f"Loaded enriched data",
                           file=latest_file,
                           stocks_count=len(self.enriched_data) if self.enriched_data else 0,
                           timestamp=self.enriched_data_timestamp)
            return True

        except Exception as e:
            self.logger.error(f"Failed to load enriched data", error=str(e))
            return False

    def _extract_file_timestamp(self, filepath: str):
        """Extract timestamp from filename like enriched_yfinance_20250914_104337.json"""
        try:
            import re
            match = re.search(r'(\d{8}_\d{6})', filepath)
            if match:
                timestamp_str = match.group(1)
                # Parse the timestamp
                self.enriched_data_timestamp = datetime.strptime(timestamp_str, '%Y%m%d_%H%M%S')
            else:
                # Fall back to file modification time
                self.enriched_data_timestamp = datetime.fromtimestamp(os.path.getmtime(filepath))
        except Exception as e:
            self.logger.warning(f"Could not extract timestamp from filename", error=str(e))
            self.enriched_data_timestamp = datetime.now()

    def _is_data_fresh(self) -> bool:
        """Check if enriched data is less than 24 hours old."""
        if not self.enriched_data_timestamp:
            return False

        age = datetime.now() - self.enriched_data_timestamp
        is_fresh = age < timedelta(hours=24)

        self.logger.debug(f"Data freshness check",
                        age_hours=age.total_seconds() / 3600,
                        is_fresh=is_fresh)
        return is_fresh

    def _get_from_enriched(self, ticker: str) -> Optional[FundamentalData]:
        """Get fundamental data from enriched JSON."""
        if not self.enriched_data:
            return None

        # Find the ticker in enriched data
        stock_data = None
        for stock in self.enriched_data:
            if stock.get('ticker') == ticker.upper():
                stock_data = stock
                break

        if not stock_data:
            self.logger.debug(f"Ticker {ticker} not found in enriched data")
            return None

        try:
            # Extract fundamentals from enriched data
            # Convert values to the expected format

            # Market cap - convert to millions
            market_cap = stock_data.get('yf_market_cap')
            market_cap_millions = market_cap / 1000000 if market_cap else None

            # Percentages - convert from decimal to percentage
            # These fields are stored as decimals (0.608 = 60.8%)
            operating_margin = stock_data.get('yf_operating_margins')
            operating_margin_percent = (operating_margin * 100) if operating_margin is not None else None

            revenue_growth = stock_data.get('yf_revenue_growth')
            revenue_growth_percent = (revenue_growth * 100) if revenue_growth is not None else None

            profit_margin = stock_data.get('yf_profit_margins')
            profit_margin_percent = (profit_margin * 100) if profit_margin is not None else None

            # ROE - might be stored as decimal or percentage
            roe = stock_data.get('yf_return_on_equity')
            if roe is not None:
                # If ROE > 2, it's likely already a percentage
                roe_percent = roe if roe > 2 else (roe * 100)
            else:
                roe_percent = None

            # Dividend yield - usually already in percentage format
            dividend_yield = stock_data.get('yf_dividend_yield')
            dividend_yield_percent = dividend_yield if dividend_yield is not None else None

            self.logger.info(f"Retrieved fundamentals from enriched data for {ticker}")

            return FundamentalData(
                market_cap=market_cap_millions,
                pe_ratio=stock_data.get('yf_pe_ratio'),
                debt_to_equity=stock_data.get('yf_debt_to_equity'),
                roe_percent=roe_percent,
                current_ratio=stock_data.get('yf_current_ratio'),
                operating_margin_percent=operating_margin_percent,
                revenue_growth_percent=revenue_growth_percent,
                profit_margin_percent=profit_margin_percent,
                dividend_yield_percent=dividend_yield_percent,
                book_value=stock_data.get('yf_book_value')
            )

        except Exception as e:
            self.logger.error(f"Error extracting fundamentals from enriched data",
                            ticker=ticker, error=str(e))
            return None

    @yfinance_retry(max_attempts=3)
    async def _fetch_fresh_from_yfinance(self, ticker: str) -> Optional[FundamentalData]:
        """
        Fallback: Fetch fresh fundamental data from Yahoo Finance API.
        This is only used when enriched data is stale or ticker not found.
        """
        try:
            self.logger.info(f"Fetching fresh fundamentals from YFinance API for {ticker}")

            # Create ticker object
            stock = yf.Ticker(ticker)

            # Fetch comprehensive data
            info = stock.info

            if not info or 'symbol' not in info:
                self.logger.warning(f"No data returned for {ticker}")
                return None

            # Extract market cap in millions
            market_cap = info.get('marketCap')
            market_cap_millions = market_cap / 1000000 if market_cap else None

            # Extract financial metrics
            pe_ratio = info.get('trailingPE') or info.get('forwardPE')

            # Debt-to-equity
            debt_to_equity = info.get('debtToEquity')
            if debt_to_equity and debt_to_equity > 10:
                debt_to_equity = debt_to_equity / 100

            # ROE
            roe = info.get('returnOnEquity')
            roe_percent = (roe * 100) if roe else None

            # Current ratio
            current_ratio = info.get('currentRatio')

            # Operating margin
            operating_margin = info.get('operatingMargins')
            operating_margin_percent = (operating_margin * 100) if operating_margin else None

            # Revenue growth
            revenue_growth = info.get('revenueGrowth')
            revenue_growth_percent = (revenue_growth * 100) if revenue_growth else None

            # Profit margin
            profit_margin = info.get('profitMargins')
            profit_margin_percent = (profit_margin * 100) if profit_margin else None

            # Dividend yield
            dividend_yield = info.get('dividendYield')

            # Book value
            book_value = info.get('bookValue')

            self.logger.info(f"Successfully fetched fresh fundamentals for {ticker}")

            return FundamentalData(
                market_cap=market_cap_millions,
                pe_ratio=round(pe_ratio, 2) if pe_ratio else None,
                debt_to_equity=round(debt_to_equity, 4) if debt_to_equity else None,
                roe_percent=round(roe_percent, 2) if roe_percent else None,
                current_ratio=round(current_ratio, 2) if current_ratio else None,
                operating_margin_percent=round(operating_margin_percent, 2) if operating_margin_percent else None,
                revenue_growth_percent=round(revenue_growth_percent, 2) if revenue_growth_percent else None,
                profit_margin_percent=round(profit_margin_percent, 2) if profit_margin_percent else None,
                dividend_yield_percent=round(dividend_yield, 2) if dividend_yield is not None else None,
                book_value=round(book_value, 2) if book_value else None
            )

        except Exception as e:
            self.logger.error(f"Error fetching fresh data from YFinance",
                            ticker=ticker, error=str(e))
            return None

    async def get_fundamentals(self, ticker: str) -> Optional[FundamentalData]:
        """
        Get fundamental data for a ticker.
        Priority:
        1. Use enriched data if fresh (<24 hours)
        2. Fallback to fresh API call if stale or not found
        """
        # First, try to get from enriched data if it's fresh
        if self._is_data_fresh():
            fundamentals = self._get_from_enriched(ticker)
            if fundamentals:
                self.logger.debug(f"Using enriched data for {ticker}")
                return fundamentals
            else:
                # Ticker not found in enriched data, need fresh API call
                self.logger.info(f"Ticker {ticker} not in enriched data, fetching from API")
        else:
            # Data is stale, reload it first
            self.logger.info("Enriched data is stale (>24 hours), attempting reload")
            if self._load_enriched_data() and self._is_data_fresh():
                # Try again with reloaded data
                fundamentals = self._get_from_enriched(ticker)
                if fundamentals:
                    return fundamentals

            self.logger.info(f"Falling back to fresh API call for {ticker}")

        # Fallback to fresh API call
        fundamentals = await self._fetch_fresh_from_yfinance(ticker)

        # Apply rate limiting for API calls
        if fundamentals:
            await YFINANCE_RATE_LIMITER.wait_if_needed()

        return fundamentals

    async def get_batch_fundamentals(self, tickers: List[str]) -> Dict[str, Optional[FundamentalData]]:
        """
        Get batch fundamental data efficiently.
        Most will come from enriched data, only missing/stale will use API.
        """
        self.logger.info(f"Getting fundamentals for {len(tickers)} tickers")

        results = {}
        api_calls_needed = []

        # Check if enriched data is fresh
        if self._is_data_fresh():
            # Process all tickers from enriched data first
            for ticker in tickers:
                fundamentals = self._get_from_enriched(ticker)
                if fundamentals:
                    results[ticker] = fundamentals
                else:
                    # Mark for API call
                    api_calls_needed.append(ticker)

            self.logger.info(f"Retrieved {len(results)} from enriched data, "
                           f"{len(api_calls_needed)} need API calls")
        else:
            # All need API calls if data is stale
            api_calls_needed = tickers
            self.logger.info("Enriched data is stale, all tickers need API calls")

        # Fetch missing tickers via API
        for ticker in api_calls_needed:
            try:
                fundamentals = await self._fetch_fresh_from_yfinance(ticker)
                results[ticker] = fundamentals

                # Apply rate limiting
                await YFINANCE_RATE_LIMITER.wait_if_needed()

            except Exception as e:
                self.logger.error(f"Error fetching {ticker}", error=str(e))
                results[ticker] = None

        return results

    def get_data_age(self) -> Optional[timedelta]:
        """Get the age of the enriched data."""
        if self.enriched_data_timestamp:
            return datetime.now() - self.enriched_data_timestamp
        return None

    async def get_sma_200_direct(self, ticker: str) -> Optional[float]:
        """
        Get pre-calculated 200-day SMA.
        Priority:
        1. Use enriched data if fresh (<24 hours)
        2. Fallback to fresh API call if stale or not found
        """
        # First try enriched data if available and fresh
        if self._is_data_fresh() and self.enriched_data:
            for stock in self.enriched_data:
                if stock.get('ticker') == ticker.upper():
                    sma_200 = stock.get('yf_200_day_average')
                    if sma_200 and sma_200 > 0:
                        self.logger.info(f"SMA_200 retrieved from enriched data for {ticker}: {sma_200}")
                        return float(sma_200)
                    break

        # Fallback to fresh API call
        try:
            import yfinance as yf
            self.logger.info(f"Fetching SMA_200 from YFinance API for {ticker}")

            stock = yf.Ticker(ticker)
            info = stock.info

            # Yahoo provides this as 'twoHundredDayAverage'
            sma_200 = info.get('twoHundredDayAverage')

            if sma_200 and sma_200 > 0:
                # Validate it's reasonable
                current_price = info.get('currentPrice') or info.get('regularMarketPrice') or info.get('previousClose')
                if current_price:
                    ratio = sma_200 / current_price
                    if 0.3 <= ratio <= 3.0:  # Within reasonable bounds
                        self.logger.info(f"SMA_200 fetched from Yahoo Finance for {ticker}: {sma_200}")
                        return float(sma_200)
                    else:
                        self.logger.warning(f"SMA_200 for {ticker} outside reasonable bounds: {sma_200} vs price {current_price}")
                        return None
                else:
                    # No price to validate against, but return it anyway
                    self.logger.info(f"SMA_200 fetched from Yahoo Finance for {ticker}: {sma_200} (no price validation)")
                    return float(sma_200)
            else:
                self.logger.warning(f"No SMA_200 available for {ticker}")
                return None

        except Exception as e:
            self.logger.error(f"Error fetching SMA_200 for {ticker}", error=str(e))
            return None

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about the enriched data."""
        stats = {
            'loaded': self.enriched_data is not None,
            'timestamp': self.enriched_data_timestamp.isoformat() if self.enriched_data_timestamp else None,
            'age_hours': self.get_data_age().total_seconds() / 3600 if self.get_data_age() else None,
            'is_fresh': self._is_data_fresh(),
            'total_stocks': len(self.enriched_data) if self.enriched_data else 0
        }
        return stats