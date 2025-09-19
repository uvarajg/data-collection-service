import structlog
import asyncio
import yfinance as yf
from datetime import datetime
from typing import List, Optional, Dict, Any, Union, Tuple
import json
from pathlib import Path

from ..models.data_models import FundamentalData
from ..utils.retry_decorator import yfinance_retry, YFINANCE_RATE_LIMITER

logger = structlog.get_logger()


class YFinanceFundamentalsService:
    """
    YFinance fundamentals service that replicates AlgoAlchemist's yfinance-fundamentals-service.ts
    Uses yfinance Python library to fetch comprehensive fundamental financial data.
    """
    
    def __init__(self, cache_duration_hours: int = 4):
        self.cache_dir = Path("/workspaces/data/cache/yfinance")
        self.cache_duration_ms = cache_duration_hours * 60 * 60 * 1000  # 4 hours in milliseconds
        self.logger = logger.bind(service="yfinance_fundamentals")
        
        # Ensure cache directory exists
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        self.logger.info("YFinance Fundamentals Service initialized", cache_dir=str(self.cache_dir))
    
    @yfinance_retry(max_attempts=3)
    async def get_fundamentals(self, ticker: str) -> Optional[FundamentalData]:
        """
        Get fundamental data for a single ticker.
        
        Args:
            ticker: Stock symbol
            
        Returns:
            FundamentalData object or None if data unavailable
        """
        self.logger.info(f"Fetching comprehensive fundamental data for {ticker} via YFinance")
        
        # Check cache first
        cached_data = self._get_cached_data(ticker)
        if cached_data:
            self.logger.info(f"Using cached YFinance data for {ticker}")
            return cached_data
        
        try:
            # Fetch from YFinance
            fundamentals = await self._fetch_from_yfinance(ticker)
            
            if fundamentals:
                # Cache the successful response
                self._cache_data(ticker, fundamentals)
                return fundamentals
            
            self.logger.warning(f"No fundamental data available for {ticker}")
            return None
            
        except Exception as e:
            self.logger.error(f"Error fetching {ticker} from YFinance", error=str(e))
            return None
    
    async def get_batch_fundamentals(self, tickers: List[str]) -> Dict[str, Optional[FundamentalData]]:
        """
        Get batch fundamental data efficiently with rate limiting.
        
        Args:
            tickers: List of stock symbols
            
        Returns:
            Dictionary mapping ticker to FundamentalData or None
        """
        self.logger.info(f"Fetching comprehensive fundamental data for {len(tickers)} tickers via YFinance")
        
        results = {}
        
        # Process tickers sequentially to avoid rate limiting (matching AlgoAlchemist)
        for ticker in tickers:
            try:
                fundamentals = await self.get_fundamentals(ticker)
                results[ticker] = fundamentals
                
                # Apply centralized rate limiting
                await YFINANCE_RATE_LIMITER.wait_if_needed()
                
            except Exception as e:
                self.logger.error(f"Error fetching {ticker}", error=str(e))
                results[ticker] = None
        
        return results
    
    async def _fetch_from_yfinance(self, ticker: str) -> Optional[FundamentalData]:
        """
        Fetch fundamental data from Yahoo Finance using yfinance library.
        This replicates the logic from AlgoAlchemist's yfinance-fundamentals-service.ts
        """
        try:
            self.logger.info(f"Fetching {ticker} from Yahoo Finance API")
            
            # Create ticker object
            stock = yf.Ticker(ticker)
            
            # Fetch comprehensive data
            info = stock.info
            
            if not info or 'symbol' not in info:
                self.logger.warning(f"No data returned for {ticker}")
                return None
            
            # Extract market cap in millions (YFinance returns in actual value)
            market_cap = info.get('marketCap')
            market_cap_millions = market_cap / 1000000 if market_cap else None
            
            # Extract financial metrics following AlgoAlchemist patterns
            pe_ratio = info.get('trailingPE') or info.get('forwardPE')
            
            # Debt-to-equity with enhanced fallback calculation
            debt_to_equity = info.get('debtToEquity')
            if debt_to_equity and debt_to_equity > 10:  # Likely a percentage
                debt_to_equity = debt_to_equity / 100
            
            # ENHANCED: Add component-based fallback for missing debt-to-equity
            if debt_to_equity is None:
                debt_to_equity = self._calculate_debt_to_equity_fallback(stock, ticker, info)
            
            # ROE (Return on Equity)
            roe = info.get('returnOnEquity')
            roe_percent = (roe * 100) if roe else None
            
            # Current ratio with fallback calculation
            current_ratio = info.get('currentRatio')
            if current_ratio is None:
                current_ratio = self._calculate_current_ratio_fallback(stock, ticker, info)
            
            # Operating margin
            operating_margin = info.get('operatingMargins')
            operating_margin_percent = (operating_margin * 100) if operating_margin else None
            
            # Revenue growth
            revenue_growth = info.get('revenueGrowth')  
            revenue_growth_percent = (revenue_growth * 100) if revenue_growth else None
            
            # Profit margin with fallback calculation
            profit_margin = info.get('profitMargins')
            if profit_margin is None:
                profit_margin = self._calculate_profit_margin_fallback(stock, ticker, info)
            profit_margin_percent = (profit_margin * 100) if profit_margin else None
            
            # Dividend yield - Properly distinguish between non-dividend paying and missing data
            dividend_yield = info.get('dividendYield')
            
            # Check if this is a non-dividend paying stock vs missing data
            if dividend_yield is None:
                # Check comprehensive dividend indicators
                dividend_rate = info.get('dividendRate')  # Annual dividend per share
                trailing_annual_dividend = info.get('trailingAnnualDividendRate')
                ex_dividend_date = info.get('exDividendDate')
                last_dividend_value = info.get('lastDividendValue')
                five_year_avg_dividend = info.get('fiveYearAvgDividendYield')
                
                # BEST INDICATOR: Check dividend history
                try:
                    stock = yf.Ticker(ticker)
                    dividends = stock.dividends
                    has_dividend_history = dividends is not None and len(dividends) > 0
                except:
                    has_dividend_history = False
                
                # Decision logic based on yfinance patterns:
                # 1. If no dividend history AND no dividend fields, it's a non-payer
                # 2. If has history but no current yield, it's suspended/cut
                # 3. If only trailingAnnualDividendRate=0.0, likely non-payer
                
                if not has_dividend_history:
                    # No dividend history at all - definitively a non-dividend payer
                    if dividend_rate is None and ex_dividend_date is None:
                        self.logger.info(f"{ticker} has no dividend history - non-dividend payer (setting to 0.0)")
                        dividend_yield_percent = 0.0
                    else:
                        # Has some dividend data but no history - data inconsistency
                        self.logger.warning(f"{ticker} has conflicting dividend data - keeping as None")
                        dividend_yield_percent = None
                elif has_dividend_history and dividend_rate is None:
                    # Has history but no current dividend - likely suspended
                    self.logger.info(f"{ticker} has dividend history but no current dividend - suspended (keeping as None)")
                    dividend_yield_percent = None
                else:
                    # Has dividend history - try to calculate from available data
                    if dividend_rate is not None:
                        # Can calculate from dividend rate
                        current_price = info.get('currentPrice') or info.get('regularMarketPrice')
                        if current_price and current_price > 0:
                            dividend_yield_percent = (dividend_rate / current_price) * 100
                            self.logger.info(f"{ticker} calculated dividend yield from rate: {dividend_yield_percent:.2f}%")
                        else:
                            dividend_yield_percent = None
                    else:
                        # Has dividend history but no current data - likely suspended
                        self.logger.info(f"{ticker} has dividend history but no current data - suspended (keeping as None)")
                        dividend_yield_percent = None
            else:
                # Has dividend yield value
                dividend_yield_percent = dividend_yield  # Already in percentage format
            
            # Book value
            book_value = info.get('bookValue')
            
            self.logger.info(f"Successfully fetched fundamental data for {ticker} from YFinance")
            self.logger.debug(f"{ticker} Key Metrics", 
                            market_cap_millions=market_cap_millions,
                            debt_to_equity=debt_to_equity,
                            pe_ratio=pe_ratio,
                            current_ratio=current_ratio,
                            roe_percent=roe_percent)
            
            return FundamentalData(
                market_cap=market_cap_millions,
                pe_ratio=round(pe_ratio, 2) if pe_ratio else None,
                debt_to_equity=round(debt_to_equity, 4) if debt_to_equity else None,
                roe_percent=round(roe_percent, 2) if roe_percent else None,
                current_ratio=round(current_ratio, 2) if current_ratio else None,
                operating_margin_percent=round(operating_margin_percent, 2) if operating_margin_percent else None,
                revenue_growth_percent=round(revenue_growth_percent, 2) if revenue_growth_percent else None,
                profit_margin_percent=round(profit_margin_percent, 2) if profit_margin_percent else None,
                dividend_yield_percent=round(dividend_yield_percent, 2) if dividend_yield_percent is not None else None,
                book_value=round(book_value, 2) if book_value else None
            )
            
        except Exception as e:
            self.logger.error(f"Error fetching {ticker} from YFinance", error=str(e))
            
            # Check for specific error types (matching AlgoAlchemist error handling)
            error_msg = str(e).lower()
            if 'no data found' in error_msg or 'invalid ticker' in error_msg:
                self.logger.error(f"{ticker} is not a valid ticker symbol")
            elif 'too many requests' in error_msg or 'rate limit' in error_msg:
                self.logger.error(f"Rate limited by Yahoo Finance API")
            
            return None
    
    def _get_cached_data(self, ticker: str) -> Optional[FundamentalData]:
        """
        Check if cached data exists and is still valid.
        Cache duration is 4 hours like AlgoAlchemist.
        """
        cache_file = self.cache_dir / f"{ticker.upper()}.json"
        
        try:
            if cache_file.exists():
                # Check if cache is still valid (4 hours)
                file_age_ms = (datetime.now().timestamp() - cache_file.stat().st_mtime) * 1000
                
                if file_age_ms < self.cache_duration_ms:
                    with open(cache_file, 'r') as f:
                        data = json.load(f)
                    
                    # Convert back to FundamentalData object
                    return FundamentalData(
                        market_cap=data.get('market_cap'),
                        pe_ratio=data.get('pe_ratio'),
                        debt_to_equity=data.get('debt_to_equity'),
                        roe_percent=data.get('roe_percent'),
                        current_ratio=data.get('current_ratio'),
                        operating_margin_percent=data.get('operating_margin_percent'),
                        revenue_growth_percent=data.get('revenue_growth_percent'),
                        profit_margin_percent=data.get('profit_margin_percent'),
                        dividend_yield_percent=data.get('dividend_yield_percent'),
                        book_value=data.get('book_value')
                    )
        except Exception as e:
            self.logger.warning(f"Error reading cache for {ticker}", error=str(e))
        
        return None
    
    def _cache_data(self, ticker: str, data: FundamentalData) -> None:
        """
        Cache fundamental data to disk.
        """
        cache_file = self.cache_dir / f"{ticker.upper()}.json"
        
        try:
            cache_data = {
                'market_cap': data.market_cap,
                'pe_ratio': data.pe_ratio,
                'debt_to_equity': data.debt_to_equity,
                'roe_percent': data.roe_percent,
                'current_ratio': data.current_ratio,
                'operating_margin_percent': data.operating_margin_percent,
                'revenue_growth_percent': data.revenue_growth_percent,
                'profit_margin_percent': data.profit_margin_percent,
                'dividend_yield_percent': data.dividend_yield_percent,
                'book_value': data.book_value,
                'last_updated': datetime.now().isoformat(),
                'data_source': 'yfinance'
            }
            
            with open(cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2)
                
            self.logger.debug(f"Cached fundamental data for {ticker}")
            
        except Exception as e:
            self.logger.warning(f"Error caching data for {ticker}", error=str(e))
    
    async def get_sma_200_direct(self, ticker: str) -> Optional[float]:
        """
        Get pre-calculated 200-day SMA directly from Yahoo Finance.
        
        Returns:
            SMA_200 value or None if unavailable
        """
        try:
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
            
            self.logger.debug(f"No SMA_200 available from Yahoo Finance for {ticker}")
            return None
            
        except Exception as e:
            self.logger.error(f"Error fetching SMA_200 from Yahoo for {ticker}: {e}")
            return None
    
    async def check_dividend_history(self, ticker: str) -> Dict[str, Any]:
        """
        Check detailed dividend history for a ticker to determine if it's a dividend payer.
        
        Returns:
            Dictionary with dividend payment status and details
        """
        try:
            stock = yf.Ticker(ticker)
            
            # Get dividend history
            dividends = stock.dividends
            info = stock.info
            
            result = {
                'ticker': ticker,
                'is_dividend_payer': False,
                'has_dividend_history': False,
                'last_dividend_date': None,
                'annual_dividend': None,
                'dividend_frequency': None,
                'current_yield': None,
                'confidence': 'low'
            }
            
            # Check if has dividend history
            if dividends is not None and len(dividends) > 0:
                result['has_dividend_history'] = True
                result['last_dividend_date'] = str(dividends.index[-1].date())
                
                # Calculate annual dividend from last 12 months
                import pandas as pd
                # Use timezone-naive comparison to avoid timezone issues
                one_year_ago = pd.Timestamp.now().tz_localize(None) - pd.Timedelta(days=365)
                # Convert index to timezone-naive for comparison
                dividends_naive = dividends.copy()
                dividends_naive.index = dividends_naive.index.tz_localize(None)
                recent_dividends = dividends_naive[dividends_naive.index >= one_year_ago]
                
                if len(recent_dividends) > 0:
                    result['annual_dividend'] = float(recent_dividends.sum())
                    result['dividend_frequency'] = len(recent_dividends)
                    result['is_dividend_payer'] = True
                    result['confidence'] = 'high'
            
            # Get current yield from info
            current_yield = info.get('dividendYield')
            if current_yield and current_yield > 0:
                result['current_yield'] = current_yield
                result['is_dividend_payer'] = True
                result['confidence'] = 'high'
            elif result['has_dividend_history']:
                # Has history but no current yield - might be suspended
                result['confidence'] = 'medium'
            else:
                # No history and no yield - likely non-dividend payer
                result['is_dividend_payer'] = False
                result['confidence'] = 'high'
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error checking dividend history for {ticker}: {e}")
            return {
                'ticker': ticker,
                'is_dividend_payer': False,
                'error': str(e),
                'confidence': 'low'
            }
    
    async def get_all_moving_averages(self, ticker: str) -> Dict[str, Optional[float]]:
        """
        Get all available moving averages from Yahoo Finance.
        
        Returns:
            Dictionary with sma_50, sma_200, and other available averages
        """
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            
            return {
                'sma_50': info.get('fiftyDayAverage'),
                'sma_200': info.get('twoHundredDayAverage'),
                'current_price': info.get('currentPrice') or info.get('regularMarketPrice'),
                'source': 'yahoo_finance',
                'fetched_at': datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.error(f"Error fetching moving averages from Yahoo for {ticker}: {e}")
            return {'sma_50': None, 'sma_200': None, 'source': 'yahoo_finance', 'error': str(e)}
    
    def validate_fundamentals(self, ticker: str, data: FundamentalData) -> Dict[str, Any]:
        """
        Validate fundamental data and return quality assessment.
        """
        issues = []
        
        # Check for reasonable P/E ratio
        if data.pe_ratio and (data.pe_ratio < 0 or data.pe_ratio > 1000):
            issues.append(f"P/E ratio seems unreasonable: {data.pe_ratio}")
        
        # Check for reasonable debt-to-equity
        if data.debt_to_equity and data.debt_to_equity > 10:
            issues.append(f"Debt-to-equity ratio seems high: {data.debt_to_equity}")
        
        # Check for reasonable ROE
        if data.roe_percent and abs(data.roe_percent) > 200:
            issues.append(f"ROE seems unreasonable: {data.roe_percent}%")
        
        # Check for reasonable dividend yield (typically 0-15% for most stocks)
        if data.dividend_yield_percent and (data.dividend_yield_percent < 0 or data.dividend_yield_percent > 50):
            issues.append(f"Dividend yield seems unreasonable: {data.dividend_yield_percent}%")
        
        data_completeness = sum([
            1 for field in [data.market_cap, data.pe_ratio, data.debt_to_equity, 
                          data.roe_percent, data.current_ratio, data.operating_margin_percent]
            if field is not None
        ]) / 6  # 6 key fields
        
        return {
            "is_valid": len(issues) == 0,
            "issues": issues,
            "data_completeness": round(data_completeness, 2),
            "key_metrics_available": {
                "market_cap": data.market_cap is not None,
                "pe_ratio": data.pe_ratio is not None,
                "debt_to_equity": data.debt_to_equity is not None,
                "roe_percent": data.roe_percent is not None,
                "current_ratio": data.current_ratio is not None,
                "operating_margin": data.operating_margin_percent is not None
            }
        }
    
    def _calculate_debt_to_equity_fallback(self, stock, ticker: str, info: dict) -> Optional[float]:
        """
        Calculate debt-to-equity ratio from component balance sheet data when direct ratio is unavailable.
        This addresses the 12.9% missing debt-to-equity data issue.
        """
        try:
            self.logger.info(f"Attempting debt-to-equity fallback calculation for {ticker}")
            
            # Try to get balance sheet data
            try:
                balance_sheet = stock.balance_sheet
                if balance_sheet is not None and not balance_sheet.empty:
                    # Get the most recent quarter (first column)
                    latest_data = balance_sheet.iloc[:, 0]
                    
                    # Look for debt components (multiple possible keys)
                    total_debt = None
                    debt_keys = [
                        'Total Debt', 'Net Debt', 'Long Term Debt', 'Short Long Term Debt',
                        'Long Term Debt And Capital Lease Obligation', 'Current Debt'
                    ]
                    
                    for key in debt_keys:
                        if key in latest_data and latest_data[key] is not None:
                            if total_debt is None:
                                total_debt = latest_data[key]
                            else:
                                # Add to existing debt if it's a different component
                                if key in ['Current Debt', 'Short Long Term Debt']:
                                    total_debt += latest_data[key]
                    
                    # Look for equity components
                    equity_keys = [
                        'Stockholders Equity', 'Total Stockholders Equity', 
                        'Common Stockholders Equity', 'Total Equity Gross Minority Interest'
                    ]
                    
                    total_equity = None
                    for key in equity_keys:
                        if key in latest_data and latest_data[key] is not None:
                            total_equity = latest_data[key]
                            break
                    
                    # Calculate debt-to-equity if we have both components
                    if total_debt is not None and total_equity is not None and total_equity != 0:
                        calculated_ratio = abs(total_debt) / abs(total_equity)
                        self.logger.info(f"{ticker} calculated D/E from balance sheet: {calculated_ratio:.4f}")
                        return calculated_ratio
                    
                    # Log what we found for debugging
                    self.logger.info(f"{ticker} balance sheet components found",
                                   total_debt=total_debt is not None,
                                   total_equity=total_equity is not None,
                                   available_keys=list(latest_data.index[:10]))  # First 10 keys
                    
            except Exception as bs_error:
                self.logger.debug(f"{ticker} balance sheet access error: {str(bs_error)}")
            
            # Try quarterly balance sheet as fallback
            try:
                quarterly_bs = stock.quarterly_balance_sheet
                if quarterly_bs is not None and not quarterly_bs.empty:
                    latest_q = quarterly_bs.iloc[:, 0]
                    
                    # Same calculation logic for quarterly data
                    total_debt = None
                    for key in debt_keys:
                        if key in latest_q and latest_q[key] is not None:
                            if total_debt is None:
                                total_debt = latest_q[key]
                            else:
                                if key in ['Current Debt', 'Short Long Term Debt']:
                                    total_debt += latest_q[key]
                    
                    total_equity = None
                    for key in equity_keys:
                        if key in latest_q and latest_q[key] is not None:
                            total_equity = latest_q[key]
                            break
                    
                    if total_debt is not None and total_equity is not None and total_equity != 0:
                        calculated_ratio = abs(total_debt) / abs(total_equity)
                        self.logger.info(f"{ticker} calculated D/E from quarterly balance sheet: {calculated_ratio:.4f}")
                        return calculated_ratio
                        
            except Exception as qbs_error:
                self.logger.debug(f"{ticker} quarterly balance sheet access error: {str(qbs_error)}")
            
            # Try using info fields as last resort
            total_debt_info = info.get('totalDebt')
            total_equity_info = info.get('totalStockholderEquity')
            
            if total_debt_info and total_equity_info and total_equity_info != 0:
                calculated_ratio = abs(total_debt_info) / abs(total_equity_info)
                self.logger.info(f"{ticker} calculated D/E from info fields: {calculated_ratio:.4f}")
                return calculated_ratio
            
            self.logger.warning(f"{ticker} unable to calculate debt-to-equity from any source")
            return None
            
        except Exception as e:
            self.logger.error(f"{ticker} debt-to-equity fallback calculation failed: {str(e)}")
            return None
    
    def _calculate_current_ratio_fallback(self, stock, ticker: str, info: dict) -> Optional[float]:
        """
        Calculate current ratio from balance sheet components when direct ratio is unavailable.
        Current Ratio = Current Assets / Current Liabilities
        """
        try:
            self.logger.info(f"Attempting current ratio fallback calculation for {ticker}")
            
            # Try balance sheet first
            try:
                balance_sheet = stock.balance_sheet
                if balance_sheet is not None and not balance_sheet.empty:
                    latest_data = balance_sheet.iloc[:, 0]
                    
                    # Look for current assets
                    current_assets_keys = [
                        'Current Assets', 'Total Current Assets', 'CurrentAssets'
                    ]
                    current_assets = None
                    for key in current_assets_keys:
                        if key in latest_data and latest_data[key] is not None:
                            current_assets = latest_data[key]
                            break
                    
                    # Look for current liabilities
                    current_liab_keys = [
                        'Current Liabilities', 'Total Current Liabilities', 'CurrentLiabilities'
                    ]
                    current_liabilities = None
                    for key in current_liab_keys:
                        if key in latest_data and latest_data[key] is not None:
                            current_liabilities = latest_data[key]
                            break
                    
                    # Calculate current ratio
                    if current_assets and current_liabilities and current_liabilities != 0:
                        calculated_ratio = abs(current_assets) / abs(current_liabilities)
                        self.logger.info(f"{ticker} calculated current ratio from balance sheet: {calculated_ratio:.4f}")
                        return calculated_ratio
                        
            except Exception as bs_error:
                self.logger.debug(f"{ticker} balance sheet access error for current ratio: {str(bs_error)}")
            
            # Try quarterly balance sheet
            try:
                quarterly_bs = stock.quarterly_balance_sheet
                if quarterly_bs is not None and not quarterly_bs.empty:
                    latest_q = quarterly_bs.iloc[:, 0]
                    
                    current_assets = None
                    for key in current_assets_keys:
                        if key in latest_q and latest_q[key] is not None:
                            current_assets = latest_q[key]
                            break
                    
                    current_liabilities = None
                    for key in current_liab_keys:
                        if key in latest_q and latest_q[key] is not None:
                            current_liabilities = latest_q[key]
                            break
                    
                    if current_assets and current_liabilities and current_liabilities != 0:
                        calculated_ratio = abs(current_assets) / abs(current_liabilities)
                        self.logger.info(f"{ticker} calculated current ratio from quarterly balance sheet: {calculated_ratio:.4f}")
                        return calculated_ratio
                        
            except Exception as qbs_error:
                self.logger.debug(f"{ticker} quarterly balance sheet access error for current ratio: {str(qbs_error)}")
            
            self.logger.warning(f"{ticker} unable to calculate current ratio from any source")
            return None
            
        except Exception as e:
            self.logger.error(f"{ticker} current ratio fallback calculation failed: {str(e)}")
            return None
    
    def _calculate_profit_margin_fallback(self, stock, ticker: str, info: dict) -> Optional[float]:
        """
        Calculate profit margin from income statement components when direct margin is unavailable.
        Profit Margin = Net Income / Total Revenue
        """
        try:
            self.logger.info(f"Attempting profit margin fallback calculation for {ticker}")
            
            # Try income statement (financials)
            try:
                financials = stock.financials
                if financials is not None and not financials.empty:
                    latest_data = financials.iloc[:, 0]
                    
                    # Look for net income
                    net_income_keys = [
                        'Net Income', 'Net Income Common Stockholders', 'NetIncome'
                    ]
                    net_income = None
                    for key in net_income_keys:
                        if key in latest_data and latest_data[key] is not None:
                            net_income = latest_data[key]
                            break
                    
                    # Look for total revenue
                    revenue_keys = [
                        'Total Revenue', 'Revenue', 'TotalRevenue'
                    ]
                    total_revenue = None
                    for key in revenue_keys:
                        if key in latest_data and latest_data[key] is not None:
                            total_revenue = latest_data[key]
                            break
                    
                    # Calculate profit margin
                    if net_income is not None and total_revenue and total_revenue != 0:
                        calculated_margin = net_income / total_revenue  # Already as decimal
                        self.logger.info(f"{ticker} calculated profit margin from financials: {calculated_margin:.4f}")
                        return calculated_margin
                        
            except Exception as fin_error:
                self.logger.debug(f"{ticker} financials access error for profit margin: {str(fin_error)}")
            
            # Try quarterly financials
            try:
                quarterly_fin = stock.quarterly_financials
                if quarterly_fin is not None and not quarterly_fin.empty:
                    latest_q = quarterly_fin.iloc[:, 0]
                    
                    net_income = None
                    for key in net_income_keys:
                        if key in latest_q and latest_q[key] is not None:
                            net_income = latest_q[key]
                            break
                    
                    total_revenue = None
                    for key in revenue_keys:
                        if key in latest_q and latest_q[key] is not None:
                            total_revenue = latest_q[key]
                            break
                    
                    if net_income is not None and total_revenue and total_revenue != 0:
                        calculated_margin = net_income / total_revenue
                        self.logger.info(f"{ticker} calculated profit margin from quarterly financials: {calculated_margin:.4f}")
                        return calculated_margin
                        
            except Exception as qfin_error:
                self.logger.debug(f"{ticker} quarterly financials access error for profit margin: {str(qfin_error)}")
            
            self.logger.warning(f"{ticker} unable to calculate profit margin from any source")
            return None
            
        except Exception as e:
            self.logger.error(f"{ticker} profit margin fallback calculation failed: {str(e)}")
            return None