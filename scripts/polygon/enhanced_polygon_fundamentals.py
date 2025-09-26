#!/usr/bin/env python3
"""
Enhanced Polygon.io Fundamental Data Collector

This script tests the collection of fundamental data using Polygon.io's
financial statement endpoints (balance sheet, income statement, cash flow).
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple

import aiohttp
from dotenv import load_dotenv

# Load environment variables
load_dotenv('/workspaces/data-collection-service/.env')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PolygonFundamentalsCollector:
    """Enhanced fundamental data collector using Polygon.io financial statements API"""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.polygon.io"
        self.session = None
        self.request_interval = 12  # 12 seconds between requests for rate limiting
        self.last_request_time = 0

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def _rate_limit(self):
        """Implement rate limiting"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.request_interval:
            wait_time = self.request_interval - time_since_last
            logger.info(f"Rate limiting: waiting {wait_time:.1f} seconds")
            await asyncio.sleep(wait_time)
        self.last_request_time = time.time()

    async def get_legacy_financials(self, ticker: str, limit: int = 4) -> Dict[str, Any]:
        """Get financial data using the legacy financials endpoint"""
        await self._rate_limit()

        url = f"{self.base_url}/v2/reference/financials/{ticker}"
        params = {
            'apikey': self.api_key,
            'limit': limit,
            'type': 'Y'  # Annual data
        }

        try:
            async with self.session.get(url, params=params) as response:
                logger.info(f"Balance sheet API response for {ticker}: {response.status}")

                if response.status == 200:
                    data = await response.json()
                    logger.info(f"Balance sheet data keys: {list(data.keys())}")

                    if data.get('status') in ['OK', 'DELAYED'] and 'results' in data:
                        results = data['results']
                        if results:
                            # Get the most recent balance sheet
                            latest = results[0]
                            logger.info(f"Latest balance sheet date: {latest.get('filing_date')}")
                            return latest

                response_text = await response.text()
                logger.warning(f"Balance sheet API error for {ticker}: {response_text}")

        except Exception as e:
            logger.error(f"Balance sheet request error for {ticker}: {e}")

        return {}

    async def get_income_statement(self, ticker: str, limit: int = 4) -> Dict[str, Any]:
        """Get income statement data for a ticker"""
        await self._rate_limit()

        url = f"{self.base_url}/rest/stocks/fundamentals/income-statements"
        params = {
            'ticker': ticker,
            'apikey': self.api_key,
            'limit': limit,
            'timeframe': 'annual'  # Get annual data
        }

        try:
            async with self.session.get(url, params=params) as response:
                logger.info(f"Income statement API response for {ticker}: {response.status}")

                if response.status == 200:
                    data = await response.json()
                    logger.info(f"Income statement data keys: {list(data.keys())}")

                    if data.get('status') in ['OK', 'DELAYED'] and 'results' in data:
                        results = data['results']
                        if results:
                            # Get the most recent income statement
                            latest = results[0]
                            logger.info(f"Latest income statement date: {latest.get('filing_date')}")
                            return latest

                response_text = await response.text()
                logger.warning(f"Income statement API error for {ticker}: {response_text}")

        except Exception as e:
            logger.error(f"Income statement request error for {ticker}: {e}")

        return {}

    async def get_cash_flow(self, ticker: str, limit: int = 4) -> Dict[str, Any]:
        """Get cash flow statement data for a ticker"""
        await self._rate_limit()

        url = f"{self.base_url}/rest/stocks/fundamentals/cash-flow-statements"
        params = {
            'ticker': ticker,
            'apikey': self.api_key,
            'limit': limit,
            'timeframe': 'annual'  # Get annual data
        }

        try:
            async with self.session.get(url, params=params) as response:
                logger.info(f"Cash flow API response for {ticker}: {response.status}")

                if response.status == 200:
                    data = await response.json()
                    logger.info(f"Cash flow data keys: {list(data.keys())}")

                    if data.get('status') in ['OK', 'DELAYED'] and 'results' in data:
                        results = data['results']
                        if results:
                            # Get the most recent cash flow statement
                            latest = results[0]
                            logger.info(f"Latest cash flow date: {latest.get('filing_date')}")
                            return latest

                response_text = await response.text()
                logger.warning(f"Cash flow API error for {ticker}: {response_text}")

        except Exception as e:
            logger.error(f"Cash flow request error for {ticker}: {e}")

        return {}

    async def get_ticker_details(self, ticker: str) -> Dict[str, Any]:
        """Get basic ticker details including market cap"""
        await self._rate_limit()

        url = f"{self.base_url}/v3/reference/tickers/{ticker}"
        params = {'apikey': self.api_key}

        try:
            async with self.session.get(url, params=params) as response:
                logger.info(f"Ticker details API response for {ticker}: {response.status}")

                if response.status == 200:
                    data = await response.json()
                    if data.get('status') in ['OK', 'DELAYED'] and 'results' in data:
                        return data['results']

                response_text = await response.text()
                logger.warning(f"Ticker details API error for {ticker}: {response_text}")

        except Exception as e:
            logger.error(f"Ticker details request error for {ticker}: {e}")

        return {}

    def calculate_fundamental_ratios(self, ticker: str, close_price: float,
                                   balance_sheet: Dict, income_statement: Dict,
                                   cash_flow: Dict, ticker_details: Dict) -> Dict[str, float]:
        """Calculate fundamental ratios from financial statement data"""

        ratios = {}

        try:
            # Market Cap (from ticker details or calculate from shares outstanding)
            market_cap = ticker_details.get('market_cap')
            if not market_cap and ticker_details.get('weighted_shares_outstanding'):
                market_cap = close_price * ticker_details['weighted_shares_outstanding']
            if market_cap:
                ratios['market_cap'] = market_cap

            # From Balance Sheet
            total_assets = balance_sheet.get('assets', {}).get('value')
            total_liabilities = balance_sheet.get('liabilities', {}).get('value')
            total_equity = balance_sheet.get('equity', {}).get('value')
            current_assets = balance_sheet.get('current_assets', {}).get('value')
            current_liabilities = balance_sheet.get('current_liabilities', {}).get('value')

            # From Income Statement
            net_income = income_statement.get('net_income_loss', {}).get('value')
            revenues = income_statement.get('revenues', {}).get('value')
            operating_income = income_statement.get('operating_income_loss', {}).get('value')

            # Basic earnings per share
            basic_eps = income_statement.get('basic_earnings_per_share', {}).get('value')

            # Shares outstanding (prefer from financials, fallback to ticker details)
            shares_outstanding = (income_statement.get('basic_average_shares', {}).get('value') or
                                ticker_details.get('weighted_shares_outstanding'))

            # Calculate ratios

            # P/E Ratio
            if basic_eps and basic_eps != 0:
                ratios['pe_ratio'] = close_price / basic_eps
            elif net_income and shares_outstanding and net_income > 0:
                eps = net_income / shares_outstanding
                ratios['pe_ratio'] = close_price / eps

            # Book Value per Share
            if total_equity and shares_outstanding:
                ratios['book_value'] = total_equity / shares_outstanding

            # Debt-to-Equity Ratio
            if total_liabilities and total_equity and total_equity != 0:
                ratios['debt_to_equity'] = total_liabilities / total_equity

            # Current Ratio
            if current_assets and current_liabilities and current_liabilities != 0:
                ratios['current_ratio'] = current_assets / current_liabilities

            # Return on Equity (ROE)
            if net_income and total_equity and total_equity != 0:
                ratios['roe_percent'] = (net_income / total_equity) * 100

            # Operating Margin
            if operating_income and revenues and revenues != 0:
                ratios['operating_margin_percent'] = (operating_income / revenues) * 100

            # Profit Margin (Net Margin)
            if net_income and revenues and revenues != 0:
                ratios['profit_margin_percent'] = (net_income / revenues) * 100

            # Note: Dividend yield and revenue growth require additional data
            # These could be calculated with historical data or separate API calls

            logger.info(f"Calculated {len(ratios)} fundamental ratios for {ticker}")

        except Exception as e:
            logger.error(f"Error calculating ratios for {ticker}: {e}")

        return ratios

    async def get_comprehensive_fundamentals(self, ticker: str, close_price: float) -> Dict[str, Any]:
        """Get comprehensive fundamental data for a ticker"""

        logger.info(f"Collecting comprehensive fundamentals for {ticker}")

        # Collect all financial data
        balance_sheet = await self.get_balance_sheet(ticker)
        income_statement = await self.get_income_statement(ticker)
        cash_flow = await self.get_cash_flow(ticker)
        ticker_details = await self.get_ticker_details(ticker)

        # Calculate ratios
        ratios = self.calculate_fundamental_ratios(
            ticker, close_price, balance_sheet, income_statement, cash_flow, ticker_details
        )

        # Return comprehensive data
        return {
            'ratios': ratios,
            'balance_sheet': balance_sheet,
            'income_statement': income_statement,
            'cash_flow': cash_flow,
            'ticker_details': ticker_details
        }

async def test_fundamentals_collection():
    """Test fundamental data collection with sample tickers"""

    api_key = os.getenv('POLYGON_API_KEY')
    if not api_key:
        logger.error("POLYGON_API_KEY not found")
        return

    test_tickers = [
        ('AAPL', 254.43),  # Apple with recent close price
        ('MSFT', 444.06),  # Microsoft with recent close price
    ]

    async with PolygonFundamentalsCollector(api_key) as collector:
        for ticker, close_price in test_tickers:
            logger.info(f"\n{'='*50}")
            logger.info(f"Testing fundamental data collection for {ticker}")
            logger.info(f"Close price: ${close_price}")
            logger.info(f"{'='*50}")

            fundamentals = await collector.get_comprehensive_fundamentals(ticker, close_price)

            print(f"\n📊 FUNDAMENTAL DATA FOR {ticker}:")
            print(f"Close Price: ${close_price}")

            ratios = fundamentals['ratios']
            if ratios:
                print("\n🔢 CALCULATED RATIOS:")
                for ratio, value in ratios.items():
                    if ratio == 'market_cap':
                        print(f"  Market Cap: ${value:,.0f}")
                    elif ratio == 'pe_ratio':
                        print(f"  P/E Ratio: {value:.2f}")
                    elif ratio == 'book_value':
                        print(f"  Book Value per Share: ${value:.2f}")
                    elif ratio == 'debt_to_equity':
                        print(f"  Debt-to-Equity: {value:.2f}")
                    elif ratio == 'current_ratio':
                        print(f"  Current Ratio: {value:.2f}")
                    elif ratio == 'roe_percent':
                        print(f"  ROE: {value:.2f}%")
                    elif ratio == 'operating_margin_percent':
                        print(f"  Operating Margin: {value:.2f}%")
                    elif ratio == 'profit_margin_percent':
                        print(f"  Profit Margin: {value:.2f}%")
            else:
                print("  ❌ No ratios calculated")

            # Show sample of raw data
            balance_sheet = fundamentals['balance_sheet']
            if balance_sheet:
                print(f"\n📋 BALANCE SHEET (Filing: {balance_sheet.get('filing_date', 'Unknown')}):")
                if 'assets' in balance_sheet:
                    print(f"  Total Assets: ${balance_sheet['assets'].get('value', 0):,.0f}")
                if 'liabilities' in balance_sheet:
                    print(f"  Total Liabilities: ${balance_sheet['liabilities'].get('value', 0):,.0f}")
                if 'equity' in balance_sheet:
                    print(f"  Total Equity: ${balance_sheet['equity'].get('value', 0):,.0f}")

            income_statement = fundamentals['income_statement']
            if income_statement:
                print(f"\n📈 INCOME STATEMENT (Filing: {income_statement.get('filing_date', 'Unknown')}):")
                if 'revenues' in income_statement:
                    print(f"  Revenues: ${income_statement['revenues'].get('value', 0):,.0f}")
                if 'net_income_loss' in income_statement:
                    print(f"  Net Income: ${income_statement['net_income_loss'].get('value', 0):,.0f}")
                if 'basic_earnings_per_share' in income_statement:
                    print(f"  EPS (Basic): ${income_statement['basic_earnings_per_share'].get('value', 0):.2f}")

            print(f"\n{'='*50}")

            # Brief pause between tickers
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(test_fundamentals_collection())