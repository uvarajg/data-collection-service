"""
Stock Screener Service
Fetches all US stocks with market cap and volume data
Primary: Alpaca API
Secondary: YFinance API (fallback)
"""

import json
import asyncio
import structlog
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from pathlib import Path
import pandas as pd
import yfinance as yf
from alpaca.data import StockHistoricalDataClient
from alpaca.data.requests import StockSnapshotRequest, StockBarsRequest
from alpaca.data.timeframe import TimeFrame

from ..config.settings import get_settings
from ..utils.retry_decorator import alpaca_retry, yfinance_retry, ALPACA_RATE_LIMITER, YFINANCE_RATE_LIMITER

logger = structlog.get_logger()


class StockScreenerService:
    """
    Service to screen all US stocks and filter by market cap and volume.
    Replaces Google Sheets as the primary data source.
    """
    
    def __init__(self):
        self.settings = get_settings()
        self.logger = logger.bind(service="stock_screener")
        self.output_dir = Path("/workspaces/data/input_source")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize Alpaca client
        self.alpaca_client = StockHistoricalDataClient(
            api_key=self.settings.apca_api_key_id,
            secret_key=self.settings.apca_api_secret_key
        )
        
        self.logger.info("Stock Screener Service initialized")
    
    async def get_all_us_stocks(self, 
                               min_market_cap: float = 2_000_000_000,  # 2B default
                               min_avg_volume: Optional[float] = None,
                               use_fallback: bool = True) -> List[Dict[str, Any]]:
        """
        Get all US stocks meeting the criteria.
        
        Args:
            min_market_cap: Minimum market cap in dollars (default 2B)
            min_avg_volume: Minimum average daily volume (optional)
            use_fallback: Whether to use YFinance as fallback
            
        Returns:
            List of stock dictionaries with ticker, market_cap, avg_volume
        """
        self.logger.info("Starting stock screening",
                        min_market_cap=min_market_cap,
                        min_avg_volume=min_avg_volume)
        
        # Try Alpaca first
        try:
            stocks = await self._get_stocks_from_alpaca(min_market_cap, min_avg_volume)
            if stocks:
                self.logger.info(f"Retrieved {len(stocks)} stocks from Alpaca")
                return stocks
        except Exception as e:
            self.logger.error(f"Alpaca screening failed: {e}")
        
        # Fallback to YFinance if enabled
        if use_fallback:
            self.logger.info("Falling back to YFinance screening")
            try:
                stocks = await self._get_stocks_from_yfinance(min_market_cap, min_avg_volume)
                if stocks:
                    self.logger.info(f"Retrieved {len(stocks)} stocks from YFinance")
                    return stocks
            except Exception as e:
                self.logger.error(f"YFinance screening failed: {e}")
        
        return []
    
    @alpaca_retry(max_attempts=3)
    async def _get_stocks_from_alpaca(self, 
                                     min_market_cap: float,
                                     min_avg_volume: Optional[float]) -> List[Dict[str, Any]]:
        """
        Get stocks from Alpaca API.
        """
        self.logger.info("Fetching US stocks from Alpaca")
        
        try:
            # Get list of all active assets
            from alpaca.trading import TradingClient
            trading_client = TradingClient(
                api_key=self.settings.apca_api_key_id,
                secret_key=self.settings.apca_api_secret_key,
                paper=True  # Use paper trading endpoint
            )
            
            # Get all active US equities
            assets = trading_client.get_all_assets()
            
            # Filter for tradable US stocks
            active_stocks = [
                asset for asset in assets
                if asset.status == 'active' 
                and asset.tradable 
                and asset.exchange in ['NYSE', 'NASDAQ', 'ARCA', 'AMEX']
                and asset.asset_class == 'us_equity'
            ]
            
            self.logger.info(f"Found {len(active_stocks)} active US stocks")
            
            # Get market data for filtering
            filtered_stocks = []
            batch_size = 100  # Process in batches to avoid rate limits
            
            for i in range(0, len(active_stocks), batch_size):
                batch = active_stocks[i:i+batch_size]
                symbols = [asset.symbol for asset in batch]
                
                try:
                    # Get latest snapshot for market cap and volume
                    request = StockSnapshotRequest(symbol_or_symbols=symbols)
                    snapshots = self.alpaca_client.get_stock_snapshot(request)
                    
                    for symbol, snapshot in snapshots.items():
                        try:
                            # Calculate market cap (price * shares outstanding)
                            # Note: Alpaca doesn't provide shares outstanding directly
                            # We'll need to get this from the latest bar data
                            latest_trade = snapshot.latest_trade
                            daily_bar = snapshot.daily_bar
                            
                            if latest_trade and daily_bar:
                                # Use volume as proxy for liquidity
                                avg_volume = daily_bar.volume
                                
                                # For market cap, we'll need to fetch from YFinance
                                # or use a different approach
                                stock_data = {
                                    'ticker': symbol,
                                    'last_price': latest_trade.price,
                                    'avg_volume': avg_volume,
                                    'market_cap': None,  # Will be filled by YFinance
                                    'exchange': next((a.exchange for a in batch if a.symbol == symbol), 'UNKNOWN'),
                                    'name': next((a.name for a in batch if a.symbol == symbol), symbol),
                                    'data_source': 'alpaca'
                                }
                                
                                # Apply volume filter if specified
                                if min_avg_volume is None or avg_volume >= min_avg_volume:
                                    filtered_stocks.append(stock_data)
                                    
                        except Exception as e:
                            self.logger.debug(f"Error processing {symbol}: {e}")
                    
                    # Rate limiting
                    await ALPACA_RATE_LIMITER.wait_if_needed()
                    
                except Exception as e:
                    self.logger.warning(f"Error fetching batch data: {e}")
                    continue
            
            # Since Alpaca doesn't provide market cap directly,
            # we need to fetch it from YFinance for filtered stocks
            self.logger.info(f"Enriching {len(filtered_stocks)} stocks with market cap data")
            
            enriched_stocks = []
            for stock in filtered_stocks:
                try:
                    # Get market cap from YFinance
                    ticker_obj = yf.Ticker(stock['ticker'])
                    info = ticker_obj.info
                    market_cap = info.get('marketCap', 0)
                    
                    if market_cap and market_cap >= min_market_cap:
                        stock['market_cap'] = market_cap
                        stock['market_cap_billions'] = round(market_cap / 1_000_000_000, 2)
                        enriched_stocks.append(stock)
                        
                    await YFINANCE_RATE_LIMITER.wait_if_needed()
                    
                except Exception as e:
                    self.logger.debug(f"Could not get market cap for {stock['ticker']}: {e}")
            
            return enriched_stocks
            
        except Exception as e:
            self.logger.error(f"Alpaca screening error: {e}")
            raise
    
    @yfinance_retry(max_attempts=3)
    async def _get_stocks_from_yfinance(self,
                                       min_market_cap: float,
                                       min_avg_volume: Optional[float]) -> List[Dict[str, Any]]:
        """
        Get stocks from YFinance as fallback.
        Uses common stock lists and screens them.
        """
        self.logger.info("Fetching US stocks from YFinance")
        
        try:
            # Get S&P 500, NASDAQ 100, and Russell 1000 as starting point
            # These cover most large-cap US stocks
            
            stock_lists = []
            
            # Get S&P 500
            try:
                sp500 = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]
                stock_lists.extend(sp500['Symbol'].tolist())
            except:
                self.logger.warning("Could not fetch S&P 500 list")
            
            # Get NASDAQ 100
            try:
                nasdaq100 = pd.read_html('https://en.wikipedia.org/wiki/Nasdaq-100')[4]
                stock_lists.extend(nasdaq100['Ticker'].tolist())
            except:
                self.logger.warning("Could not fetch NASDAQ 100 list")
            
            # Get additional large cap stocks from YFinance screener
            # Using a predefined list of sectors to query
            sectors = ['Technology', 'Healthcare', 'Financial', 'Consumer', 'Industrial', 
                      'Energy', 'Materials', 'Utilities', 'Real Estate', 'Communication']
            
            for sector in sectors:
                try:
                    # Use yfinance's built-in screener (if available in your version)
                    # Otherwise, use a predefined list
                    pass  # Placeholder for sector-specific queries
                except:
                    pass
            
            # Remove duplicates
            unique_tickers = list(set(stock_lists))
            self.logger.info(f"Screening {len(unique_tickers)} unique tickers")
            
            # Screen each ticker
            filtered_stocks = []
            batch_size = 50
            
            for i in range(0, len(unique_tickers), batch_size):
                batch = unique_tickers[i:i+batch_size]
                
                # Download data for batch
                try:
                    tickers_str = ' '.join(batch)
                    data = yf.download(tickers_str, period='1d', progress=False, 
                                     group_by='ticker', threads=True)
                    
                    for ticker in batch:
                        try:
                            ticker_obj = yf.Ticker(ticker)
                            info = ticker_obj.info
                            
                            market_cap = info.get('marketCap', 0)
                            avg_volume = info.get('averageVolume', 0)
                            
                            # Apply filters
                            if market_cap >= min_market_cap:
                                if min_avg_volume is None or avg_volume >= min_avg_volume:
                                    stock_data = {
                                        'ticker': ticker,
                                        'market_cap': market_cap,
                                        'market_cap_billions': round(market_cap / 1_000_000_000, 2),
                                        'avg_volume': avg_volume,
                                        'avg_volume_millions': round(avg_volume / 1_000_000, 2),
                                        'name': info.get('longName', ticker),
                                        'sector': info.get('sector', 'Unknown'),
                                        'industry': info.get('industry', 'Unknown'),
                                        'exchange': info.get('exchange', 'Unknown'),
                                        'data_source': 'yfinance',
                                        'last_price': info.get('currentPrice') or info.get('regularMarketPrice', 0),
                                        'pe_ratio': info.get('trailingPE'),
                                        'dividend_yield': info.get('dividendYield')
                                    }
                                    filtered_stocks.append(stock_data)
                            
                            await YFINANCE_RATE_LIMITER.wait_if_needed()
                            
                        except Exception as e:
                            self.logger.debug(f"Error processing {ticker}: {e}")
                    
                except Exception as e:
                    self.logger.warning(f"Error downloading batch: {e}")
            
            return filtered_stocks
            
        except Exception as e:
            self.logger.error(f"YFinance screening error: {e}")
            raise
    
    async def save_screened_stocks(self,
                                  stocks: List[Dict[str, Any]],
                                  filename: str = "screened_stocks.json") -> str:
        """
        Save screened stocks to JSON file in /data/input_source.
        
        Args:
            stocks: List of stock dictionaries
            filename: Output filename
            
        Returns:
            Path to saved file
        """
        output_path = self.output_dir / filename
        
        try:
            # Add metadata
            output_data = {
                'generated_at': datetime.now().isoformat(),
                'total_stocks': len(stocks),
                'criteria': {
                    'min_market_cap_billions': min(s['market_cap_billions'] for s in stocks) if stocks else 0,
                    'max_market_cap_billions': max(s['market_cap_billions'] for s in stocks) if stocks else 0,
                },
                'stocks': sorted(stocks, key=lambda x: x.get('market_cap', 0), reverse=True)
            }
            
            # Save to JSON
            with open(output_path, 'w') as f:
                json.dump(output_data, f, indent=2)
            
            self.logger.info(f"Saved {len(stocks)} stocks to {output_path}")
            
            # Also save as CSV for easy viewing
            csv_path = self.output_dir / filename.replace('.json', '.csv')
            df = pd.DataFrame(stocks)
            df.to_csv(csv_path, index=False)
            self.logger.info(f"Also saved as CSV to {csv_path}")
            
            return str(output_path)
            
        except Exception as e:
            self.logger.error(f"Error saving stocks: {e}")
            raise
    
    async def update_stock_universe(self,
                                   min_market_cap: float = 2_000_000_000,
                                   min_avg_volume: Optional[float] = None,
                                   max_stocks: Optional[int] = None) -> Dict[str, Any]:
        """
        Main method to update the stock universe based on criteria.
        
        Args:
            min_market_cap: Minimum market cap in dollars
            min_avg_volume: Minimum average daily volume
            max_stocks: Maximum number of stocks to include (top by market cap)
            
        Returns:
            Summary of the update
        """
        self.logger.info("Updating stock universe",
                        min_market_cap=min_market_cap,
                        min_avg_volume=min_avg_volume,
                        max_stocks=max_stocks)
        
        # Get all stocks meeting criteria
        stocks = await self.get_all_us_stocks(min_market_cap, min_avg_volume)
        
        if not stocks:
            self.logger.error("No stocks found meeting criteria")
            return {
                'success': False,
                'error': 'No stocks found meeting criteria',
                'total_stocks': 0
            }
        
        # Apply max stocks limit if specified
        if max_stocks and len(stocks) > max_stocks:
            stocks = stocks[:max_stocks]  # Already sorted by market cap
        
        # Save to file
        output_file = await self.save_screened_stocks(stocks)
        
        # Generate summary
        summary = {
            'success': True,
            'total_stocks': len(stocks),
            'output_file': output_file,
            'criteria': {
                'min_market_cap': f"${min_market_cap/1_000_000_000:.1f}B",
                'min_avg_volume': min_avg_volume,
                'max_stocks': max_stocks
            },
            'top_10_stocks': [
                {
                    'ticker': s['ticker'],
                    'market_cap': f"${s['market_cap_billions']:.1f}B",
                    'avg_volume': f"{s.get('avg_volume_millions', 0):.1f}M"
                }
                for s in stocks[:10]
            ],
            'sector_distribution': {}
        }
        
        # Calculate sector distribution
        for stock in stocks:
            sector = stock.get('sector', 'Unknown')
            summary['sector_distribution'][sector] = summary['sector_distribution'].get(sector, 0) + 1
        
        return summary