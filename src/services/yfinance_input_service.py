import json
import os
import glob
from typing import List, Optional, Dict, Any
from datetime import datetime
import structlog

logger = structlog.get_logger()

class YFinanceInputService:
    """Service for fetching ticker data from enriched YFinance JSON files"""

    def __init__(self, base_path: str = "/workspaces/data/input_source"):
        self.base_path = base_path
        self.logger = logger.bind(service="yfinance_input")

    def get_latest_enriched_file(self) -> Optional[str]:
        """Find the latest enriched_yfinance_*.json file"""
        try:
            # Pattern to match enriched YFinance files
            pattern = os.path.join(self.base_path, "enriched_yfinance_*.json")

            # Find all matching files
            files = glob.glob(pattern)

            if not files:
                # Check for compressed files as well
                pattern_gz = os.path.join(self.base_path, "enriched_yfinance_*.json.gz")
                compressed_files = glob.glob(pattern_gz)

                if compressed_files:
                    self.logger.warning(
                        "Found compressed files only. Please decompress first.",
                        count=len(compressed_files),
                        latest=max(compressed_files)
                    )
                    # For now, we'll handle compressed files too
                    import gzip
                    latest_compressed = max(compressed_files)

                    # Decompress and read
                    with gzip.open(latest_compressed, 'rt') as f:
                        # Return the path to indicate we found it
                        return latest_compressed

                self.logger.error("No enriched YFinance files found", path=self.base_path)
                return None

            # Get the most recent file based on filename timestamp
            latest_file = max(files)

            self.logger.info(
                "Found latest enriched YFinance file",
                file=latest_file,
                total_files=len(files)
            )

            return latest_file

        except Exception as e:
            self.logger.error("Failed to find enriched YFinance files", error=str(e))
            return None

    def load_enriched_data(self, file_path: Optional[str] = None) -> List[Dict[str, Any]]:
        """Load enriched data from YFinance JSON file"""
        try:
            # Use provided file or find latest
            if not file_path:
                file_path = self.get_latest_enriched_file()

            if not file_path:
                return []

            # Handle compressed files
            if file_path.endswith('.gz'):
                import gzip
                with gzip.open(file_path, 'rt') as f:
                    data = json.load(f)
            else:
                with open(file_path, 'r') as f:
                    data = json.load(f)

            self.logger.info(
                "Loaded enriched data",
                file=file_path,
                stocks_count=len(data) if isinstance(data, list) else 0
            )

            return data if isinstance(data, list) else []

        except Exception as e:
            self.logger.error("Failed to load enriched data", error=str(e), file=file_path)
            return []

    async def fetch_active_tickers(self,
                                  limit: Optional[int] = None,
                                  min_market_cap: Optional[float] = None,
                                  max_market_cap: Optional[float] = None,
                                  sectors: Optional[List[str]] = None) -> List[str]:
        """
        Fetch ticker symbols from the latest enriched YFinance JSON file.

        Note: The enriched data is already pre-filtered for stocks >$2B market cap.
        Additional filters are optional and can be applied for specific use cases.

        Args:
            limit: Maximum number of tickers to return (optional)
            min_market_cap: Minimum market cap filter (optional, data already >$2B)
            max_market_cap: Maximum market cap filter (optional)
            sectors: List of sectors to filter by (optional)

        Returns:
            List of ticker symbols
        """
        try:
            # Load the enriched data
            data = self.load_enriched_data()

            if not data:
                self.logger.warning("No data found in enriched YFinance file")
                return []

            # Extract tickers (data is already pre-filtered for >$2B market cap)
            tickers = []

            for stock in data:
                # Skip if no ticker
                ticker = stock.get('ticker')
                if not ticker:
                    continue

                # Apply additional market cap filters if specified
                if min_market_cap or max_market_cap:
                    market_cap = stock.get('market_cap', 0)

                    if min_market_cap and market_cap < min_market_cap:
                        continue

                    if max_market_cap and market_cap > max_market_cap:
                        continue

                # Apply sector filter if specified
                if sectors:
                    stock_sector = stock.get('sector', '').lower()
                    if not any(sector.lower() in stock_sector for sector in sectors):
                        continue

                tickers.append(ticker)

            # Sort by market cap (descending) to prioritize larger companies
            sorted_stocks = sorted(
                [s for s in data if s.get('ticker') in tickers],
                key=lambda x: x.get('market_cap', 0),
                reverse=True
            )

            tickers = [s.get('ticker') for s in sorted_stocks]

            # Apply limit if specified
            if limit and len(tickers) > limit:
                tickers = tickers[:limit]

            self.logger.info(
                "Fetched tickers from enriched YFinance data (pre-filtered >$2B market cap)",
                total_available=len(data),
                returned_count=len(tickers),
                limit_applied=limit,
                additional_market_cap_filter=bool(min_market_cap or max_market_cap),
                sectors_filter=sectors,
                sample_tickers=tickers[:10] if tickers else []
            )

            return tickers

        except Exception as e:
            self.logger.error("Failed to fetch tickers from YFinance data", error=str(e))
            raise

    async def get_ticker_metadata(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Get metadata for a specific ticker from the enriched data.

        Args:
            ticker: Stock symbol

        Returns:
            Dictionary with ticker metadata or None if not found
        """
        try:
            data = self.load_enriched_data()

            for stock in data:
                if stock.get('ticker') == ticker:
                    return {
                        'ticker': ticker,
                        'name': stock.get('name', ''),
                        'market_cap': stock.get('market_cap', 0),
                        'market_cap_category': stock.get('market_cap_category', ''),
                        'sector': stock.get('sector', ''),
                        'industry': stock.get('industry', ''),
                        'country': stock.get('country', 'US'),
                        'exchange': stock.get('exchange', ''),
                        'last_updated': stock.get('timestamp', '')
                    }

            return None

        except Exception as e:
            self.logger.error("Failed to get ticker metadata", ticker=ticker, error=str(e))
            return None

    async def validate_connection(self) -> bool:
        """Test if the YFinance input source is available"""
        try:
            latest_file = self.get_latest_enriched_file()
            if latest_file:
                # Try to load a small sample
                data = self.load_enriched_data(latest_file)
                return len(data) > 0
            return False

        except Exception as e:
            self.logger.error("YFinance input validation failed", error=str(e))
            return False

    async def get_summary_stats(self) -> Dict[str, Any]:
        """Get summary statistics from the enriched data"""
        try:
            data = self.load_enriched_data()

            if not data:
                return {}

            # Calculate statistics
            total_stocks = len(data)

            # Market cap distribution
            mega_cap = len([s for s in data if s.get('market_cap', 0) > 200_000_000_000])
            large_cap = len([s for s in data if 10_000_000_000 <= s.get('market_cap', 0) <= 200_000_000_000])
            mid_cap = len([s for s in data if 2_000_000_000 <= s.get('market_cap', 0) < 10_000_000_000])

            # Sector distribution
            sectors = {}
            for stock in data:
                sector = stock.get('sector', 'Unknown')
                sectors[sector] = sectors.get(sector, 0) + 1

            # Get file info
            latest_file = self.get_latest_enriched_file()
            file_timestamp = None
            if latest_file:
                # Extract timestamp from filename
                import re
                match = re.search(r'(\d{8}_\d{6})', latest_file)
                if match:
                    file_timestamp = match.group(1)

            return {
                'total_stocks': total_stocks,
                'mega_cap_count': mega_cap,
                'large_cap_count': large_cap,
                'mid_cap_count': mid_cap,
                'sectors': sectors,
                'file_path': latest_file,
                'file_timestamp': file_timestamp,
                'top_10_by_market_cap': sorted(
                    data,
                    key=lambda x: x.get('market_cap', 0),
                    reverse=True
                )[:10]
            }

        except Exception as e:
            self.logger.error("Failed to get summary stats", error=str(e))
            return {}