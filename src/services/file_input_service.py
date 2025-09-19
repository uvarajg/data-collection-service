"""
File Input Service
Reads stock tickers from local JSON file instead of Google Sheets.
This provides more control and eliminates external dependencies.
"""

import json
import structlog
from typing import List, Dict, Any, Optional
from pathlib import Path
from datetime import datetime

logger = structlog.get_logger()


class FileInputService:
    """
    Service to read stock tickers from local JSON file.
    Replaces GoogleSheetsService for more reliable data sourcing.
    """
    
    def __init__(self, input_file: str = "screened_stocks.json"):
        self.logger = logger.bind(service="file_input")
        self.input_dir = Path("/workspaces/data/input_source")
        self.input_file = self.input_dir / input_file
        
        self.logger.info(f"File Input Service initialized with {self.input_file}")
    
    async def fetch_active_tickers(self, 
                                  limit: Optional[int] = None,
                                  min_market_cap: Optional[float] = None,
                                  min_volume: Optional[float] = None) -> List[str]:
        """
        Fetch active tickers from the JSON file.
        
        Args:
            limit: Maximum number of tickers to return
            min_market_cap: Additional market cap filter
            min_volume: Additional volume filter
            
        Returns:
            List of ticker symbols
        """
        try:
            if not self.input_file.exists():
                self.logger.error(f"Input file not found: {self.input_file}")
                self.logger.info("Run 'python update_stock_universe.py' to generate the file")
                return []
            
            with open(self.input_file, 'r') as f:
                data = json.load(f)
            
            stocks = data.get('stocks', [])
            
            # Apply additional filters if specified
            if min_market_cap:
                stocks = [s for s in stocks if s.get('market_cap', 0) >= min_market_cap]
            
            if min_volume:
                stocks = [s for s in stocks if s.get('avg_volume', 0) >= min_volume]
            
            # Extract tickers
            tickers = [s['ticker'] for s in stocks]
            
            # Apply limit if specified
            if limit:
                tickers = tickers[:limit]
            
            self.logger.info(f"Fetched {len(tickers)} active tickers from {self.input_file.name}")
            
            # Log some statistics
            if stocks:
                total_market_cap = sum(s.get('market_cap', 0) for s in stocks[:len(tickers)])
                avg_market_cap = total_market_cap / len(tickers) if tickers else 0
                self.logger.info(f"Average market cap: ${avg_market_cap/1_000_000_000:.1f}B")
            
            return tickers
            
        except Exception as e:
            self.logger.error(f"Error reading input file: {e}")
            return []
    
    async def get_stock_details(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information about a specific stock.
        
        Args:
            ticker: Stock symbol
            
        Returns:
            Stock details dictionary or None
        """
        try:
            if not self.input_file.exists():
                return None
            
            with open(self.input_file, 'r') as f:
                data = json.load(f)
            
            stocks = data.get('stocks', [])
            
            for stock in stocks:
                if stock['ticker'] == ticker:
                    return stock
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting stock details: {e}")
            return None
    
    async def get_all_stocks(self) -> List[Dict[str, Any]]:
        """
        Get all stocks with their details.
        
        Returns:
            List of stock dictionaries
        """
        try:
            if not self.input_file.exists():
                self.logger.error(f"Input file not found: {self.input_file}")
                return []
            
            with open(self.input_file, 'r') as f:
                data = json.load(f)
            
            return data.get('stocks', [])
            
        except Exception as e:
            self.logger.error(f"Error reading all stocks: {e}")
            return []
    
    async def get_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the stock universe.
        
        Returns:
            Metadata dictionary
        """
        try:
            if not self.input_file.exists():
                return {
                    'error': 'Input file not found',
                    'file': str(self.input_file)
                }
            
            with open(self.input_file, 'r') as f:
                data = json.load(f)
            
            # Calculate additional statistics
            stocks = data.get('stocks', [])
            
            metadata = {
                'generated_at': data.get('generated_at'),
                'total_stocks': data.get('total_stocks', len(stocks)),
                'criteria': data.get('criteria', {}),
                'file': str(self.input_file),
                'file_modified': datetime.fromtimestamp(
                    self.input_file.stat().st_mtime
                ).isoformat() if self.input_file.exists() else None,
                'statistics': {
                    'total_market_cap': sum(s.get('market_cap', 0) for s in stocks),
                    'avg_market_cap': sum(s.get('market_cap', 0) for s in stocks) / len(stocks) if stocks else 0,
                    'total_volume': sum(s.get('avg_volume', 0) for s in stocks),
                    'sectors': len(set(s.get('sector', 'Unknown') for s in stocks))
                }
            }
            
            return metadata
            
        except Exception as e:
            self.logger.error(f"Error getting metadata: {e}")
            return {'error': str(e)}
    
    async def validate_input_file(self) -> bool:
        """
        Validate that the input file exists and has valid data.
        
        Returns:
            True if valid, False otherwise
        """
        try:
            if not self.input_file.exists():
                self.logger.error(f"Input file does not exist: {self.input_file}")
                return False
            
            with open(self.input_file, 'r') as f:
                data = json.load(f)
            
            if 'stocks' not in data:
                self.logger.error("Input file missing 'stocks' field")
                return False
            
            stocks = data['stocks']
            if not stocks:
                self.logger.error("Input file has no stocks")
                return False
            
            # Check that stocks have required fields
            required_fields = ['ticker']
            for stock in stocks[:5]:  # Check first 5
                for field in required_fields:
                    if field not in stock:
                        self.logger.error(f"Stock missing required field: {field}")
                        return False
            
            self.logger.info(f"Input file validated: {len(stocks)} stocks")
            return True
            
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in input file: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Error validating input file: {e}")
            return False