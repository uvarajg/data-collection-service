import structlog
import asyncio
from datetime import datetime, date
from typing import List, Optional, Dict
import alpaca_trade_api as tradeapi
from alpaca_trade_api.common import URL
from alpaca_trade_api.rest import TimeFrame
import pandas as pd

from ..config.settings import get_settings
from ..models.data_models import StockDataRecord, RecordMetadata
from ..utils.retry_decorator import alpaca_retry, ALPACA_RATE_LIMITER

logger = structlog.get_logger()


class AlpacaService:
    """
    Alpaca API service for collecting OHLCV data.
    Replicates the functionality from AlgoAlchemist's alpaca-service.ts
    """
    
    def __init__(self):
        self.settings = get_settings()
        self._api_client = None
        self._initialize_client()
    
    def _initialize_client(self):
        """Initialize Alpaca API client with credentials from settings"""
        try:
            # Use settings from environment variables
            base_url = URL(self.settings.apca_api_base_url)
            
            self._api_client = tradeapi.REST(
                key_id=self.settings.apca_api_key_id,
                secret_key=self.settings.apca_api_secret_key,
                base_url=base_url,
                api_version='v2'
            )
            
            logger.info("Alpaca API client initialized", base_url=str(base_url))
            
        except Exception as e:
            logger.error("Failed to initialize Alpaca API client", error=str(e))
            raise
    
    @alpaca_retry(max_attempts=3)
    async def get_daily_bars(
        self, 
        ticker: str, 
        start_date: str, 
        end_date: str,
        job_id: Optional[str] = None
    ) -> List[StockDataRecord]:
        """
        Get daily OHLCV bars for a ticker from Alpaca API.
        
        Args:
            ticker: Stock symbol (e.g., 'AAPL')
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            job_id: Optional job ID for tracking
            
        Returns:
            List of StockDataRecord objects
        """
        logger.info("Fetching daily bars from Alpaca", 
                   ticker=ticker, start_date=start_date, end_date=end_date)
        
        records = []
        
        try:
            # Apply centralized rate limiting
            await ALPACA_RATE_LIMITER.wait_if_needed()
            
            # Get bars from Alpaca API using getBarsV2 equivalent
            bars_response = self._api_client.get_bars(
                symbol=ticker,
                timeframe=TimeFrame.Day,
                start=start_date,
                end=end_date,
                adjustment='raw',  # No split/dividend adjustments
                limit=10000  # Maximum allowed by Alpaca
            )
            
            # Convert to pandas DataFrame for easier processing
            df = bars_response.df
            
            if df.empty:
                logger.warning("No data returned from Alpaca", ticker=ticker)
                return records
            
            # Process each bar into StockDataRecord
            for index, row in df.iterrows():
                # Convert timestamp to date string
                trade_date = index.strftime('%Y-%m-%d') if hasattr(index, 'strftime') else str(index.date())
                
                # Create metadata
                metadata = RecordMetadata(
                    collection_timestamp=datetime.utcnow(),
                    data_source="alpaca",
                    collection_job_id=job_id,
                    processing_status="collected"
                )
                
                # Create stock data record
                record = StockDataRecord(
                    ticker=ticker.upper(),
                    date=trade_date,
                    open=float(row['open']),
                    high=float(row['high']),
                    low=float(row['low']),
                    close=float(row['close']),
                    volume=int(row['volume']),
                    metadata=metadata
                )
                
                records.append(record)
            
            # Sort records by date to ensure chronological order for technical indicators
            records.sort(key=lambda r: r.date)
            
            logger.info("Successfully collected daily bars", 
                       ticker=ticker, record_count=len(records))
            
        except Exception as e:
            logger.error("Failed to fetch daily bars from Alpaca", 
                        ticker=ticker, error=str(e))
            
            # Create error record to maintain data integrity
            error_metadata = RecordMetadata(
                collection_timestamp=datetime.utcnow(),
                data_source="alpaca",
                collection_job_id=job_id,
                error_message=str(e),
                processing_status="error"
            )
            
            error_record = StockDataRecord(
                ticker=ticker.upper(),
                date=start_date,
                metadata=error_metadata
            )
            
            records.append(error_record)
        
        return records
    
    @alpaca_retry(max_attempts=3)
    async def get_latest_bar(self, ticker: str, job_id: Optional[str] = None) -> Optional[StockDataRecord]:
        """
        Get the latest daily bar for a ticker.
        
        Args:
            ticker: Stock symbol
            job_id: Optional job ID for tracking
            
        Returns:
            Latest StockDataRecord or None if not found
        """
        logger.info("Fetching latest bar from Alpaca", ticker=ticker)
        
        try:
            # Add rate limiting delay
            await asyncio.sleep(0.1)
            
            # Get latest bar
            latest_bars = self._api_client.get_bars(
                symbol=ticker,
                timeframe=TimeFrame.Day,
                limit=1,
                adjustment='raw'
            )
            
            df = latest_bars.df
            
            if df.empty:
                logger.warning("No latest data available", ticker=ticker)
                return None
            
            # Get the latest record
            latest_row = df.iloc[-1]
            latest_date = df.index[-1].strftime('%Y-%m-%d')
            
            metadata = RecordMetadata(
                collection_timestamp=datetime.utcnow(),
                data_source="alpaca",
                collection_job_id=job_id,
                processing_status="collected"
            )
            
            record = StockDataRecord(
                ticker=ticker.upper(),
                date=latest_date,
                open=float(latest_row['open']),
                high=float(latest_row['high']),
                low=float(latest_row['low']),
                close=float(latest_row['close']),
                volume=int(latest_row['volume']),
                metadata=metadata
            )
            
            logger.info("Successfully collected latest bar", ticker=ticker, date=latest_date)
            return record
            
        except Exception as e:
            logger.error("Failed to fetch latest bar from Alpaca", ticker=ticker, error=str(e))
            return None
    
    def get_account_info(self) -> Dict:
        """Get Alpaca account information for verification"""
        try:
            account = self._api_client.get_account()
            return {
                "account_id": account.id,
                "status": account.status,
                "trading_blocked": account.trading_blocked,
                "pattern_day_trader": account.pattern_day_trader,
                "buying_power": str(account.buying_power),
                "cash": str(account.cash)
            }
        except Exception as e:
            logger.error("Failed to get account info", error=str(e))
            raise
    
    async def validate_connection(self) -> bool:
        """Validate Alpaca API connection"""
        try:
            account_info = self.get_account_info()
            logger.info("Alpaca connection validated", account_status=account_info.get("status"))
            return True
        except Exception as e:
            logger.error("Alpaca connection validation failed", error=str(e))
            return False