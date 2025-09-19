import structlog
import asyncio
import json
import os
from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Any, Tuple
import yfinance as yf
import pandas as pd
from dataclasses import asdict
from uuid import uuid4

from ..config.settings import get_settings
from ..models.data_models import StockDataRecord, RecordMetadata
from .alpaca_service import AlpacaService
from .technical_indicator_validator import TechnicalIndicatorValidator

logger = structlog.get_logger()


class EnhancedDataService:
    """
    Enhanced data collection service with multiple data sources and fallback mechanisms.
    
    Provides high reliability through:
    1. Primary source: Alpaca API 
    2. Fallback source: Yahoo Finance
    3. Data quality validation
    4. Automatic retry logic
    5. Error handling and recovery
    """
    
    def __init__(self):
        self.settings = get_settings()
        self.alpaca_service = AlpacaService()
        self.retry_attempts = 3
        self.retry_delay = 1.0  # seconds
        self.logger = logger.bind(service="enhanced_data")
        self.indicator_validator = TechnicalIndicatorValidator()
        
    async def get_reliable_daily_bars(
        self,
        ticker: str,
        start_date: str,
        end_date: str,
        job_id: Optional[str] = None,
        prefer_source: str = "alpaca"
    ) -> List[StockDataRecord]:
        """
        Get daily bars with automatic fallback between data sources.
        
        Args:
            ticker: Stock symbol
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)  
            job_id: Optional job ID for tracking
            prefer_source: Preferred data source ('alpaca' or 'yfinance')
            
        Returns:
            List of StockDataRecord objects
        """
        self.logger.info("Starting reliable data collection", 
                        ticker=ticker, 
                        start_date=start_date, 
                        end_date=end_date,
                        prefer_source=prefer_source)
        
        # Define source priority
        sources = ["alpaca", "yfinance"] if prefer_source == "alpaca" else ["yfinance", "alpaca"]
        
        records = []
        last_error = None
        
        for source_name in sources:
            try:
                self.logger.info(f"Attempting data collection from {source_name}", ticker=ticker)
                
                if source_name == "alpaca":
                    records = await self._get_from_alpaca_with_retry(ticker, start_date, end_date, job_id)
                elif source_name == "yfinance":
                    records = await self._get_from_yfinance_with_retry(ticker, start_date, end_date, job_id)
                
                if records and self._validate_data_quality(records):
                    self.logger.info(f"Successfully collected data from {source_name}", 
                                   ticker=ticker, record_count=len(records))
                    return records
                else:
                    self.logger.warning(f"Data quality issues with {source_name}", ticker=ticker)
                    
            except Exception as e:
                last_error = e
                self.logger.warning(f"Failed to collect from {source_name}", 
                                  ticker=ticker, error=str(e))
                continue
        
        # If all sources failed, return empty list and record error for monitoring
        if last_error:
            self.logger.error("All data sources failed", ticker=ticker, last_error=str(last_error))
            
            # Record the error for tracking but don't create fake data records
            await self._record_collection_error(ticker, start_date, end_date, str(last_error), job_id)
            
            # Return empty list - let downstream services handle appropriately
            return []
        
        return records
    
    async def _get_from_alpaca_with_retry(
        self,
        ticker: str,
        start_date: str, 
        end_date: str,
        job_id: Optional[str] = None
    ) -> List[StockDataRecord]:
        """Get data from Alpaca with retry logic."""
        
        for attempt in range(self.retry_attempts):
            try:
                records = await self.alpaca_service.get_daily_bars(ticker, start_date, end_date, job_id)
                
                # Check if we got valid data (not just error records)
                valid_records = [r for r in records if r.open > 0 or r.high > 0 or r.low > 0 or r.close > 0]
                if valid_records:
                    return records
                else:
                    self.logger.warning("Alpaca returned no valid price data", 
                                      ticker=ticker, attempt=attempt + 1)
                    
            except Exception as e:
                self.logger.warning("Alpaca attempt failed", 
                                  ticker=ticker, attempt=attempt + 1, error=str(e))
                
                if attempt < self.retry_attempts - 1:
                    await asyncio.sleep(self.retry_delay * (2 ** attempt))  # Exponential backoff
        
        raise Exception(f"Alpaca failed after {self.retry_attempts} attempts")
    
    async def _get_from_yfinance_with_retry(
        self,
        ticker: str,
        start_date: str,
        end_date: str, 
        job_id: Optional[str] = None
    ) -> List[StockDataRecord]:
        """Get data from Yahoo Finance with retry logic."""
        
        for attempt in range(self.retry_attempts):
            try:
                # Convert dates to datetime objects
                start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
                end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
                
                # Add one day to end_date for yfinance (it's exclusive)
                end_dt_exclusive = end_dt + timedelta(days=1)
                
                self.logger.debug("Fetching from Yahoo Finance", 
                                ticker=ticker, 
                                start=start_date,
                                end=str(end_dt_exclusive),
                                attempt=attempt + 1)
                
                # Fetch data from Yahoo Finance
                stock = yf.Ticker(ticker)
                hist = stock.history(start=start_dt, end=end_dt_exclusive, auto_adjust=False, prepost=False)
                
                if hist.empty:
                    self.logger.warning("Yahoo Finance returned no data", 
                                      ticker=ticker, attempt=attempt + 1)
                    if attempt < self.retry_attempts - 1:
                        await asyncio.sleep(self.retry_delay)
                        continue
                    else:
                        raise Exception("No data available from Yahoo Finance")
                
                # Convert to StockDataRecord format
                records = []
                for index, row in hist.iterrows():
                    trade_date = index.strftime('%Y-%m-%d')
                    
                    metadata = RecordMetadata(
                        collection_timestamp=datetime.utcnow(),
                        data_source="yfinance", 
                        collection_job_id=job_id,
                        processing_status="collected"
                    )
                    
                    record = StockDataRecord(
                        ticker=ticker.upper(),
                        date=trade_date,
                        open=float(row['Open']) if pd.notna(row['Open']) else 0.0,
                        high=float(row['High']) if pd.notna(row['High']) else 0.0,
                        low=float(row['Low']) if pd.notna(row['Low']) else 0.0,
                        close=float(row['Close']) if pd.notna(row['Close']) else 0.0,
                        volume=int(row['Volume']) if pd.notna(row['Volume']) and row['Volume'] >= 0 else 0,
                        metadata=metadata
                    )
                    
                    records.append(record)
                
                # Sort records by date to ensure chronological order for technical indicators
                records.sort(key=lambda r: r.date)
                
                self.logger.info("Successfully collected from Yahoo Finance", 
                               ticker=ticker, record_count=len(records))
                return records
                
            except Exception as e:
                self.logger.warning("Yahoo Finance attempt failed", 
                                  ticker=ticker, attempt=attempt + 1, error=str(e))
                
                if attempt < self.retry_attempts - 1:
                    await asyncio.sleep(self.retry_delay * (2 ** attempt))
        
        raise Exception(f"Yahoo Finance failed after {self.retry_attempts} attempts")
    
    def _validate_data_quality(self, records: List[StockDataRecord]) -> bool:
        """
        Validate basic data quality requirements.
        
        Args:
            records: List of data records to validate
            
        Returns:
            True if data meets minimum quality standards
        """
        if not records:
            return False
        
        # Check for valid price data
        valid_records = 0
        total_records = len(records)
        
        for record in records:
            # Must have at least one valid price
            if record.open > 0 or record.high > 0 or record.low > 0 or record.close > 0:
                # Basic OHLC validation
                if record.high >= max(record.open, record.close) and record.low <= min(record.open, record.close):
                    # Volume should not be negative
                    if record.volume >= 0:
                        valid_records += 1
        
        # Require at least 70% of records to be valid
        quality_ratio = valid_records / total_records
        min_quality_threshold = 0.7
        
        self.logger.debug("Data quality assessment", 
                         valid_records=valid_records,
                         total_records=total_records,
                         quality_ratio=quality_ratio,
                         threshold=min_quality_threshold)
        
        return quality_ratio >= min_quality_threshold
    
    async def get_data_source_health(self) -> Dict[str, Any]:
        """
        Check the health and availability of all data sources.
        
        Returns:
            Dictionary with health status of each source
        """
        health_status = {
            "alpaca": {"status": "unknown", "response_time": None, "error": None},
            "yfinance": {"status": "unknown", "response_time": None, "error": None}
        }
        
        # Test Alpaca
        try:
            start_time = datetime.utcnow()
            test_result = await self.alpaca_service.validate_connection()
            response_time = (datetime.utcnow() - start_time).total_seconds()
            
            health_status["alpaca"] = {
                "status": "healthy" if test_result else "unhealthy",
                "response_time": response_time,
                "error": None
            }
        except Exception as e:
            health_status["alpaca"] = {
                "status": "error",
                "response_time": None,
                "error": str(e)
            }
        
        # Test Yahoo Finance
        try:
            start_time = datetime.utcnow()
            # Simple test - try to fetch AAPL data for yesterday
            yesterday = (datetime.now().date() - timedelta(days=1)).strftime('%Y-%m-%d')
            test_records = await self._get_from_yfinance_with_retry("AAPL", yesterday, yesterday)
            response_time = (datetime.utcnow() - start_time).total_seconds()
            
            health_status["yfinance"] = {
                "status": "healthy" if test_records else "unhealthy",
                "response_time": response_time,
                "error": None
            }
        except Exception as e:
            health_status["yfinance"] = {
                "status": "error",
                "response_time": None,
                "error": str(e)
            }
        
        return health_status

    async def _record_collection_error(
        self, 
        ticker: str, 
        start_date: str, 
        end_date: str, 
        error_message: str, 
        job_id: Optional[str] = None
    ) -> None:
        """
        Record collection errors for monitoring and alerting.
        
        Creates error records in /workspaces/data/error_records/ structure
        matching the pattern of successful data collection.
        """
        try:
            error_record = {
                "error_id": str(uuid4()),
                "ticker": ticker.upper(),
                "requested_start_date": start_date,
                "requested_end_date": end_date,
                "error_timestamp": datetime.utcnow().isoformat(),
                "error_message": error_message,
                "collection_job_id": job_id,
                "failed_sources": ["alpaca", "yfinance"],  # Both sources failed
                "error_type": "all_sources_failed",
                "service": "enhanced_data_service"
            }
            
            # Create error record file path following same pattern as data files
            # Pattern: /workspaces/data/error_records/daily/{TICKER}/{YEAR}/{MM}/{YYYY-MM-DD}.json
            current_date = datetime.now()
            error_dir = f"/workspaces/data/error_records/daily/{ticker.upper()}/{current_date.year}/{current_date.month:02d}"
            
            # Ensure directory exists
            os.makedirs(error_dir, exist_ok=True)
            
            # File name uses current date (when error occurred)
            error_file = f"{error_dir}/{current_date.strftime('%Y-%m-%d')}.json"
            
            # If error file already exists today, append to it (multiple errors for same ticker)
            if os.path.exists(error_file):
                with open(error_file, 'r') as f:
                    existing_errors = json.load(f)
                if isinstance(existing_errors, list):
                    existing_errors.append(error_record)
                else:
                    existing_errors = [existing_errors, error_record]
            else:
                existing_errors = [error_record]
                
            # Write error record
            with open(error_file, 'w') as f:
                json.dump(existing_errors, f, indent=2)
            
            self.logger.info("Collection error recorded", 
                           ticker=ticker,
                           error_file=error_file,
                           error_id=error_record["error_id"])
                           
        except Exception as e:
            self.logger.error("Failed to record collection error", 
                            ticker=ticker, 
                            error=str(e))