import structlog
import json
import os
import gzip
import aiofiles
import asyncio
import pandas as pd
from pathlib import Path
from typing import List, Optional, Dict, Any
from datetime import datetime, date
from ..models.data_models import StockDataRecord, CollectionJob
from .robust_file_writer import RobustFileWriter

logger = structlog.get_logger()


class StorageService:
    """
    File storage service that replicates AlgoAlchemist's directory structure.
    Manages the /workspaces/data/ directory with UUID tracking and atomic writes.
    """
    
    def __init__(self, base_path: str = "/workspaces/data"):
        self.base_path = Path(base_path)
        self.logger = logger.bind(service="storage")
        
        # Initialize robust file writer for corruption-free writes
        self.file_writer = RobustFileWriter()
        
        self._ensure_directory_structure()
    
    def _ensure_directory_structure(self):
        """Create the base directory structure if it doesn't exist"""
        directories = [
            self.base_path / "historical" / "daily",
            self.base_path / "compressed",
            self.base_path / "cache" / "alpaca",
            self.base_path / "cache" / "yfinance", 
            self.base_path / "cache" / "technical_indicators",
            self.base_path / "jobs"
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
        
        self.logger.info("Storage directory structure ensured", base_path=str(self.base_path))
    
    async def save_daily_record(self, record: StockDataRecord) -> bool:
        """
        Save a single daily record to the file system.
        Path: /workspaces/data/historical/daily/{TICKER}/{YYYY}/{MM}/{YYYY-MM-DD}.json
        
        Args:
            record: StockDataRecord to save
            
        Returns:
            True if saved successfully, False otherwise
        """
        try:
            # Parse date components
            date_obj = datetime.strptime(record.date, "%Y-%m-%d").date()
            year = date_obj.strftime("%Y")
            month = date_obj.strftime("%m")
            
            # Create file path
            file_path = (self.base_path / "historical" / "daily" / 
                        record.ticker.upper() / year / month / f"{record.date}.json")
            
            # Ensure directory exists
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Use robust file writer with validation and verification
            success = await self.file_writer.write_json_file(
                file_path=file_path,
                data=record,
                verify_after_write=True
            )
            
            if success:
                self.logger.info("Daily record saved with validation", 
                               ticker=record.ticker, 
                               date=record.date,
                               file_path=str(file_path),
                               record_id=record.record_id)
            else:
                self.logger.error("Failed to save daily record with validation", 
                                ticker=record.ticker,
                                date=record.date)
            
            return success
            
        except Exception as e:
            self.logger.error("Failed to save daily record", 
                            ticker=record.ticker,
                            date=record.date,
                            error=str(e))
            return False
    
    async def save_daily_records_batch(self, records: List[StockDataRecord]) -> Dict[str, int]:
        """
        Save a batch of daily records using robust file writer.
        
        Args:
            records: List of StockDataRecord objects to save
            
        Returns:
            Dictionary with success/failure counts
        """
        if not records:
            return {"successful": 0, "failed": 0}
        
        # Prepare file path and data pairs for batch write
        file_data_pairs = []
        for record in records:
            try:
                date_obj = datetime.strptime(record.date, "%Y-%m-%d").date()
                year = date_obj.strftime("%Y")
                month = date_obj.strftime("%m")
                
                file_path = (self.base_path / "historical" / "daily" / 
                           record.ticker.upper() / year / month / f"{record.date}.json")
                
                file_data_pairs.append((file_path, record))
            except Exception as e:
                self.logger.error("Failed to prepare record for batch save",
                                ticker=record.ticker,
                                date=record.date,
                                error=str(e))
        
        # Use robust file writer's batch method
        result = await self.file_writer.write_batch(file_data_pairs, max_concurrent=10)
        
        self.logger.info("Batch save completed with validation", 
                        total_records=len(records),
                        successful=result["successful"],
                        failed=result["failed"],
                        writer_stats=self.file_writer.get_stats())
        
        return result
    
    async def load_daily_record(self, ticker: str, date_str: str) -> Optional[StockDataRecord]:
        """
        Load a daily record from storage.
        
        Args:
            ticker: Stock symbol
            date_str: Date in YYYY-MM-DD format
            
        Returns:
            StockDataRecord if found, None otherwise
        """
        try:
            # Parse date components
            date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
            year = date_obj.strftime("%Y")
            month = date_obj.strftime("%m")
            
            # Create file path
            file_path = (self.base_path / "historical" / "daily" / 
                        ticker.upper() / year / month / f"{date_str}.json")
            
            if not file_path.exists():
                return None
            
            async with aiofiles.open(file_path, 'r') as f:
                data = json.loads(await f.read())
            
            # Convert back to StockDataRecord (simplified - would need full deserialization)
            self.logger.info("Daily record loaded", ticker=ticker, date=date_str)
            return data  # Return dict for now, could implement full object reconstruction
            
        except Exception as e:
            self.logger.error("Failed to load daily record", 
                            ticker=ticker, date=date_str, error=str(e))
            return None
    
    async def load_ticker_date_range(
        self, 
        ticker: str, 
        start_date: str, 
        end_date: str
    ) -> List[Dict[str, Any]]:
        """
        Load all records for a ticker within a date range.
        
        Args:
            ticker: Stock symbol
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            List of record dictionaries
        """
        records = []
        
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
            
            current_date = start_dt
            while current_date <= end_dt:
                date_str = current_date.strftime("%Y-%m-%d")
                record = await self.load_daily_record(ticker, date_str)
                if record:
                    records.append(record)
                
                # Move to next day
                current_date = datetime.combine(current_date, datetime.min.time()).date()
                current_date = (datetime.combine(current_date, datetime.min.time()) + 
                               pd.Timedelta(days=1)).date()
        
        except Exception as e:
            self.logger.error("Failed to load ticker date range", 
                            ticker=ticker, start_date=start_date, 
                            end_date=end_date, error=str(e))
        
        return records
    
    def get_chronological_json_files(self, directory_path: Path) -> List[Path]:
        """
        Get JSON files from a directory in chronological order.
        
        This is a critical function for ensuring data consistency. Files are sorted
        by their filename (YYYY-MM-DD format) to guarantee chronological processing.
        
        Args:
            directory_path: Path to directory containing JSON files
            
        Returns:
            List of Path objects sorted chronologically by date
        """
        if not directory_path.exists() or not directory_path.is_dir():
            return []
            
        # Get all JSON files and sort by filename (YYYY-MM-DD format)
        # This ensures chronological order which is critical for validation
        json_files = sorted(directory_path.glob("*.json"), 
                          key=lambda f: f.stem)  # YYYY-MM-DD sorts naturally
        
        self.logger.debug("Retrieved chronological JSON files", 
                         directory=str(directory_path),
                         file_count=len(json_files),
                         first_file=json_files[0].stem if json_files else None,
                         last_file=json_files[-1].stem if json_files else None)
        
        return json_files
    
    async def load_ticker_chronological_data(
        self,
        ticker: str,
        start_date: str,
        end_date: str
    ) -> List[Dict[str, Any]]:
        """
        Load ticker data in strict chronological order.
        
        This function ensures data consistency by loading records in proper
        chronological sequence, which is critical for validation and analysis.
        
        Args:
            ticker: Stock symbol
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            List of record dictionaries in chronological order
        """
        records = []
        
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            
            # Generate all year/month combinations in the date range
            year_months = set()
            current = start_dt
            while current <= end_dt:
                year_months.add((current.year, current.month))
                if current.month == 12:
                    current = current.replace(year=current.year + 1, month=1)
                else:
                    current = current.replace(month=current.month + 1)
            
            # Process each year/month in chronological order
            for year, month in sorted(year_months):
                month_dir = self.base_path / "historical" / "daily" / ticker / str(year) / f"{month:02d}"
                
                if not month_dir.exists():
                    continue
                
                # Get files in chronological order
                json_files = self.get_chronological_json_files(month_dir)
                
                for json_file in json_files:
                    try:
                        file_date_str = json_file.stem
                        file_date = datetime.strptime(file_date_str, '%Y-%m-%d')
                        
                        # Only include files in the requested date range
                        if start_dt <= file_date <= end_dt:
                            async with aiofiles.open(json_file, 'r') as f:
                                data = json.loads(await f.read())
                                records.append(data)
                                
                    except (ValueError, json.JSONDecodeError) as e:
                        self.logger.warning("Skipping invalid data file",
                                          file=str(json_file), error=str(e))
                        continue
            
            self.logger.info("Loaded chronological ticker data",
                           ticker=ticker,
                           date_range=f"{start_date} to {end_date}",
                           record_count=len(records))
            
            return records
            
        except Exception as e:
            self.logger.error("Failed to load chronological ticker data",
                            ticker=ticker,
                            start_date=start_date,
                            end_date=end_date,
                            error=str(e))
            return []
    
    async def save_collection_job(self, job: CollectionJob) -> bool:
        """
        Save collection job metadata.
        Path: /workspaces/data/jobs/{job_id}/metadata.json
        
        Args:
            job: CollectionJob object
            
        Returns:
            True if saved successfully, False otherwise
        """
        try:
            job_dir = self.base_path / "jobs" / job.job_id
            job_dir.mkdir(parents=True, exist_ok=True)
            
            metadata_path = job_dir / "metadata.json"
            temp_path = metadata_path.with_suffix('.json.tmp')
            
            job_data = job.to_dict()
            
            async with aiofiles.open(temp_path, 'w') as f:
                await f.write(json.dumps(job_data, indent=2, default=str))
            
            temp_path.rename(metadata_path)
            
            self.logger.info("Collection job saved", job_id=job.job_id)
            return True
            
        except Exception as e:
            self.logger.error("Failed to save collection job", 
                            job_id=job.job_id, error=str(e))
            return False
    
    async def update_collection_job(self, job: CollectionJob) -> bool:
        """Update existing collection job metadata"""
        return await self.save_collection_job(job)
    
    async def load_collection_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Load collection job metadata.
        
        Args:
            job_id: Job identifier
            
        Returns:
            Job metadata dictionary if found, None otherwise
        """
        try:
            metadata_path = self.base_path / "jobs" / job_id / "metadata.json"
            
            if not metadata_path.exists():
                return None
            
            async with aiofiles.open(metadata_path, 'r') as f:
                job_data = json.loads(await f.read())
            
            self.logger.info("Collection job loaded", job_id=job_id)
            return job_data
            
        except Exception as e:
            self.logger.error("Failed to load collection job", 
                            job_id=job_id, error=str(e))
            return None
    
    def get_storage_stats(self) -> Dict[str, Any]:
        """
        Get storage statistics and disk usage.
        
        Returns:
            Dictionary with storage statistics
        """
        try:
            stats = {
                "base_path": str(self.base_path),
                "directories": {},
                "total_files": 0,
                "total_size_bytes": 0
            }
            
            # Count files and sizes in each directory
            for subdir in ["historical", "compressed", "cache", "jobs"]:
                dir_path = self.base_path / subdir
                if dir_path.exists():
                    file_count = sum(1 for _ in dir_path.rglob("*.json"))
                    size_bytes = sum(f.stat().st_size for f in dir_path.rglob("*.json") if f.is_file())
                    
                    stats["directories"][subdir] = {
                        "file_count": file_count,
                        "size_bytes": size_bytes,
                        "size_mb": round(size_bytes / (1024 * 1024), 2)
                    }
                    
                    stats["total_files"] += file_count
                    stats["total_size_bytes"] += size_bytes
            
            stats["total_size_mb"] = round(stats["total_size_bytes"] / (1024 * 1024), 2)
            
            return stats
            
        except Exception as e:
            self.logger.error("Failed to get storage stats", error=str(e))
            return {"error": str(e)}
    
    async def get_all_tickers_chronological_data(
        self,
        start_date: str,
        end_date: str,
        tickers: Optional[List[str]] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get chronological data for all or specified tickers.
        
        This method is designed for external services (like validation) that need
        data in strict chronological order to avoid consistency issues.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format  
            tickers: Optional list of specific tickers (if None, gets all)
            
        Returns:
            Dictionary mapping ticker -> list of records in chronological order
        """
        result = {}
        
        try:
            # Get list of tickers if not provided
            if tickers is None:
                historical_dir = self.base_path / "historical" / "daily"
                if historical_dir.exists():
                    tickers = [d.name for d in historical_dir.iterdir() 
                             if d.is_dir() and d.name.isalpha()]
                else:
                    self.logger.warning("Historical data directory not found")
                    return result
            
            # Load data for each ticker in chronological order
            for ticker in tickers:
                ticker_data = await self.load_ticker_chronological_data(
                    ticker, start_date, end_date
                )
                if ticker_data:
                    result[ticker] = ticker_data
            
            self.logger.info("Loaded chronological data for all tickers",
                           ticker_count=len(result),
                           date_range=f"{start_date} to {end_date}",
                           total_records=sum(len(data) for data in result.values()))
            
            return result
            
        except Exception as e:
            self.logger.error("Failed to load all tickers chronological data",
                            start_date=start_date,
                            end_date=end_date,
                            error=str(e))
            return result
    
    async def compress_monthly_data(self, ticker: str, year: str, month: str) -> bool:
        """
        Compress monthly data into .gz files for space efficiency.
        Source: /workspaces/data/historical/daily/{TICKER}/{YYYY}/{MM}/
        Target: /workspaces/data/compressed/{TICKER}/{YYYY}/{MM}.gz
        
        Args:
            ticker: Stock symbol
            year: Year (YYYY)
            month: Month (MM)
            
        Returns:
            True if compressed successfully, False otherwise
        """
        try:
            source_dir = self.base_path / "historical" / "daily" / ticker.upper() / year / month
            target_dir = self.base_path / "compressed" / ticker.upper() / year
            target_dir.mkdir(parents=True, exist_ok=True)
            
            target_file = target_dir / f"{month}.gz"
            
            if not source_dir.exists():
                self.logger.warning("Source directory not found", source_dir=str(source_dir))
                return False
            
            # Collect all JSON files for the month in chronological order
            monthly_data = []
            
            # Sort files by date to ensure chronological order
            json_files = sorted(source_dir.glob("*.json"), 
                              key=lambda f: f.stem)  # YYYY-MM-DD format naturally sorts chronologically
            
            for json_file in json_files:
                async with aiofiles.open(json_file, 'r') as f:
                    data = json.loads(await f.read())
                    monthly_data.append(data)
            
            if not monthly_data:
                self.logger.info("No data to compress", ticker=ticker, year=year, month=month)
                return False
            
            # Compress and save
            compressed_data = json.dumps(monthly_data, separators=(',', ':')).encode('utf-8')
            
            with gzip.open(target_file, 'wb') as f:
                f.write(compressed_data)
            
            self.logger.info("Monthly data compressed", 
                           ticker=ticker, year=year, month=month,
                           record_count=len(monthly_data),
                           compressed_file=str(target_file))
            
            return True
            
        except Exception as e:
            self.logger.error("Failed to compress monthly data", 
                            ticker=ticker, year=year, month=month, error=str(e))
            return False