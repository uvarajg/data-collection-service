"""
SMA Error Handler Service

Handles records with missing SMA_200 values by moving them to error_records
after all fallback attempts have been exhausted.
"""

import structlog
import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import asdict

from ..models.data_models import StockDataRecord

logger = structlog.get_logger()


class SMAErrorHandler:
    """
    Handles records with missing SMA_200 and other critical moving averages.
    Segregates incomplete records to maintain dataset quality.
    """
    
    def __init__(self):
        self.logger = logger.bind(service="sma_error_handler")
        self.error_base_path = Path("/workspaces/data/error_records/missing_indicators")
        self.stats = {
            'total_moved': 0,
            'by_ticker': {}
        }
    
    def move_to_error_records_missing_sma(
        self, 
        record: StockDataRecord,
        reason: str = "sma_200_unavailable",
        attempts: Dict[str, str] = None
    ):
        """
        Move records with missing critical indicators to error_records.
        
        Args:
            record: The stock data record with missing SMA_200
            reason: Reason for moving to error_records
            attempts: Dictionary of attempted sources and their results
        """
        # Create directory structure
        ticker_path = self.error_base_path / record.ticker
        ticker_path.mkdir(parents=True, exist_ok=True)
        
        error_file = ticker_path / f"{record.date}.json"
        
        # Prepare error record with detailed information
        error_data = {
            'original_record': asdict(record),
            'error_type': 'missing_sma_200',
            'reason': reason,
            'attempts': attempts or {
                'historical_calculation': 'failed_insufficient_data',
                'yahoo_finance': 'failed_not_available',
            },
            'moved_at': datetime.now().isoformat(),
            'impact': 'Technical analysis incomplete without SMA_200'
        }
        
        # Write to error records
        try:
            with open(error_file, 'w') as f:
                json.dump(error_data, f, indent=2, default=str)
            
            self.logger.info(
                "Record moved to error_records due to missing SMA_200",
                ticker=record.ticker,
                date=record.date,
                file=str(error_file)
            )
            
            # Update statistics
            self.stats['total_moved'] += 1
            if record.ticker not in self.stats['by_ticker']:
                self.stats['by_ticker'][record.ticker] = 0
            self.stats['by_ticker'][record.ticker] += 1
            
        except Exception as e:
            self.logger.error(
                "Failed to move record to error_records",
                ticker=record.ticker,
                date=record.date,
                error=str(e)
            )
    
    def batch_move_missing_sma(
        self,
        records: List[StockDataRecord],
        job_id: Optional[str] = None
    ) -> tuple[List[StockDataRecord], List[StockDataRecord]]:
        """
        Process a batch of records and segregate those with missing SMA_200.
        
        Args:
            records: List of stock data records to check
            job_id: Optional job ID for tracking
            
        Returns:
            Tuple of (valid_records, error_records)
        """
        valid_records = []
        error_records = []
        
        for record in records:
            # Check if SMA_200 is missing
            if record.technical and record.technical.sma_200 is None:
                # Move to error records
                attempts = {
                    'historical_calculation': 'failed_insufficient_data',
                    'yahoo_finance': 'attempted',
                    'job_id': job_id or 'manual'
                }
                self.move_to_error_records_missing_sma(record, attempts=attempts)
                error_records.append(record)
            else:
                valid_records.append(record)
        
        if error_records:
            self.logger.warning(
                "Batch processing completed with missing SMA_200 records",
                total_records=len(records),
                valid=len(valid_records),
                moved_to_errors=len(error_records)
            )
        
        return valid_records, error_records
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about moved records.
        
        Returns:
            Dictionary with statistics
        """
        return {
            'total_records_moved': self.stats['total_moved'],
            'affected_tickers': len(self.stats['by_ticker']),
            'by_ticker': dict(self.stats['by_ticker']),
            'error_directory': str(self.error_base_path),
            'timestamp': datetime.now().isoformat()
        }
    
    def can_recover_sma(self, record: StockDataRecord) -> bool:
        """
        Check if a record's SMA_200 can potentially be recovered.
        
        Args:
            record: Stock data record to check
            
        Returns:
            True if recovery might be possible
        """
        # Check if the stock is old enough to have SMA_200
        try:
            from datetime import datetime, timedelta
            record_date = datetime.strptime(record.date, "%Y-%m-%d")
            
            # If the record date is less than 200 trading days from IPO,
            # SMA_200 legitimately cannot exist
            # Approximate: 200 trading days ≈ 280 calendar days
            min_date_for_sma = datetime(2020, 1, 1)  # Assume stocks before 2020 should have SMA_200
            
            if record_date < min_date_for_sma:
                return True  # Old enough stock, should have SMA_200
            
            # For newer dates, check if it's a new IPO
            # This would require additional IPO date data
            return True  # Assume recoverable by default
            
        except Exception:
            return True  # Assume recoverable if we can't determine
    
    def create_recovery_report(self) -> Dict[str, Any]:
        """
        Create a report of records that need SMA_200 recovery.
        
        Returns:
            Dictionary with recovery recommendations
        """
        report = {
            'timestamp': datetime.now().isoformat(),
            'total_missing_sma_200': self.stats['total_moved'],
            'recommendations': [],
            'priority_tickers': []
        }
        
        # Identify tickers with the most missing records
        if self.stats['by_ticker']:
            sorted_tickers = sorted(
                self.stats['by_ticker'].items(),
                key=lambda x: x[1],
                reverse=True
            )
            
            # Top 10 tickers with missing SMA_200
            report['priority_tickers'] = [
                {'ticker': ticker, 'missing_count': count}
                for ticker, count in sorted_tickers[:10]
            ]
            
            # Recommendations
            if sorted_tickers[0][1] > 10:
                report['recommendations'].append(
                    f"Ticker {sorted_tickers[0][0]} has {sorted_tickers[0][1]} records missing SMA_200. "
                    "Consider fetching extended historical data for this ticker."
                )
            
            if len(sorted_tickers) > 20:
                report['recommendations'].append(
                    f"{len(sorted_tickers)} tickers affected. "
                    "Consider implementing additional data sources for SMA_200."
                )
        
        return report