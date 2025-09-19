"""
Error rate monitoring service for data collection failures.
"""

import structlog
import json
import os
import glob
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple, Optional
from collections import defaultdict, Counter
from dataclasses import dataclass

from .email_notification_service import EmailNotificationService, ErrorSummary

logger = structlog.get_logger()


@dataclass
class CollectionStats:
    """Statistics for data collection performance"""
    total_attempted: int
    total_successful: int
    total_errors: int
    error_rate: float
    timeframe_start: datetime
    timeframe_end: datetime
    failed_tickers: List[str]
    error_details: List[Dict[str, Any]]


class ErrorRateMonitor:
    """
    Monitor error rates and trigger alerts when thresholds are exceeded.
    
    Analyzes both successful data collection and error records to calculate
    real-time error rates and send notifications when critical thresholds
    are breached.
    """
    
    def __init__(self):
        self.logger = logger.bind(service="error_rate_monitor")
        self.email_service = EmailNotificationService()
        
        # Configuration
        self.critical_threshold = 0.02  # 2% error rate
        self.data_base_path = "/workspaces/data"
        self.historical_data_path = f"{self.data_base_path}/historical/daily"
        self.error_records_path = f"{self.data_base_path}/error_records/daily"
        
        # Monitoring window (analyze last 24 hours by default)
        self.monitoring_window_hours = 24
    
    async def check_error_rates(self, custom_window_hours: Optional[int] = None) -> Dict[str, Any]:
        """
        Check current error rates and trigger alerts if necessary.
        
        Args:
            custom_window_hours: Override default 24-hour monitoring window
            
        Returns:
            Dictionary with monitoring results and actions taken
        """
        window_hours = custom_window_hours or self.monitoring_window_hours
        
        self.logger.info("Starting error rate analysis", window_hours=window_hours)
        
        try:
            # Calculate time window
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=window_hours)
            
            # Analyze collection performance
            stats = await self._analyze_collection_stats(start_time, end_time)
            
            result = {
                "analysis_timestamp": end_time.isoformat(),
                "timeframe": f"{start_time.strftime('%Y-%m-%d %H:%M')} to {end_time.strftime('%Y-%m-%d %H:%M')}",
                "window_hours": window_hours,
                "stats": {
                    "total_attempted": stats.total_attempted,
                    "total_successful": stats.total_successful,
                    "total_errors": stats.total_errors,
                    "error_rate": stats.error_rate,
                    "failed_tickers_count": len(stats.failed_tickers)
                },
                "threshold_exceeded": stats.error_rate >= self.critical_threshold,
                "critical_threshold": self.critical_threshold,
                "alert_sent": False,
                "alert_recipients": self.email_service.alert_recipients
            }
            
            # Check if threshold exceeded
            if stats.error_rate >= self.critical_threshold:
                self.logger.warning("Error rate threshold exceeded", 
                                  error_rate=stats.error_rate,
                                  threshold=self.critical_threshold)
                
                # Create error summary for alert
                error_summary = await self._create_error_summary(stats)
                
                # Send critical failure alert
                alert_sent = await self.email_service.send_critical_failure_alert(
                    error_summary, 
                    stats.error_details[:50]  # Include up to 50 error details
                )
                
                result["alert_sent"] = alert_sent
                result["error_summary"] = {
                    "total_errors": error_summary.total_errors,
                    "error_rate": error_summary.error_rate,
                    "failed_tickers": error_summary.failed_tickers,
                    "most_common_error": error_summary.most_common_error
                }
                
                if alert_sent:
                    self.logger.info("Critical failure alert sent successfully")
                else:
                    self.logger.error("Failed to send critical failure alert")
            
            else:
                self.logger.info("Error rate within acceptable threshold", 
                               error_rate=stats.error_rate,
                               threshold=self.critical_threshold)
            
            return result
            
        except Exception as e:
            self.logger.error("Error rate analysis failed", error=str(e))
            return {
                "analysis_timestamp": datetime.now().isoformat(),
                "error": str(e),
                "status": "analysis_failed"
            }
    
    async def _analyze_collection_stats(
        self, 
        start_time: datetime, 
        end_time: datetime
    ) -> CollectionStats:
        """
        Analyze collection statistics within the specified time window.
        
        Counts successful data files and error records to calculate
        comprehensive error rates.
        """
        # Count successful collections
        successful_files = await self._count_successful_collections(start_time, end_time)
        
        # Count error records  
        error_records = await self._count_error_records(start_time, end_time)
        
        # Calculate statistics
        total_errors = len(error_records)
        total_successful = len(successful_files)
        total_attempted = total_successful + total_errors
        
        error_rate = total_errors / total_attempted if total_attempted > 0 else 0.0
        
        # Extract failed tickers
        failed_tickers = list(set(error["ticker"] for error in error_records))
        
        return CollectionStats(
            total_attempted=total_attempted,
            total_successful=total_successful,
            total_errors=total_errors,
            error_rate=error_rate,
            timeframe_start=start_time,
            timeframe_end=end_time,
            failed_tickers=failed_tickers,
            error_details=error_records
        )
    
    async def _count_successful_collections(
        self, 
        start_time: datetime, 
        end_time: datetime
    ) -> List[Dict[str, Any]]:
        """Count successful data collection files within time window"""
        successful_files = []
        
        try:
            # Search pattern for all historical data files
            pattern = f"{self.historical_data_path}/*/*/*/*.json"
            
            for file_path in glob.glob(pattern):
                try:
                    # Get file modification time
                    file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                    
                    if start_time <= file_mtime <= end_time:
                        # Extract ticker and date from path
                        path_parts = file_path.split('/')
                        ticker = path_parts[-4]  # /workspaces/data/historical/daily/TICKER/YEAR/MM/file.json
                        file_name = os.path.basename(file_path)
                        date_str = file_name.replace('.json', '')
                        
                        successful_files.append({
                            "ticker": ticker,
                            "date": date_str,
                            "file_path": file_path,
                            "collection_time": file_mtime.isoformat()
                        })
                        
                except (OSError, IndexError, ValueError) as e:
                    self.logger.debug("Skipping file in successful count", 
                                    file_path=file_path, error=str(e))
                    continue
            
            self.logger.info("Counted successful collections", count=len(successful_files))
            return successful_files
            
        except Exception as e:
            self.logger.error("Failed to count successful collections", error=str(e))
            return []
    
    async def _count_error_records(
        self, 
        start_time: datetime, 
        end_time: datetime
    ) -> List[Dict[str, Any]]:
        """Count error records within time window"""
        error_records = []
        
        try:
            # Search pattern for all error record files
            pattern = f"{self.error_records_path}/*/*/*/*.json"
            
            for file_path in glob.glob(pattern):
                try:
                    with open(file_path, 'r') as f:
                        file_errors = json.load(f)
                    
                    # Handle both single error object and array of errors
                    if isinstance(file_errors, dict):
                        file_errors = [file_errors]
                    
                    for error_record in file_errors:
                        try:
                            error_time = datetime.fromisoformat(
                                error_record.get("error_timestamp", "").replace('Z', '')
                            )
                            
                            if start_time <= error_time <= end_time:
                                error_records.append(error_record)
                                
                        except ValueError:
                            # Skip records with invalid timestamps
                            continue
                            
                except (json.JSONDecodeError, OSError) as e:
                    self.logger.debug("Skipping error file", file_path=file_path, error=str(e))
                    continue
            
            self.logger.info("Counted error records", count=len(error_records))
            return error_records
            
        except Exception as e:
            self.logger.error("Failed to count error records", error=str(e))
            return []
    
    async def _create_error_summary(self, stats: CollectionStats) -> ErrorSummary:
        """Create error summary for alert notifications"""
        
        # Analyze error types
        error_types = Counter()
        error_messages = []
        
        for error in stats.error_details:
            error_type = error.get("error_type", "unknown")
            error_types[error_type] += 1
            error_messages.append(error.get("error_message", ""))
        
        # Find most common error message
        most_common_error = "Multiple error types detected"
        if error_messages:
            common_messages = Counter(error_messages)
            most_common_error = common_messages.most_common(1)[0][0]
        
        # Create timeframe description
        timeframe = f"{stats.timeframe_start.strftime('%Y-%m-%d %H:%M')} to {stats.timeframe_end.strftime('%Y-%m-%d %H:%M')}"
        
        return ErrorSummary(
            total_errors=stats.total_errors,
            total_attempted=stats.total_attempted,
            error_rate=stats.error_rate,
            failed_tickers=stats.failed_tickers,
            error_types=dict(error_types),
            error_timeframe=timeframe,
            most_common_error=most_common_error
        )
    
    async def get_error_trends(self, days: int = 7) -> Dict[str, Any]:
        """
        Analyze error trends over the specified number of days.
        
        Args:
            days: Number of days to analyze
            
        Returns:
            Dictionary with trend analysis
        """
        try:
            end_time = datetime.now()
            start_time = end_time - timedelta(days=days)
            
            # Get daily error rates
            daily_rates = []
            
            for i in range(days):
                day_start = start_time + timedelta(days=i)
                day_end = day_start + timedelta(days=1)
                
                day_stats = await self._analyze_collection_stats(day_start, day_end)
                
                daily_rates.append({
                    "date": day_start.strftime('%Y-%m-%d'),
                    "error_rate": day_stats.error_rate,
                    "total_errors": day_stats.total_errors,
                    "total_attempted": day_stats.total_attempted,
                    "failed_tickers": len(day_stats.failed_tickers)
                })
            
            # Calculate trend metrics
            recent_rates = [day["error_rate"] for day in daily_rates[-3:]]  # Last 3 days
            avg_recent_rate = sum(recent_rates) / len(recent_rates) if recent_rates else 0
            
            all_rates = [day["error_rate"] for day in daily_rates]
            avg_overall_rate = sum(all_rates) / len(all_rates) if all_rates else 0
            
            trend = "stable"
            if avg_recent_rate > avg_overall_rate * 1.5:
                trend = "worsening"
            elif avg_recent_rate < avg_overall_rate * 0.5:
                trend = "improving"
            
            return {
                "analysis_period": f"{days} days",
                "daily_rates": daily_rates,
                "trend": trend,
                "avg_recent_rate": avg_recent_rate,
                "avg_overall_rate": avg_overall_rate,
                "max_daily_rate": max(all_rates) if all_rates else 0,
                "min_daily_rate": min(all_rates) if all_rates else 0
            }
            
        except Exception as e:
            self.logger.error("Failed to analyze error trends", error=str(e))
            return {"error": str(e), "status": "analysis_failed"}