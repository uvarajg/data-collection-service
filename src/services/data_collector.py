import structlog
import asyncio
from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Any, Union, Tuple, Awaitable
import uuid

from ..models.data_models import StockDataRecord, CollectionJob, RecordMetadata
from .alpaca_service import AlpacaService
from .enhanced_data_service import EnhancedDataService
from .technical_indicators import TechnicalIndicatorsService
from .storage_service import StorageService
from .yfinance_fundamentals import YFinanceFundamentalsService
from .enriched_fundamentals_service import EnrichedFundamentalsService
from .google_sheets_service import GoogleSheetsService
from .yfinance_input_service import YFinanceInputService
from .error_rate_monitor import ErrorRateMonitor
from .completeness_scorer import CompletenessScorer
from .technical_indicator_validator import TechnicalIndicatorValidator
from .sma_error_handler import SMAErrorHandler

logger = structlog.get_logger()


class DataCollectionCoordinator:
    """
    Data collection coordinator that orchestrates the entire data collection process.
    Replicates AlgoAlchemist patterns with sequential processing and rate limiting.
    """
    
    def __init__(self, use_yfinance_input: bool = True, use_enriched_fundamentals: bool = True):
        self.alpaca_service = AlpacaService()
        self.enhanced_data_service = EnhancedDataService()  # New enhanced service with fallbacks
        self.technical_service = TechnicalIndicatorsService()
        self.storage_service = StorageService()

        # Choose fundamentals service based on configuration
        if use_enriched_fundamentals:
            self.fundamentals_service = EnrichedFundamentalsService()
            self.logger = logger.bind(service="data_coordinator", fundamentals="enriched")
        else:
            self.fundamentals_service = YFinanceFundamentalsService()
            self.logger = logger.bind(service="data_coordinator", fundamentals="fresh_api")

        self.sheets_service = None  # Lazy initialization for Google Sheets
        self.yfinance_input_service = YFinanceInputService() if use_yfinance_input else None
        self.use_yfinance_input = use_yfinance_input
        self.use_enriched_fundamentals = use_enriched_fundamentals
        self.error_monitor = ErrorRateMonitor()  # Error rate monitoring and alerting
        self.completeness_scorer = CompletenessScorer()  # Data completeness tracking
        self.indicator_validator = TechnicalIndicatorValidator()  # Technical indicator validation
        self.sma_error_handler = SMAErrorHandler()  # Handle missing SMA_200 records
        
        # Rate limiting settings (matching AlgoAlchemist)
        self.api_delay_seconds = 0.1  # 100ms between API calls
        self.max_concurrent_tickers = 1  # Sequential processing only
    
    def _get_sheets_service(self):
        """Lazy initialization of Google Sheets service"""
        if self.sheets_service is None:
            self.sheets_service = GoogleSheetsService()
        return self.sheets_service
    
    async def collect_ticker_data(
        self, 
        ticker: str, 
        start_date: str, 
        end_date: str,
        job_id: Optional[str] = None,
        include_technical_indicators: bool = True,
        include_fundamentals: bool = True
    ) -> Dict[str, Any]:
        """
        Collect complete data for a single ticker.
        
        Args:
            ticker: Stock symbol
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            job_id: Optional job ID for tracking
            include_technical_indicators: Whether to calculate technical indicators
            include_fundamentals: Whether to fetch fundamental data
            
        Returns:
            Dictionary with collection results
        """
        self.logger.info("Starting ticker data collection", 
                        ticker=ticker, start_date=start_date, end_date=end_date)
        
        result = {
            "ticker": ticker,
            "start_date": start_date,
            "end_date": end_date,
            "job_id": job_id,
            "status": "pending",
            "records_collected": 0,
            "records_saved": 0,
            "technical_indicators_calculated": 0,
            "fundamentals_added": 0,
            "error_message": None,
            "processing_time_seconds": 0
        }
        
        start_time = datetime.utcnow()
        
        try:
            # Step 1: Collect OHLCV data using enhanced service with fallback
            self.logger.info("Collecting OHLCV data with enhanced reliability", ticker=ticker)
            
            ohlcv_records = await self.enhanced_data_service.get_reliable_daily_bars(
                ticker=ticker,
                start_date=start_date,
                end_date=end_date,
                job_id=job_id,
                prefer_source="alpaca"  # Try Alpaca first, fallback to Yahoo Finance
            )
            
            if not ohlcv_records:
                result["status"] = "no_data"
                result["error_message"] = "No OHLCV data returned from Alpaca"
                return result
            
            result["records_collected"] = len(ohlcv_records)
            
            # Step 2: Calculate technical indicators if requested (enhanced method)
            if include_technical_indicators:
                self.logger.info("Calculating technical indicators with historical data", ticker=ticker)
                
                # Use enhanced method that fetches more historical data
                ohlcv_records = await self.technical_service.calculate_indicators_with_history(
                    ticker=ticker,
                    current_records=ohlcv_records,
                    alpaca_service=self.alpaca_service
                )
                
                # Count records with calculated indicators
                indicators_calculated = sum(1 for r in ohlcv_records if r.metadata.technical_indicators_calculated)
                result["technical_indicators_calculated"] = indicators_calculated
                
                self.logger.info("Technical indicators calculated", 
                               ticker=ticker, 
                               indicators_calculated=indicators_calculated)
                
                # Step 2b: Apply SMA_200 fallback for records missing it
                self.logger.info("Checking SMA_200 availability", ticker=ticker)
                ohlcv_records = await self.technical_service.enrich_with_fallback_sma(
                    ohlcv_records, ticker, self.fundamentals_service
                )
            
            # Step 3: Fetch fundamental data BEFORE validation (critical fix)
            if include_fundamentals:
                self.logger.info("Fetching fundamental data", ticker=ticker)
                
                try:
                    fundamental_data = await self.fundamentals_service.get_fundamentals(ticker)
                    
                    if fundamental_data:
                        # Add fundamentals to all records BEFORE validation
                        for record in ohlcv_records:
                            record.fundamental = fundamental_data
                            record.metadata.fundamental_data_available = True
                        
                        result["fundamentals_added"] = len(ohlcv_records)
                        self.logger.info("Fundamental data added to all records", ticker=ticker)
                    else:
                        self.logger.warning("No fundamental data available", ticker=ticker)
                        
                except Exception as e:
                    self.logger.error("Error fetching fundamental data", 
                                    ticker=ticker, error=str(e))
            
            # Step 4: NOW validate technical indicators (after fundamentals are added)
            if include_technical_indicators:
                self.logger.info("Validating technical indicators", ticker=ticker)
                valid_records, invalid_records = self.indicator_validator.validate_batch(ohlcv_records)
                
                if invalid_records:
                    self.logger.warning(f"Found {len(invalid_records)} records with invalid technical indicators",
                                      ticker=ticker, 
                                      invalid_count=len(invalid_records),
                                      valid_count=len(valid_records))
                    
                    # Now invalid records will have fundamentals when moved to error_records
                    self.indicator_validator.move_to_error_records(invalid_records, job_id or "manual")
                    
                    # Only keep valid records for further processing
                    ohlcv_records = valid_records
                    result["technical_validation_failed"] = len(invalid_records)
                
                result["technical_indicators_valid"] = len(valid_records) if invalid_records else result["technical_indicators_calculated"]
                
                self.logger.info("Technical indicators validated", 
                               ticker=ticker, 
                               valid_count=len(valid_records) if invalid_records else result["technical_indicators_calculated"])
                
                # Check if any records still missing SMA_200 after fallback
                missing_sma_records = [r for r in ohlcv_records 
                                      if r.technical and r.technical.sma_200 is None]
                
                if missing_sma_records:
                    self.logger.warning(f"Found {len(missing_sma_records)} records still missing SMA_200 after fallback",
                                      ticker=ticker)
                    
                    # Move records without SMA_200 to error_records (with fundamentals now)
                    valid_with_sma, error_without_sma = self.sma_error_handler.batch_move_missing_sma(
                        ohlcv_records, job_id
                    )
                    
                    # Update records list to exclude those moved to errors
                    ohlcv_records = valid_with_sma
                    result["sma_200_missing_moved_to_errors"] = len(error_without_sma)
                    
                    self.logger.info(f"Moved {len(error_without_sma)} records to error_records due to missing SMA_200",
                                   ticker=ticker)
            
            # Step 5: Calculate completeness scores for quality tracking
            self.logger.info("Calculating data completeness scores", ticker=ticker)
            
            # Add completeness scores to metadata and track quality
            for record in ohlcv_records:
                self.completeness_scorer.add_completeness_to_metadata(record)
            
            # Generate completeness statistics
            completeness_stats = self.completeness_scorer.score_batch(ohlcv_records)
            result["completeness_stats"] = completeness_stats
            
            self.logger.info("Data completeness analyzed", 
                           ticker=ticker,
                           average_score=completeness_stats['average_score'],
                           completeness_level=completeness_stats['completeness_distribution'])
            
            # Step 6: Final sort by date to ensure chronological order (CRITICAL)
            # This prevents chronological validation errors in the validation service
            self.logger.info("Sorting records chronologically before storage", ticker=ticker)
            ohlcv_records.sort(key=lambda r: r.date)
            self.logger.debug("Records sorted chronologically", 
                             ticker=ticker,
                             first_date=ohlcv_records[0].date if ohlcv_records else None,
                             last_date=ohlcv_records[-1].date if ohlcv_records else None,
                             record_count=len(ohlcv_records))
            
            # Step 7: Save records to storage
            self.logger.info("Saving records to storage", ticker=ticker)
            
            save_results = await self.storage_service.save_daily_records_batch(ohlcv_records)
            result["records_saved"] = save_results["successful"]
            
            if save_results["failed"] > 0:
                self.logger.warning("Some records failed to save", 
                                  ticker=ticker, failed_count=save_results["failed"])
            
            # Step 8: Determine final status
            if result["records_saved"] == result["records_collected"]:
                result["status"] = "completed"
            elif result["records_saved"] > 0:
                result["status"] = "partial_success"
            else:
                result["status"] = "failed"
                result["error_message"] = "All records failed to save"
            
        except Exception as e:
            self.logger.error("Error during ticker data collection", 
                            ticker=ticker, error=str(e))
            result["status"] = "error"
            result["error_message"] = str(e)
        
        finally:
            end_time = datetime.utcnow()
            result["processing_time_seconds"] = (end_time - start_time).total_seconds()
            
            self.logger.info("Ticker data collection completed", 
                           ticker=ticker, 
                           status=result["status"],
                           records_collected=result["records_collected"],
                           records_saved=result["records_saved"],
                           processing_time=result["processing_time_seconds"])
        
        return result
    
    async def collect_multiple_tickers(
        self,
        tickers: List[str],
        start_date: str,
        end_date: str,
        job_id: Optional[str] = None
    ) -> CollectionJob:
        """
        Collect data for multiple tickers sequentially (matching AlgoAlchemist pattern).
        
        Args:
            tickers: List of stock symbols
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            job_id: Optional job ID, will create new one if not provided
            
        Returns:
            CollectionJob with results
        """
        if not job_id:
            job_id = str(uuid.uuid4())
        
        # Create collection job
        collection_job = CollectionJob(
            job_id=job_id,
            tickers=tickers,
            start_date=start_date,
            end_date=end_date,
            job_status="running",
            started_at=datetime.utcnow(),
            total_records=0
        )
        
        # Save initial job state
        await self.storage_service.save_collection_job(collection_job)
        
        self.logger.info("Starting batch data collection", 
                        job_id=job_id, ticker_count=len(tickers))
        
        try:
            ticker_results = []
            
            # Process tickers sequentially with rate limiting
            for i, ticker in enumerate(tickers):
                self.logger.info("Processing ticker", 
                               ticker=ticker, 
                               progress=f"{i+1}/{len(tickers)}")
                
                # Rate limiting delay between tickers
                if i > 0:
                    await asyncio.sleep(self.api_delay_seconds)
                
                # Collect data for this ticker
                ticker_result = await self.collect_ticker_data(
                    ticker=ticker,
                    start_date=start_date,
                    end_date=end_date,
                    job_id=job_id
                )
                
                ticker_results.append(ticker_result)
                
                # Update job statistics
                collection_job.total_records += ticker_result["records_collected"]
                collection_job.successful_records += ticker_result["records_saved"]
                
                if ticker_result["status"] in ["error", "failed"]:
                    collection_job.failed_records += 1
                    collection_job.error_summary[ticker] = ticker_result.get("error_message", "Unknown error")
                
                # Save updated job state
                await self.storage_service.update_collection_job(collection_job)
            
            # Update final job status
            collection_job.completed_at = datetime.utcnow()
            
            if collection_job.failed_records == 0:
                collection_job.job_status = "completed"
            elif collection_job.successful_records > 0:
                collection_job.job_status = "partial_success"
            else:
                collection_job.job_status = "failed"
            
            # Final job save
            await self.storage_service.update_collection_job(collection_job)
            
            self.logger.info("Batch data collection completed", 
                           job_id=job_id,
                           status=collection_job.job_status,
                           total_records=collection_job.total_records,
                           successful_records=collection_job.successful_records,
                           failed_records=collection_job.failed_records)
        
            # Monitor error rates after batch collection
            try:
                monitoring_result = await self.monitor_error_rates_and_alert(24)
                collection_job.error_summary["post_collection_monitoring"] = {
                    "error_rate": monitoring_result["stats"]["error_rate"],
                    "threshold_exceeded": monitoring_result.get("threshold_exceeded", False),
                    "alert_sent": monitoring_result.get("alert_sent", False)
                }
            except Exception as monitor_error:
                self.logger.warning("Error rate monitoring failed after batch collection", 
                                  error=str(monitor_error))
        
        except Exception as e:
            self.logger.error("Error during batch data collection", 
                            job_id=job_id, error=str(e))
            
            collection_job.job_status = "error"
            collection_job.error_summary["batch_error"] = str(e)
            collection_job.completed_at = datetime.utcnow()
            
            await self.storage_service.update_collection_job(collection_job)
        
        return collection_job
    
    async def collect_latest_data(self, tickers: List[str]) -> Dict[str, Any]:
        """
        Collect the latest available data for a list of tickers.
        
        Args:
            tickers: List of stock symbols
            
        Returns:
            Dictionary with collection results
        """
        self.logger.info("Collecting latest data", ticker_count=len(tickers))
        
        results = {
            "job_id": str(uuid.uuid4()),
            "timestamp": datetime.utcnow().isoformat(),
            "tickers_processed": 0,
            "tickers_successful": 0,
            "tickers_failed": 0,
            "ticker_results": {}
        }
        
        for ticker in tickers:
            try:
                # Rate limiting
                if results["tickers_processed"] > 0:
                    await asyncio.sleep(self.api_delay_seconds)
                
                # Get latest bar
                latest_record = await self.alpaca_service.get_latest_bar(ticker, results["job_id"])
                
                if latest_record:
                    # Save to storage
                    saved = await self.storage_service.save_daily_record(latest_record)
                    
                    results["ticker_results"][ticker] = {
                        "status": "success" if saved else "save_failed",
                        "date": latest_record.date,
                        "close": latest_record.close,
                        "volume": latest_record.volume
                    }
                    
                    if saved:
                        results["tickers_successful"] += 1
                    else:
                        results["tickers_failed"] += 1
                else:
                    results["ticker_results"][ticker] = {
                        "status": "no_data",
                        "error": "No latest data available"
                    }
                    results["tickers_failed"] += 1
            
            except Exception as e:
                self.logger.error("Error collecting latest data for ticker", 
                                ticker=ticker, error=str(e))
                
                results["ticker_results"][ticker] = {
                    "status": "error",
                    "error": str(e)
                }
                results["tickers_failed"] += 1
            
            results["tickers_processed"] += 1
        
        self.logger.info("Latest data collection completed",
                        processed=results["tickers_processed"],
                        successful=results["tickers_successful"],
                        failed=results["tickers_failed"])
        
        return results
    
    async def validate_services(self) -> Dict[str, bool]:
        """
        Validate all services are working correctly.
        
        Returns:
            Dictionary with service validation results
        """
        self.logger.info("Validating services")
        
        validation_results = {
            "alpaca_connection": False,
            "storage_accessible": False,
            "technical_indicators_available": False,
            "enhanced_data_service": False
        }
        
        try:
            # Test Alpaca connection
            validation_results["alpaca_connection"] = await self.alpaca_service.validate_connection()
            
            # Test storage
            stats = self.storage_service.get_storage_stats()
            validation_results["storage_accessible"] = "error" not in stats
            
            # Test technical indicators (simple test)
            try:
                import talib
                validation_results["technical_indicators_available"] = True
            except ImportError:
                validation_results["technical_indicators_available"] = False
            
            # Test enhanced data service health
            try:
                health_status = await self.enhanced_data_service.get_data_source_health()
                # Consider service healthy if at least one source is working
                validation_results["enhanced_data_service"] = any(
                    status["status"] == "healthy" for status in health_status.values()
                )
                self.logger.info("Enhanced data service health check", health_status=health_status)
            except Exception as e:
                self.logger.error("Enhanced data service health check failed", error=str(e))
                validation_results["enhanced_data_service"] = False
            
        except Exception as e:
            self.logger.error("Error during service validation", error=str(e))
        
        self.logger.info("Service validation completed", results=validation_results)
        return validation_results
    
    async def collect_most_active_tickers_one_month(self) -> Dict[str, Any]:
        """
        Collect one month of data for tickers from Google Sheets "Most Active" sheet.
        This replicates the AlgoAlchemist pattern for batch data collection.
        
        Returns:
            Dictionary with collection results
        """
        self.logger.info("Starting collection for Most Active tickers - one month data")
        
        # Calculate date range - one month from today
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=30)
        
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")
        
        job_id = str(uuid.uuid4())
        
        result = {
            "job_id": job_id,
            "start_date": start_date_str,
            "end_date": end_date_str,
            "status": "pending",
            "tickers_fetched_from_sheets": 0,
            "tickers_processed": 0,
            "total_records_collected": 0,
            "total_records_saved": 0,
            "error_message": None,
            "processing_time_seconds": 0,
            "tickers": [],
            "ticker_results": []
        }
        
        start_time = datetime.utcnow()
        
        try:
            # Step 1: Fetch tickers from input source
            if self.use_yfinance_input:
                self.logger.info("Fetching ticker list from YFinance enriched data")

                # Fetch tickers from enriched data (filtering already applied during enrichment)
                tickers = await self.yfinance_input_service.fetch_active_tickers()

                if not tickers:
                    result["status"] = "no_tickers"
                    result["error_message"] = "No tickers found in YFinance enriched data"
                    return result

                result["tickers_fetched_from_yfinance"] = len(tickers)
                result["tickers"] = tickers

                self.logger.info("Tickers fetched from YFinance enriched data",
                               ticker_count=len(tickers),
                               tickers=tickers[:10])  # Log first 10
            else:
                # Fallback to Google Sheets
                self.logger.info("Fetching ticker list from Google Sheets")

                sheets_service = self._get_sheets_service()
                tickers = await sheets_service.fetch_active_tickers()

                if not tickers:
                    result["status"] = "no_tickers"
                    result["error_message"] = "No tickers found in Google Sheets"
                    return result

                result["tickers_fetched_from_sheets"] = len(tickers)
                result["tickers"] = tickers

                self.logger.info("Tickers fetched from Google Sheets",
                               ticker_count=len(tickers),
                               tickers=tickers[:10])  # Log first 10
            
            # Step 2: Collect data for all tickers using batch method
            self.logger.info("Starting batch data collection for one month", 
                           ticker_count=len(tickers),
                           date_range=f"{start_date_str} to {end_date_str}")
            
            collection_job = await self.collect_multiple_tickers(
                tickers=tickers,
                start_date=start_date_str,
                end_date=end_date_str,
                job_id=job_id
            )
            
            # Update result with batch collection results
            result["tickers_processed"] = len(tickers)
            result["total_records_collected"] = collection_job.total_records
            result["total_records_saved"] = collection_job.successful_records
            result["status"] = collection_job.job_status
            
            if collection_job.error_summary:
                result["error_summary"] = collection_job.error_summary
            
            # Generate summary statistics
            successful_tickers = len(tickers) - collection_job.failed_records
            
            self.logger.info("Most Active tickers collection completed",
                           job_id=job_id,
                           status=collection_job.job_status,
                           tickers_fetched=len(tickers),
                           tickers_successful=successful_tickers,
                           tickers_failed=collection_job.failed_records,
                           total_records=collection_job.total_records,
                           successful_records=collection_job.successful_records)
            
        except Exception as e:
            self.logger.error("Error during Most Active tickers collection", 
                            job_id=job_id, error=str(e))
            result["status"] = "error"
            result["error_message"] = str(e)
        
        finally:
            end_time = datetime.utcnow()
            result["processing_time_seconds"] = (end_time - start_time).total_seconds()
        
        return result
    
    async def collect_demo_tickers_one_month(self, demo_tickers: List[str]) -> Dict[str, Any]:
        """
        Collect one month of data for demo tickers with full technical indicators and fundamentals.
        This replicates the AlgoAlchemist pattern but with a predefined ticker list.
        
        Args:
            demo_tickers: List of ticker symbols to collect data for
            
        Returns:
            Dictionary with collection results
        """
        self.logger.info("Starting demo tickers collection - one month data", ticker_count=len(demo_tickers))
        
        # Calculate date range - one month from today
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=30)
        
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")
        
        job_id = str(uuid.uuid4())
        
        result = {
            "job_id": job_id,
            "start_date": start_date_str,
            "end_date": end_date_str,
            "status": "pending",
            "tickers_processed": 0,
            "total_records_collected": 0,
            "total_records_saved": 0,
            "error_message": None,
            "processing_time_seconds": 0,
            "tickers": demo_tickers,
            "ticker_results": []
        }
        
        start_time = datetime.utcnow()
        
        try:
            self.logger.info("Starting batch data collection for demo tickers", 
                           ticker_count=len(demo_tickers),
                           tickers=demo_tickers,
                           date_range=f"{start_date_str} to {end_date_str}")
            
            collection_job = await self.collect_multiple_tickers(
                tickers=demo_tickers,
                start_date=start_date_str,
                end_date=end_date_str,
                job_id=job_id
            )
            
            # Update result with batch collection results
            result["tickers_processed"] = len(demo_tickers)
            result["total_records_collected"] = collection_job.total_records
            result["total_records_saved"] = collection_job.successful_records
            result["status"] = collection_job.job_status
            
            if collection_job.error_summary:
                result["error_summary"] = collection_job.error_summary
            
            # Generate summary statistics
            successful_tickers = len(demo_tickers) - collection_job.failed_records
            
            self.logger.info("Demo tickers collection completed",
                           job_id=job_id,
                           status=collection_job.job_status,
                           tickers_processed=len(demo_tickers),
                           tickers_successful=successful_tickers,
                           tickers_failed=collection_job.failed_records,
                           total_records=collection_job.total_records,
                           successful_records=collection_job.successful_records)
            
        except Exception as e:
            self.logger.error("Error during demo tickers collection", 
                            job_id=job_id, error=str(e))
            result["status"] = "error"
            result["error_message"] = str(e)
        
        finally:
            end_time = datetime.utcnow()
            result["processing_time_seconds"] = (end_time - start_time).total_seconds()
        
        return result
    
    async def validate_google_sheets_connection(self) -> bool:
        """
        Validate Google Sheets connection.
        
        Returns:
            True if connection is working, False otherwise
        """
        try:
            sheets_service = self._get_sheets_service()
            return await sheets_service.validate_connection()
        except Exception as e:
            self.logger.error("Google Sheets validation failed", error=str(e))
            return False
    
    async def monitor_error_rates_and_alert(self, window_hours: int = 24) -> Dict[str, Any]:
        """
        Monitor error rates and send alerts if critical threshold exceeded.
        
        This method should be called after major data collection operations
        or on a scheduled basis to ensure system health.
        
        Args:
            window_hours: Monitoring window in hours (default 24)
            
        Returns:
            Dictionary with monitoring results and alert status
        """
        self.logger.info("Starting error rate monitoring", window_hours=window_hours)
        
        try:
            # Check error rates and trigger alerts if necessary
            monitoring_result = await self.error_monitor.check_error_rates(window_hours)
            
            # Log the results
            if monitoring_result.get("threshold_exceeded", False):
                self.logger.error("CRITICAL: Error rate threshold exceeded",
                                error_rate=monitoring_result["stats"]["error_rate"],
                                threshold=monitoring_result["critical_threshold"],
                                alert_sent=monitoring_result.get("alert_sent", False))
            else:
                self.logger.info("Error rate monitoring completed - within threshold",
                               error_rate=monitoring_result["stats"]["error_rate"],
                               threshold=monitoring_result["critical_threshold"])
            
            return monitoring_result
            
        except Exception as e:
            self.logger.error("Error rate monitoring failed", error=str(e))
            return {
                "status": "monitoring_failed",
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }
    
    async def get_collection_health_report(self, days: int = 7) -> Dict[str, Any]:
        """
        Generate comprehensive health report for data collection service.
        
        Args:
            days: Number of days to analyze for trends
            
        Returns:
            Dictionary with comprehensive health metrics
        """
        self.logger.info("Generating collection health report", analysis_days=days)
        
        try:
            # Get error trends
            error_trends = await self.error_monitor.get_error_trends(days)
            
            # Get current error rates (last 24 hours)
            current_status = await self.error_monitor.check_error_rates(24)
            
            # Check service health
            service_health = await self.enhanced_data_service.health_check()
            
            # Compile comprehensive report
            health_report = {
                "report_timestamp": datetime.utcnow().isoformat(),
                "analysis_period_days": days,
                "current_status": {
                    "error_rate_24h": current_status["stats"]["error_rate"],
                    "total_errors_24h": current_status["stats"]["total_errors"],
                    "total_attempted_24h": current_status["stats"]["total_attempted"],
                    "threshold_exceeded": current_status.get("threshold_exceeded", False),
                    "critical_threshold": current_status.get("critical_threshold", 0.02)
                },
                "error_trends": error_trends,
                "service_health": service_health,
                "recommendations": []
            }
            
            # Add recommendations based on analysis
            if current_status.get("threshold_exceeded", False):
                health_report["recommendations"].append({
                    "priority": "CRITICAL",
                    "issue": "Error rate exceeds critical threshold",
                    "action": "Investigate data source failures and system issues immediately"
                })
            
            if error_trends.get("trend") == "worsening":
                health_report["recommendations"].append({
                    "priority": "HIGH",
                    "issue": "Error rate trend is worsening",
                    "action": "Review recent changes and monitor data sources closely"
                })
            
            if service_health["alpaca"]["status"] != "healthy":
                health_report["recommendations"].append({
                    "priority": "MEDIUM",
                    "issue": "Alpaca API health issues detected",
                    "action": "Check Alpaca API credentials and service status"
                })
            
            if service_health["yfinance"]["status"] != "healthy":
                health_report["recommendations"].append({
                    "priority": "MEDIUM",
                    "issue": "Yahoo Finance service issues detected",
                    "action": "Monitor Yahoo Finance reliability and consider alternative sources"
                })
            
            self.logger.info("Health report generated successfully",
                           current_error_rate=health_report["current_status"]["error_rate_24h"],
                           trend=error_trends.get("trend", "unknown"),
                           recommendations_count=len(health_report["recommendations"]))
            
            return health_report
            
        except Exception as e:
            self.logger.error("Failed to generate health report", error=str(e))
            return {
                "status": "report_generation_failed",
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }