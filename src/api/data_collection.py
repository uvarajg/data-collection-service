from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime, date
import structlog

from ..services.data_collector import DataCollectionCoordinator
from ..models.data_models import CollectionJob

logger = structlog.get_logger()
router = APIRouter()

# Lazy initialization of coordinator
coordinator = None

def get_coordinator():
    global coordinator
    if coordinator is None:
        coordinator = DataCollectionCoordinator()
    return coordinator


class CollectTickerRequest(BaseModel):
    ticker: str = Field(..., description="Stock symbol (e.g., 'AAPL')")
    start_date: str = Field(..., description="Start date in YYYY-MM-DD format")
    end_date: str = Field(..., description="End date in YYYY-MM-DD format")
    include_technical_indicators: bool = Field(default=True, description="Calculate technical indicators")
    include_fundamentals: bool = Field(default=True, description="Fetch fundamental data")


class BatchCollectRequest(BaseModel):
    tickers: List[str] = Field(..., description="List of stock symbols")
    start_date: str = Field(..., description="Start date in YYYY-MM-DD format")
    end_date: str = Field(..., description="End date in YYYY-MM-DD format")


class CollectionResponse(BaseModel):
    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None
    job_id: Optional[str] = None


@router.post("/collect/daily/{ticker}", response_model=CollectionResponse)
async def collect_daily_ticker(
    ticker: str,
    request: CollectTickerRequest
):
    """
    Collect daily OHLCV data for a single ticker.
    """
    logger.info("Single ticker collection requested", 
               ticker=ticker, 
               start_date=request.start_date, 
               end_date=request.end_date)
    
    try:
        # Validate ticker format
        ticker = ticker.upper().strip()
        if not ticker or len(ticker) > 10:
            raise HTTPException(status_code=400, detail="Invalid ticker symbol")
        
        # Validate dates
        try:
            datetime.strptime(request.start_date, "%Y-%m-%d")
            datetime.strptime(request.end_date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
        
        # Collect data
        coord = get_coordinator()
        result = await coord.collect_ticker_data(
            ticker=ticker,
            start_date=request.start_date,
            end_date=request.end_date,
            include_technical_indicators=request.include_technical_indicators,
            include_fundamentals=request.include_fundamentals
        )
        
        if result["status"] in ["completed", "partial_success"]:
            return CollectionResponse(
                success=True,
                message=f"Successfully collected {result['records_saved']} records for {ticker}",
                data=result,
                job_id=result.get("job_id")
            )
        else:
            return CollectionResponse(
                success=False,
                message=f"Failed to collect data for {ticker}: {result.get('error_message', 'Unknown error')}",
                data=result,
                job_id=result.get("job_id")
            )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error in single ticker collection", ticker=ticker, error=str(e))
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.post("/collect/batch", response_model=CollectionResponse)
async def collect_batch_data(
    request: BatchCollectRequest,
    background_tasks: BackgroundTasks
):
    """
    Collect daily OHLCV data for multiple tickers (runs in background).
    """
    logger.info("Batch collection requested", 
               ticker_count=len(request.tickers), 
               start_date=request.start_date, 
               end_date=request.end_date)
    
    try:
        # Validate input
        if not request.tickers or len(request.tickers) > 50:
            raise HTTPException(status_code=400, detail="Invalid number of tickers (1-50 allowed)")
        
        # Validate and clean tickers
        cleaned_tickers = []
        for ticker in request.tickers:
            cleaned = ticker.upper().strip()
            if cleaned and len(cleaned) <= 10:
                cleaned_tickers.append(cleaned)
        
        if not cleaned_tickers:
            raise HTTPException(status_code=400, detail="No valid tickers provided")
        
        # Validate dates
        try:
            datetime.strptime(request.start_date, "%Y-%m-%d")
            datetime.strptime(request.end_date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
        
        # Start background task for batch collection
        background_tasks.add_task(
            _background_batch_collection,
            cleaned_tickers,
            request.start_date,
            request.end_date
        )
        
        return CollectionResponse(
            success=True,
            message=f"Batch collection started for {len(cleaned_tickers)} tickers",
            data={
                "tickers": cleaned_tickers,
                "start_date": request.start_date,
                "end_date": request.end_date,
                "status": "started"
            }
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error starting batch collection", error=str(e))
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/data/{ticker}/{date}")
async def get_ticker_data(ticker: str, date: str):
    """
    Retrieve stored data for a specific ticker and date.
    """
    logger.info("Data retrieval requested", ticker=ticker, date=date)
    
    try:
        # Validate ticker and date
        ticker = ticker.upper().strip()
        try:
            datetime.strptime(date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
        
        # Load data from storage
        coord = get_coordinator()
        record_data = await coord.storage_service.load_daily_record(ticker, date)
        
        if record_data:
            return {
                "success": True,
                "message": f"Data found for {ticker} on {date}",
                "data": record_data
            }
        else:
            raise HTTPException(status_code=404, detail=f"No data found for {ticker} on {date}")
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error retrieving data", ticker=ticker, date=date, error=str(e))
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/jobs/{job_id}/status")
async def get_job_status(job_id: str):
    """
    Get the status of a collection job.
    """
    logger.info("Job status requested", job_id=job_id)
    
    try:
        coord = get_coordinator()
        job_data = await coord.storage_service.load_collection_job(job_id)
        
        if job_data:
            return {
                "success": True,
                "message": "Job status retrieved successfully",
                "data": job_data
            }
        else:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error retrieving job status", job_id=job_id, error=str(e))
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.post("/collect/most-active-one-month")
async def collect_most_active_one_month():
    """
    Collect one month of data for tickers from Google Sheets "Most Active" sheet.
    """
    logger.info("Most Active one month collection requested")
    
    try:
        coord = get_coordinator()
        results = await coord.collect_most_active_tickers_one_month()
        
        success = results["status"] in ["completed", "partial_success"]
        
        return CollectionResponse(
            success=success,
            message=f"Most Active collection {results['status']}: {results['tickers_processed']} tickers processed, {results['total_records_saved']} records saved",
            data=results,
            job_id=results["job_id"]
        )
    
    except Exception as e:
        logger.error("Error collecting Most Active data", error=str(e))
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.post("/collect/demo-tickers-one-month")
async def collect_demo_tickers_one_month():
    """
    Collect one month of data for a demo set of popular tickers.
    This demonstrates the full data collection pipeline with technical indicators and fundamentals.
    """
    logger.info("Demo tickers one month collection requested")
    
    try:
        # Popular tech and market tickers for demonstration
        demo_tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META", "NFLX"]
        
        coord = get_coordinator()
        results = await coord.collect_demo_tickers_one_month(demo_tickers)
        
        success = results["status"] in ["completed", "partial_success"]
        
        return CollectionResponse(
            success=success,
            message=f"Demo collection {results['status']}: {results['tickers_processed']} tickers processed, {results['total_records_saved']} records saved",
            data=results,
            job_id=results["job_id"]
        )
    
    except Exception as e:
        logger.error("Error collecting demo ticker data", error=str(e))
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.post("/collect/latest")
async def collect_latest_data(tickers: List[str]):
    """
    Collect the latest available data for specified tickers.
    """
    logger.info("Latest data collection requested", ticker_count=len(tickers))
    
    try:
        if not tickers or len(tickers) > 20:
            raise HTTPException(status_code=400, detail="Invalid number of tickers (1-20 allowed)")
        
        # Clean ticker symbols
        cleaned_tickers = [t.upper().strip() for t in tickers if t.strip()]
        
        if not cleaned_tickers:
            raise HTTPException(status_code=400, detail="No valid tickers provided")
        
        # Collect latest data
        coord = get_coordinator()
        results = await coord.collect_latest_data(cleaned_tickers)
        
        return {
            "success": True,
            "message": f"Latest data collection completed for {results['tickers_processed']} tickers",
            "data": results
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error collecting latest data", error=str(e))
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/health/services")
async def validate_services():
    """
    Validate that all data collection services are working.
    """
    logger.info("Service validation requested")
    
    try:
        coord = get_coordinator()
        validation_results = await coord.validate_services()
        
        # Add Google Sheets validation
        validation_results["google_sheets_connection"] = await coord.validate_google_sheets_connection()
        
        all_services_ok = all(validation_results.values())
        
        return {
            "success": all_services_ok,
            "message": "All services operational" if all_services_ok else "Some services have issues",
            "data": validation_results
        }
    
    except Exception as e:
        logger.error("Error validating services", error=str(e))
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


async def _background_batch_collection(tickers: List[str], start_date: str, end_date: str):
    """
    Background task for batch data collection.
    """
    try:
        logger.info("Background batch collection started", 
                   ticker_count=len(tickers))
        
        coord = get_coordinator()
        job = await coord.collect_multiple_tickers(
            tickers=tickers,
            start_date=start_date,
            end_date=end_date
        )
        
        logger.info("Background batch collection completed", 
                   job_id=job.job_id, 
                   status=job.job_status)
    
    except Exception as e:
        logger.error("Error in background batch collection", error=str(e))