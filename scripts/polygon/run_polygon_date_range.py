#!/usr/bin/env python3
"""
Run Polygon.io collection for a specific date range.
Wrapper script for historical_input_data_polygon.py with custom date parameters.
"""

import asyncio
import sys
import os
from datetime import datetime
from pathlib import Path

# Add the parent directory to Python path to import the main script modules
sys.path.insert(0, str(Path(__file__).parent))

# Import from the main polygon script
from historical_input_data_polygon import (
    PolygonConfig, PolygonDataCollector, get_all_us_stocks,
    logger, load_dotenv
)

# Load environment
load_dotenv('/workspaces/data-collection-service/.env')


async def collect_date_range(start_date_str: str, end_date_str: str):
    """
    Collect data for a specific date range for all US stocks.

    Args:
        start_date_str: Start date in YYYY-MM-DD format
        end_date_str: End date in YYYY-MM-DD format
    """
    # Validate dates
    try:
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
    except ValueError as e:
        logger.error(f"Invalid date format: {e}. Use YYYY-MM-DD")
        sys.exit(1)

    if start_date > end_date:
        logger.error("Start date must be before or equal to end date")
        sys.exit(1)

    logger.info(f"Date range: {start_date} to {end_date}")

    # Configuration
    api_key = os.getenv('POLYGON_API_KEY')
    if not api_key:
        logger.error("POLYGON_API_KEY environment variable not set")
        sys.exit(1)

    config = PolygonConfig(api_key=api_key)

    # Get enriched tickers from YFinance input data (same as main service)
    input_source_path = Path("/workspaces/data/input_source")
    enriched_files = sorted(input_source_path.glob("enriched_yfinance_*.json"), reverse=True)

    if not enriched_files:
        logger.error("No enriched YFinance data found. Run collect_us_market_stocks.py first.")
        sys.exit(1)

    import json
    with open(enriched_files[0], 'r') as f:
        enriched_data = json.load(f)

    tickers = [stock['ticker'] for stock in enriched_data]
    logger.info(f"Using {len(tickers)} tickers from {enriched_files[0].name}")

    # Create collector and process tickers
    async with PolygonDataCollector(config) as collector:
        successful = 0
        failed = 0

        # Override the date calculation in the collector
        original_method = collector.collect_historical_data

        async def custom_collect(ticker: str, min_market_cap: float = 2_000_000_000) -> bool:
            """Custom collect method with fixed date range"""
            try:
                # Check market cap first
                logger.info(f"Checking market cap for {ticker}...")
                financials_data = await collector._get_financials_data(ticker)

                market_cap = None
                if financials_data and 'marketCapitalization' in financials_data:
                    market_cap = financials_data['marketCapitalization']

                if market_cap is None:
                    logger.warning(f"No market cap data available for {ticker}, skipping")
                    await collector._log_error(ticker, "market_cap_missing", "No market cap data available")
                    return False

                if market_cap < min_market_cap:
                    logger.info(f"❌ Skipping {ticker}: Market cap ${market_cap:,.0f} < ${min_market_cap:,.0f}")
                    return False

                logger.info(f"✅ {ticker} passes market cap filter: ${market_cap:,.0f}")

                # Use custom date range
                logger.info(f"Collecting data for {ticker}: {start_date} to {end_date}")

                # Get OHLCV data for the specified range
                ohlcv_data = await collector._get_ohlcv_data(ticker, start_date, end_date)
                if not ohlcv_data:
                    await collector._log_error(ticker, "ohlcv_collection", "No OHLCV data received")
                    return False

                # Continue with rest of processing (same as original)
                from historical_input_data_polygon import StockDataPoint, TechnicalIndicatorsCalculator
                import time

                records = []
                for daily_data in ohlcv_data:
                    record = StockDataPoint(
                        record_id=f"{ticker}_{daily_data['t']}_{int(time.time())}",
                        ticker=ticker,
                        date=daily_data['t'],
                        open=daily_data['o'],
                        high=daily_data['h'],
                        low=daily_data['l'],
                        close=daily_data['c'],
                        volume=daily_data['v'],
                        adjusted_close=daily_data.get('c', daily_data['c']),
                        collection_timestamp=datetime.now().isoformat()
                    )
                    records.append(record)

                logger.info(f"Collected {len(records)} OHLCV records for {ticker}")

                # Calculate technical indicators
                if len(records) >= 50:
                    calc = TechnicalIndicatorsCalculator()
                    records = calc.calculate_indicators(records)
                    logger.info(f"Technical indicators calculated for {ticker}")
                else:
                    logger.warning(f"Insufficient data for technical indicators: {len(records)} records")

                # Add fundamental data
                if financials_data:
                    for record in records:
                        record.market_cap = market_cap
                        # Add other fundamentals as available

                # Save records
                await collector._save_records(ticker, records)
                logger.info(f"✅ Successfully processed {ticker}")
                return True

            except Exception as e:
                logger.error(f"Error processing {ticker}: {str(e)}")
                await collector._log_error(ticker, "processing_error", str(e))
                return False

        # Process in batches
        batch_size = 10
        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i + batch_size]

            logger.info(f"Processing batch {i//batch_size + 1}/{(len(tickers) + batch_size - 1)//batch_size}")

            tasks = [custom_collect(ticker) for ticker in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, Exception):
                    failed += 1
                    logger.error(f"Batch processing exception: {result}")
                elif result:
                    successful += 1
                else:
                    failed += 1

            await asyncio.sleep(15)

            total_processed = successful + failed
            logger.info(f"Progress: {total_processed}/{len(tickers)} ({successful} successful, {failed} failed)")

    logger.info(f"Collection completed: {successful} successful, {failed} failed out of {len(tickers)} tickers")

    # Generate summary
    import json
    summary = {
        "collection_date": datetime.now().isoformat(),
        "date_range": {
            "start": start_date_str,
            "end": end_date_str
        },
        "total_tickers": len(tickers),
        "successful_collections": successful,
        "failed_collections": failed,
        "success_rate": (successful / len(tickers)) * 100 if tickers else 0,
        "data_source": "polygon.io",
        "storage_path": "/workspaces/data/raw_data/polygon"
    }

    summary_path = Path("/workspaces/data/raw_data/polygon") / f"collection_summary_{start_date_str}_to_{end_date_str}.json"
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)

    logger.info(f"Collection summary saved to: {summary_path}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python run_polygon_date_range.py START_DATE END_DATE")
        print("Example: python run_polygon_date_range.py 2025-09-01 2025-09-14")
        sys.exit(1)

    start_date = sys.argv[1]
    end_date = sys.argv[2]

    asyncio.run(collect_date_range(start_date, end_date))