# Current Sprint - Data Collection Service

## Sprint Goal
Implement comprehensive data collection service replicating AlgoAlchemist patterns in Python with FastAPI architecture.

## Current Status
- [x] Create repository structure
- [x] Set up Claude instructions and context
- [x] Analyze AlgoAlchemist codebase patterns
- [x] Define technical implementation plan
- [ ] Implement core data collection service

## Detailed Implementation Plan

### Phase 1: Core Infrastructure (Priority 1)
1. **FastAPI Application Setup**
   - Create `src/main.py` with FastAPI app
   - Add health check endpoint `/health`
   - Configure CORS and middleware
   - Set up structured logging

2. **Environment & Configuration**
   - Copy and adapt `.env` from AlgoAlchemist
   - Create `src/config.py` for settings management
   - Add validation for required API keys

3. **Data Models (Python Dataclasses)**
   ```python
   @dataclass
   class StockDataRecord:
       record_id: str  # UUID
       ticker: str
       date: str  # YYYY-MM-DD
       open: float
       high: float
       low: float
       close: float
       volume: int
       technical: TechnicalIndicators
       fundamental: Optional[FundamentalData]
       metadata: RecordMetadata
   ```

### Phase 2: Alpaca Integration (Priority 1)
1. **Alpaca Service (`src/services/alpaca_service.py`)**
   - Replicate `alpaca-service.ts` functionality
   - Use `alpaca-trade-api` Python library
   - Implement rate limiting (same as AlgoAlchemist)
   - Key endpoints to implement:
     ```python
     # OHLCV Data Collection
     client.get_bars(
         symbol=ticker,
         timeframe=TimeFrame.Day,
         start=start_date,
         end=end_date,
         adjustment='raw'
     )
     ```

2. **Technical Indicators (`src/services/technical_indicators.py`)**
   - Use `talib` library (same as AlgoAlchemist)
   - Calculate: RSI(14), MACD, SMA(50,200), EMA(12,26), Bollinger Bands, ATR(14)
   - Match exact calculations from `technicalindicators` npm package

### Phase 3: Data Storage (Priority 1)
1. **File Storage Service (`src/services/storage_service.py`)**
   - Replicate directory structure: `/workspaces/data/historical/daily/{TICKER}/{YYYY}/{MM}/{YYYY-MM-DD}.json`
   - UUID-based record tracking
   - Atomic file writes with error handling
   - Daily data aggregation and compression

2. **Data Collection Coordinator (`src/services/data_collector.py`)**
   - Sequential ticker processing (avoid parallel API calls)
   - Rate limiting: 100ms between API calls
   - Error handling without fallbacks (explicit error records)
   - Job metadata tracking with UUIDs

### Phase 4: API Endpoints (Priority 2)
1. **Data Collection Endpoints**
   ```python
   POST /api/v1/collect/daily/{ticker}  # Single ticker
   POST /api/v1/collect/batch          # Batch collection
   GET  /api/v1/data/{ticker}/{date}   # Retrieve data
   GET  /api/v1/jobs/{job_id}/status   # Job tracking
   ```

2. **Monitoring Endpoints**
   ```python
   GET /api/v1/health                  # Health check
   GET /api/v1/metrics                 # Collection metrics
   GET /api/v1/cache/stats             # Cache statistics
   ```

### Phase 5: Advanced Features (Priority 3)
1. **Google Sheets Integration**
   - Replicate ticker list management from AlgoAlchemist
   - Use same Google Sheets API patterns
   - Service account authentication

2. **YFinance Fundamentals**
   - Port `yfinance-fundamentals-service.ts` to Python
   - Maintain same caching strategy (4 hours)
   - Same fundamental metrics collection

## Technical Specifications

### Data Points to Collect (Match AlgoAlchemist)
1. **OHLCV Data (Alpaca getBarsV2)**
   - Open, High, Low, Close, Volume
   - Daily timeframe
   - Raw adjustment (no splits/dividends)

2. **Technical Indicators**
   - RSI(14): Relative Strength Index
   - MACD: Moving Average Convergence Divergence
   - SMA(50, 200): Simple Moving Averages
   - EMA(12, 26): Exponential Moving Averages
   - Bollinger Bands: Upper, Lower, Middle
   - ATR(14): Average True Range
   - Volatility calculation

3. **Fundamental Data (YFinance)**
   - Market cap, P/E ratio, Debt-to-equity
   - ROE%, Current ratio, Operating margin%
   - Revenue growth%, Profit margin%
   - Dividend yield%, Book value

### Storage Structure
```
/workspaces/data/
├── historical/
│   └── daily/
│       └── {TICKER}/
│           └── {YYYY}/
│               └── {MM}/
│                   └── {YYYY-MM-DD}.json
├── compressed/
│   └── {TICKER}/
│       └── {YYYY}/
│           └── {MM}.gz
├── cache/
│   ├── alpaca/
│   ├── yfinance/
│   └── technical_indicators/
└── jobs/
    └── {job_id}/
        ├── metadata.json
        └── results.json
```

### Rate Limiting Strategy
- Alpaca API: 200 requests/minute (default)
- YFinance API: 100ms delay between calls
- Google Sheets API: Respect quotas
- Redis caching for repeated requests

### Error Handling
- **No intelligent fallbacks** - mark errors explicitly
- Log all API failures with context
- Continue processing other tickers on individual failures
- Maintain data integrity over completeness

## Implementation Order
1. FastAPI app with health endpoint
2. Alpaca service with OHLCV collection
3. Technical indicators calculation
4. File storage service
5. Data collection coordinator
6. Job tracking and metadata
7. YFinance fundamentals integration
8. Google Sheets ticker management
9. API endpoints and monitoring
10. Caching and optimization

## Success Criteria
- Collect same data as AlgoAlchemist current system
- Maintain same file structure and naming
- Achieve same or better collection reliability
- Support same ticker universe (12+ stocks)
- Provide REST API for other services
- Independent operation without AlgoAlchemist dependencies

## Dependencies & Prerequisites
- Python 3.9+
- Alpaca API credentials (live recommended)
- Google Sheets API service account
- YFinance (no API key required)
- Redis for caching (optional but recommended)

## Key Libraries
- `fastapi`: Web framework
- `alpaca-trade-api`: Alpaca integration
- `yfinance`: Yahoo Finance data
- `talib`: Technical analysis
- `redis`: Caching layer
- `google-api-python-client`: Google Sheets
- `pydantic`: Data validation
- `aiofiles`: Async file operations

## Next Actions for Claude Code
1. Start with Phase 1: Set up FastAPI application
2. Implement Alpaca service following exact patterns from AlgoAlchemist
3. Create storage service with UUID tracking
4. Add technical indicators calculation
5. Test with single ticker before batch processing

## Notes
- **Critical**: Maintain exact data compatibility with AlgoAlchemist
- **Critical**: No intelligent fallbacks - explicit error handling only
- **Critical**: Use same rate limiting and caching strategies
- Focus on reliability and data integrity over speed
- Use async operations for better performance
- Implement comprehensive logging for troubleshooting

Last Updated: 2024-12-19