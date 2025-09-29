# Failure Analysis Tools

## Overview

This directory contains comprehensive tools for analyzing and resolving data collection failures in the Polygon.io service. These tools were developed in September 2025 to investigate 480 "failed" ticker collections and achieved a **97.7% recovery rate**.

## Key Scripts

### 1. 🌟 `retry_failed_collections.py` - PRIMARY TOOL

**Purpose**: Comprehensive retry collection with enhanced validation and error handling.

**Features**:
- Pre-collection ticker validation
- Smart retry logic with exponential backoff (2^attempt seconds)
- Batch processing (20 tickers per batch)
- Enhanced error logging with 11 detailed categories
- Rate limiting protection
- Non-US company inclusion (trading on US exchanges)

**Requirements**:
- POLYGON_API_KEY environment variable (loaded from .env file)
- YFinance enriched data for fundamentals

**Usage**:
```bash
python scripts/utils/failure_analysis/retry_failed_collections.py
```

**Results**: Successfully recovered 469 of 480 failed tickers (97.7% success rate)

### 2. `investigate_failed_tickers.py`

**Purpose**: Detailed API investigation to categorize failure reasons.

**Features**:
- Comprehensive ticker validation with Polygon API
- Categorizes failures into 12 specific categories
- Checks ticker status, market type, and data availability
- Generates detailed investigation reports

**Requirements**:
- POLYGON_API_KEY environment variable
- python-dotenv for .env file loading

**Usage**:
```bash
python scripts/utils/failure_analysis/investigate_failed_tickers.py
```

### 3. `analyze_failures_without_api.py`

**Purpose**: Market cap and metadata-based failure analysis (no API required).

**Features**:
- Categorizes based on available YFinance data
- Identifies market cap thresholds
- Detects non-US companies
- Works without API access

**Usage**:
```bash
python scripts/utils/failure_analysis/analyze_failures_without_api.py
```

### 4. Legacy Analysis Scripts

- `analyze_polygon_failures.py` - Original failure analyzer
- `enhanced_failure_analyzer.py` - Enhanced version with detailed categorization
- `failure_summary_generator.py` - Summary report generator
- `final_failure_report.py` - Comprehensive final report

## Failure Categories

### Recoverable (97.7%)
- **Data Available**: 469 tickers successfully collected with retry logic
- **Root Cause**: Insufficient retry logic in original collection script

### True Failures (2.3%)
- **NO_DATA** (10 tickers): Recently listed/delisted, no historical data available
- **INVALID_TICKER** (1 ticker): Format issues (e.g., trailing spaces)

## Key Findings

### 1. Original Collection Issues
- No retry logic for rate limiting (429 errors)
- No exponential backoff
- Poor error categorization
- No pre-collection validation

### 2. Data Availability
- 97.7% of "failed" tickers had data available
- Only 2.3% were true failures
- Non-US companies on US exchanges should be included

### 3. Success Metrics Achieved
- **Before**: 72.5% success rate (1,267/1,747 tickers)
- **After**: 99.4% success rate (1,736/1,747 tickers)
- **Files Added**: 28,697 new files (+30.3%)
- **Market Cap Coverage**: +$400B in market capitalization

## Implementation Pattern

The enhanced collection pattern for future use:

```python
async def collect_with_retry(ticker, max_retries=3):
    # 1. Pre-validate ticker
    is_valid, category, details = await validate_ticker(session, ticker)
    if not is_valid:
        log_skip(ticker, category, details)
        return {'status': 'skipped', 'category': category}

    # 2. Attempt collection with retry
    for attempt in range(max_retries):
        try:
            data = await collect_data(session, ticker, start_date, end_date)
            if attempt > 0:
                log_retry_success(ticker, attempt)
            return {'status': 'success', 'data': data}

        except RateLimitError:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                await asyncio.sleep(wait_time)
                continue
            else:
                return {'status': 'failed', 'category': 'RATE_LIMIT'}

        except Exception as e:
            log_detailed_error(ticker, e)
            return {'status': 'failed', 'category': categorize_error(e)}

    return {'status': 'failed', 'category': 'MAX_RETRIES_EXCEEDED'}
```

## Report Outputs

### Generated Reports Location
`/workspaces/data/error_records/polygon_failures/`

### Key Reports
- `FINAL_INVESTIGATION_REPORT.md` - Comprehensive findings and statistics
- `FINAL_SUMMARY.md` - Executive summary
- `retry_logs/retry_collection_report_*.json` - Detailed retry results
- `investigation_results/complete_investigation_*.json` - API investigation results

### Summary Statistics
```json
{
  "total_attempted": 480,
  "successful_collections": 469,
  "failed_collections": 11,
  "success_rate": "97.7%",
  "files_added": 28697,
  "market_cap_recovered": "$400B+"
}
```

## Best Practices

### 1. Always Use Retry Logic
- Implement exponential backoff
- Maximum 3 retries per ticker
- Handle rate limiting gracefully

### 2. Pre-validate Tickers
- Check ticker existence before collection
- Validate market type (stocks)
- Check active status

### 3. Detailed Error Logging
- Categorize every failure type
- Include timestamp and context
- Save detailed error logs for debugging

### 4. Batch Processing
- Process 20 tickers per batch
- Add delays between batches (1 second)
- Monitor success rate in real-time

### 5. Include International Coverage
- Don't filter by country for US exchanges
- Include non-US companies trading on NYSE/NASDAQ
- Major international companies provide significant market coverage

## Dependencies

```python
# Required packages
import os
import json
import asyncio
import aiohttp
import logging
from datetime import datetime
from collections import defaultdict
from dotenv import load_dotenv  # For .env file loading
```

## Environment Setup

```bash
# Required environment variable
POLYGON_API_KEY=your_polygon_api_key_here

# Load from .env file
load_dotenv('/workspaces/data-collection-service/.env')
```

## Success Metrics

### Targets vs Achieved
- **Target Success Rate**: 85% → **Achieved**: 99.4% ✅
- **Target Recovery Rate**: 50% → **Achieved**: 97.7% ✅
- **Target File Count**: 100,000 → **Achieved**: 123,344 ✅
- **Target Coverage**: 1,500 tickers → **Achieved**: 1,736 ✅

---

**Last Updated**: September 29, 2025
**Investigation Period**: June 1 - September 1, 2025
**Status**: Production Ready ✅