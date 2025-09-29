# Polygon.io Collection Failure Resolution Guide

## Quick Start

If you encounter collection failures with Polygon.io data collection:

1. **Run the retry collection script**:
   ```bash
   python scripts/utils/failure_analysis/retry_failed_collections.py
   ```

2. **Check the results**: Success rate should be 95%+

3. **Review error logs**: Check `/workspaces/data/error_records/polygon_failures/retry_logs/`

## Common Failure Types and Solutions

### 1. Rate Limiting (HTTP 429)
**Solution**: Built into retry script with exponential backoff
```python
# Automatic retry with increasing delays
wait_time = 2 ** retry_count  # 1s, 2s, 4s
```

### 2. No Data Available
**Solution**: Check if ticker is recently listed
```bash
# Investigate specific ticker
python -c "
import requests
r = requests.get('https://api.polygon.io/v3/reference/tickers/TICKER',
                 params={'apikey': 'your_key'})
print(r.json())
"
```

### 3. Invalid Ticker Format
**Solution**: Clean ticker symbols
```python
ticker = ticker.strip()  # Remove trailing spaces
```

### 4. Original Collection Script Issues
**Root Causes**:
- No retry logic
- No rate limit handling
- Poor error categorization
- No pre-validation

**Solution**: Use the enhanced retry collection script which includes all these fixes.

## Success Metrics

Based on September 2025 investigation:
- **97.7% of failures are recoverable** with proper retry logic
- **Only 2.3% are true failures** (no data available)
- **99.4% overall success rate** achievable

## Process Improvements Implemented

### 1. Enhanced Error Handling
```python
failure_categories = {
    'RATE_LIMIT': 'API rate limit exceeded',
    'NO_DATA': 'No data available for date range',
    'INVALID_TICKER': 'Ticker not found or invalid',
    'TIMEOUT': 'Request timeout',
    'NETWORK_ERROR': 'Network or connection error',
    'API_ERROR': 'API returned error status',
    'DATA_QUALITY': 'Data quality validation failed',
    'ALREADY_COLLECTED': 'Data already exists',
    'BELOW_THRESHOLD': 'Market cap below threshold',
    'NON_US': 'Non-US company (now included)',
    'UNKNOWN': 'Unknown error'
}
```

### 2. Smart Retry Logic
```python
async def collect_with_retry(ticker, max_retries=3):
    for attempt in range(max_retries):
        try:
            return await collect_data(ticker)
        except RateLimitError:
            wait_time = 2 ** attempt
            await asyncio.sleep(wait_time)
        except Exception as e:
            if attempt == max_retries - 1:
                return failure_with_category(e)
```

### 3. Pre-Collection Validation
```python
# Check ticker validity before collection
is_valid, category, details = await validate_ticker(ticker)
if not is_valid:
    return skip_with_reason(category, details)
```

### 4. Batch Processing
```python
# Process in batches to prevent rate limiting
batch_size = 20
for batch in chunks(tickers, batch_size):
    await process_batch(batch)
    await asyncio.sleep(1)  # Rate limiting delay
```

## Monitoring and Alerting

### Success Rate Targets
- **Target**: 99%+ success rate
- **Alert Threshold**: <95% success rate
- **Recovery Target**: 95%+ for retry attempts

### Key Metrics to Track
- Collection success rate
- Retry success rate
- True failure count
- API rate limit hits
- Collection time per ticker

## Troubleshooting Guide

### Problem: High Failure Rate (>5%)
**Investigation Steps**:
1. Check API key validity
2. Verify network connectivity
3. Review rate limiting configuration
4. Check for API service issues

### Problem: Specific Ticker Always Fails
**Investigation Steps**:
1. Validate ticker exists: `/v3/reference/tickers/{ticker}`
2. Check delisting status
3. Verify data availability for date range
4. Test with smaller date range

### Problem: Rate Limiting Issues
**Solutions**:
1. Increase delays between requests
2. Reduce batch size
3. Consider API tier upgrade
4. Implement adaptive rate limiting

## API Best Practices

### 1. Authentication
```python
# Always use API key from environment
api_key = os.getenv('POLYGON_API_KEY')
params = {'apikey': api_key}
```

### 2. Request Optimization
```python
# Use appropriate parameters
params = {
    'apikey': api_key,
    'adjusted': 'true',  # Get adjusted prices
    'sort': 'asc',       # Chronological order
    'limit': 50000       # Maximum records
}
```

### 3. Error Handling
```python
async with session.get(url, params=params, timeout=30) as response:
    if response.status == 429:
        # Rate limit - retry with backoff
        await exponential_backoff(attempt)
    elif response.status >= 400:
        # API error - log and categorize
        log_api_error(response.status, await response.text())
    else:
        # Success - process data
        return await response.json()
```

## Recovery Procedures

### For Mass Failures (>100 tickers)
1. Run comprehensive investigation:
   ```bash
   python scripts/utils/failure_analysis/investigate_failed_tickers.py
   ```

2. Review categorization results

3. Run targeted retry:
   ```bash
   python scripts/utils/failure_analysis/retry_failed_collections.py
   ```

4. Monitor success rate and adjust parameters

### For Individual Ticker Failures
1. Check ticker validity manually
2. Test data availability for different date ranges
3. Add to manual review list if consistently failing

## Historical Context

### September 2025 Investigation Results
- **Total Investigated**: 480 failed tickers
- **Successfully Recovered**: 469 tickers (97.7%)
- **True Failures**: 11 tickers (2.3%)
- **Files Added**: 28,697 new data files
- **Success Rate Improvement**: 72.5% → 99.4%

### Key Learning
> **The data was always available - we just needed better collection logic!**

Most "failures" were due to insufficient retry logic, not actual data unavailability.

## Future Enhancements

### Planned Improvements
1. **Predictive Failure Detection**
   - Use historical patterns to predict failures
   - Proactive ticker validation

2. **Adaptive Rate Limiting**
   - Dynamic adjustment based on API response times
   - Real-time optimization

3. **Automated Recovery**
   - Automatic retry for any failures
   - Self-healing collection pipeline

4. **Enhanced Monitoring**
   - Real-time success rate dashboards
   - Automated alerts for degradation

---

**Document Version**: 1.0
**Last Updated**: September 29, 2025
**Status**: Production Ready ✅