# Fresh Input Source Update Prompt

## Task Description

Run a fresh input source update to collect the latest US market stock data from GitHub repositories and enrich it with YFinance data, applying the >$2B market cap filter.

## Specific Requirements

1. **Data Collection Process**:
   - Download raw stock data from GitHub repositories (AMEX, NASDAQ, NYSE)
   - Extract and filter stocks with market cap > $2B
   - Enrich filtered stocks with comprehensive YFinance data
   - Create new timestamped enriched data file

2. **Expected Outputs**:
   - Raw combined data file: `raw_combined_YYYYMMDD_HHMMSS.json`
   - Enriched data file: `enriched_yfinance_YYYYMMDD_HHMMSS.json`
   - Job summary file: `input_source_data_job_summary_YYYYMMDD_HHMMSS.json`
   - Failed tickers log (if any): `failed_tickers_YYYYMMDD_HHMMSS.json`

3. **Success Criteria**:
   - Target: ~2,075-2,080 stocks successfully processed
   - Success rate: >99% (acceptable: 2-5 failed tickers out of 2,077)
   - Comprehensive data: 85+ fields per stock including fundamentals
   - Data location: `/workspaces/data/input_source/`

## Command to Execute

```bash
python collect_us_market_stocks.py
```

## Expected Process Flow

1. **Step 1**: Download from GitHub (~7,038 raw stocks)
   - AMEX: ~286 stocks
   - NASDAQ: ~4,020 stocks
   - NYSE: ~2,732 stocks

2. **Step 2**: Extract required fields from raw data

3. **Step 3**: Apply market cap filter (>$2B)
   - Mega Cap (>$200B): ~60 stocks
   - Large Cap ($10B-$200B): ~880 stocks
   - Mid Cap ($2B-$10B): ~1,137 stocks
   - **Total qualified**: ~2,077 stocks

4. **Step 4**: YFinance enrichment (parallel processing)
   - Process in batches of 50 stocks
   - Expected duration: 4-6 minutes
   - Target success rate: 99.9%

## Post-Collection Verification

After collection completes, verify:

```bash
# Check the new enriched file exists and has correct size
ls -la /workspaces/data/input_source/enriched_yfinance_$(date +%Y%m%d)*.json

# Verify stock count and sample data
python3 -c "
import json
data = json.load(open('/workspaces/data/input_source/enriched_yfinance_$(ls -t /workspaces/data/input_source/enriched_yfinance_*.json | head -1 | xargs basename)'))
print(f'📊 Total stocks: {len(data)}')
print(f'📈 Top stock: {data[0][\"ticker\"]} - Market Cap: \${data[0][\"market_cap\"]/1e9:.1f}B')
"
```

## Service Integration Impact

Once fresh data is collected:
- **EnrichedFundamentalsService** will automatically detect and use the new data
- **Data age** will be <1 hour (considered "fresh" vs 24-hour threshold)
- **API calls** will be eliminated for fundamental data retrieval
- **Response time** will be instant (cached data vs API delays)

## Troubleshooting

If collection fails or has low success rate:
1. Check network connectivity to GitHub and YFinance
2. Review failed tickers log for patterns
3. Verify YFinance API rate limiting isn't being exceeded
4. Check disk space in `/workspaces/data/input_source/`

## Expected Completion Message

```
================================================================================
✅ DATA COLLECTION COMPLETE!
================================================================================
📊 Summary:
   • Raw stocks downloaded: 7038
   • Stocks extracted: 7038
   • Stocks > $2B: 2077
   • Stocks enriched with YFinance: 2075
   • Total execution time: 0:04:XX

💾 Enriched data saved to: /workspaces/data/input_source/enriched_yfinance_YYYYMMDD_HHMMSS.json
```

## Automation Note

This process is also automated via GitHub Actions to run daily at 8 PM EST. This manual execution provides immediate fresh data when needed outside the scheduled runs.