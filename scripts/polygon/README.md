# Polygon.io Data Collection Scripts

This directory contains scripts for collecting historical market data from Polygon.io API.

## 📁 Directory Structure

```
scripts/polygon/
├── README.md                           # This file
├── historical_input_data_polygon.py    # Main collection script for 5 years of data
└── enhanced_polygon_fundamentals.py    # Enhanced fundamental data collection

tests/polygon/
├── test_polygon_collection.py          # Test script with limited tickers
└── test_legacy_financials.py          # Test for legacy financial endpoints

docs/polygon/
├── POLYGON_SETUP.md                   # Complete setup and usage guide
└── requirements_polygon.txt           # Python dependencies
```

## 🚀 Quick Start

### 1. Install Dependencies
```bash
cd /workspaces/data-collection-service
pip install -r docs/polygon/requirements_polygon.txt
```

### 2. Set Environment Variable
```bash
# The API key should already be in .env file
export POLYGON_API_KEY="your_api_key_here"
# Or source the environment file
source /workspaces/data-collection-service/.env
```

### 3. Run Test Collection (Recommended First)
```bash
# Test with 5 tickers, last 30 days
python tests/polygon/test_polygon_collection.py
```

### 4. Run Full Historical Collection
```bash
# Collect 5 years for all US stocks (8-12 hours)
python scripts/polygon/historical_input_data_polygon.py
```

## 📊 Data Output

Data is stored in hierarchical JSON format:
```
/workspaces/data/raw_data/polygon/
├── {ticker}/
│   └── {year}/
│       └── {month}/
│           └── {date}.json
├── error_records/
├── collection.log
└── collection_summary.json
```

## 🔧 Key Features

- **5 Years Historical Data**: Complete OHLCV data from 2020-2025
- **Technical Indicators**: RSI, MACD, SMA, EMA, Bollinger Bands, ATR, Volatility
- **Fundamental Data**: Market cap, P/E ratio, financial ratios
- **Market Cap Filter**: Only collects stocks > $2B market cap
- **Rate Limiting**: Respects Polygon.io API limits (5 req/min for Starter)
- **Error Handling**: Comprehensive retry logic and error documentation
- **Progress Tracking**: Real-time progress updates and logging

## ⚠️ Important Notes

1. **API Key Required**: Must have Polygon.io Stocks Starter plan (paid)
2. **Runtime**: Full collection takes 8-12 hours due to rate limiting
3. **Storage**: Requires ~50-100 GB for complete dataset
4. **Compatibility**: Data format compatible with AlgoAlchemist validation service

## 📚 Documentation

For complete setup and usage instructions, see:
- [POLYGON_SETUP.md](/workspaces/data-collection-service/docs/polygon/POLYGON_SETUP.md)

## 🧪 Testing

Test scripts available:
- `test_polygon_collection.py`: Limited test with 5 tickers
- `test_legacy_financials.py`: Test financial data endpoints

## 📈 Integration

This data integrates with:
- AlgoAlchemist Data Validation Service
- ML Training Pipeline
- Trading Decision Engine

---
*Part of AlgoAlchemist Data Collection Service*
*Last Updated: September 2025*