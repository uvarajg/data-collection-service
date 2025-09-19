# Data Collection Service 📊

[![Production Ready](https://img.shields.io/badge/Status-Production%20Ready-green)]()
[![Success Rate](https://img.shields.io/badge/Success%20Rate-99.9%25-brightgreen)]()
[![Coverage](https://img.shields.io/badge/US%20Market-2,077%20stocks-blue)]()
[![Storage](https://img.shields.io/badge/Compression-90%25%20savings-orange)]()

**The single source of truth for external data ingestion in the AlgoAlchemist trading platform.**

## 🎯 Mission

This microservice provides **99.9% reliable data collection** for the entire AlgoAlchemist ecosystem, processing **2,077 US market stocks** with complete automation and intelligent storage management.

## ⭐ Key Achievements

- **✅ 99.9% Collection Success Rate** (2,075/2,077 stocks)
- **✅ Complete US Market Coverage** (7,038 raw → 2,077 filtered stocks >$2B)
- **✅ Automated Daily Collection** via GitHub Actions at 8 PM EST
- **✅ 90% Storage Savings** through intelligent compression
- **✅ Zero Configuration Required** for US market data

## 🏗️ Architecture

### Data Sources
1. **GitHub Repositories** (Stock Discovery)
   - Raw data: 7,038 stocks from AMEX, NASDAQ, NYSE
   - Market cap and sector classification
   - Daily automated updates

2. **YFinance API** (Primary Enrichment)
   - Comprehensive OHLCV data
   - Fundamental metrics with fallback calculations
   - Technical indicators
   - 99.9% success rate with respectful rate limiting

### Data Flow
```
GitHub Raw Data → Filter >$2B → YFinance Enrichment → Validation → Storage → Compression
     7,038           2,077           2,075              97%        JSON      90% savings
```

## 🚀 Quick Start

### Prerequisites
```bash
# Python 3.11+
python -m venv venv
source venv/bin/activate  # Linux/Mac
pip install -r requirements.txt
```

### Environment Setup
```bash
cp .env.example .env
# Edit .env with your configuration
```

### Daily Collection (Recommended)
```bash
# Collect latest US market data
python scripts/main/collect_us_market_stocks.py

# Interactive collection for specific dates
python scripts/main/run_data_collection_with_dates.py
```

### Storage Management
```bash
# Archive old data (dry run first)
python scripts/main/archive_historical_data.py --cutoff-date 2025-09-14 --dry-run

# Execute archiving
python scripts/main/archive_historical_data.py --cutoff-date 2025-09-14 --yes

# Cleanup temporary files
python scripts/utils/compress_old_files.py
```

## 📁 Project Structure

```
data-collection-service/
├── src/                          # Core service code
│   ├── api/                      # FastAPI routes
│   ├── services/                 # Business logic
│   ├── models/                   # Data models
│   ├── utils/                    # Utilities
│   └── config/                   # Configuration
├── scripts/                      # Organized scripts
│   ├── main/                     # Core production scripts
│   ├── utils/                    # Utility scripts
│   ├── legacy/                   # Legacy scripts
│   └── tests/                    # Test scripts
├── .github/workflows/            # GitHub Actions
├── data/                        # Data storage (external)
└── docs/                        # Documentation
```

## 🎛️ Core Scripts

### Main Production Scripts

| Script | Purpose | Usage |
|--------|---------|-------|
| `collect_us_market_stocks.py` | Primary data collection | `python scripts/main/collect_us_market_stocks.py` |
| `run_data_collection_with_dates.py` | Interactive collection | `python scripts/main/run_data_collection_with_dates.py` |
| `archive_historical_data.py` | Data archiving | `python scripts/main/archive_historical_data.py --cutoff-date YYYY-MM-DD` |
| `daily_pipeline_automation.py` | Automated pipeline | Triggered by GitHub Actions |

### Utility Scripts

| Script | Purpose | Usage |
|--------|---------|-------|
| `compress_old_files.py` | Storage cleanup | `python scripts/utils/compress_old_files.py` |
| `monitor_pipeline.py` | Pipeline monitoring | `python scripts/utils/monitor_pipeline.py` |
| `monitor_live.py` | Live data monitoring | `python scripts/utils/monitor_live.py` |

## 📊 Performance Metrics

| Metric | Target | Current | Status |
|--------|--------|---------|--------|
| Collection Success Rate | >95% | 99.9% | ✅ |
| US Market Coverage | >2,000 stocks | 2,077 stocks | ✅ |
| Data Quality | >90% | 97% | ✅ |
| Storage Efficiency | >70% compression | 90% compression | ✅ |
| Automation | Daily | 8 PM EST via GitHub Actions | ✅ |

## 🔧 Configuration

### Environment Variables
```bash
# .env file
DATABASE_URL=postgresql://user:pass@localhost/db
REDIS_URL=redis://localhost:6379
ALPACA_API_KEY=your_key_here
ALPACA_SECRET_KEY=your_secret_here
EMAIL_SMTP_SERVER=smtp.gmail.com
EMAIL_FROM=your_email@domain.com
```

### GitHub Actions
Automated daily collection configured in `.github/workflows/daily_stock_collection.yml`:
- Runs daily at 8 PM EST
- Collects 2,077 US market stocks
- Compresses old files
- Sends email notifications

## 📈 Data Output

### Enriched Stock Data Format
```json
{
  "AAPL": {
    "basic_info": {
      "symbol": "AAPL",
      "company_name": "Apple Inc.",
      "market_cap": 2800000000000,
      "sector": "Technology"
    },
    "price_data": {
      "open": 150.00,
      "high": 155.00,
      "low": 149.00,
      "close": 154.00,
      "volume": 50000000
    },
    "fundamentals": {
      "pe_ratio": 25.5,
      "debt_to_equity": 1.8,
      "current_ratio": 1.1,
      "profit_margin": 0.25
    },
    "technical_indicators": {
      "sma_20": 152.00,
      "bb_upper": 158.00,
      "bb_lower": 146.00,
      "rsi": 55.5
    }
  }
}
```

## 📦 Data Storage

### File Organization
```
/workspaces/data/
├── input_source/                 # Raw and enriched data
│   ├── enriched_yfinance_YYYYMMDD_HHMMSS.json
│   └── raw_combined_YYYYMMDD_HHMMSS.json
├── historical/daily/             # Processed daily data
│   └── TICKER/YYYY/MM/YYYY-MM-DD.json
└── archives/historical/daily/    # Compressed archives
    └── TICKER/YYYY/TICKER_YYYY_MM.tar.gz
```

### Storage Management
- **Active Data**: JSON files in daily structure
- **Compressed Archives**: 90% space savings with tar.gz
- **Automated Cleanup**: Old files compressed after 7 days
- **Forever Retention**: Archives maintained indefinitely

## 🚨 Monitoring & Alerts

### Health Checks
- Collection success rate monitoring
- Data quality validation
- Storage space monitoring
- API rate limit tracking

### Email Notifications
- Daily collection summaries
- Error alerts and failures
- Storage cleanup reports
- Performance metric updates

## 🔄 CI/CD Pipeline

### GitHub Actions Workflow
1. **Daily Collection** (8 PM EST)
2. **Data Validation** (Completeness check)
3. **Storage Optimization** (Compression)
4. **Email Notifications** (Success/failure reports)
5. **Metric Updates** (Performance tracking)

## 🛠️ Development

### Adding New Data Sources
1. Create collector in `src/services/`
2. Add validation rules
3. Update configuration
4. Add tests
5. Update documentation

### Testing
```bash
# Run test suite
python -m pytest scripts/tests/

# Test specific functionality
python scripts/tests/test_yfinance_input.py
python scripts/tests/test_enriched_fundamentals.py
```

## 📚 Documentation

- **[CLAUDE.md](CLAUDE.md)** - AI Assistant instructions
- **[Scripts README](scripts/README.md)** - Script organization
- **[GitHub Actions Setup](GITHUB_ACTION_SETUP.md)** - CI/CD configuration
- **[Data Collection Prompts](DATA_COLLECTION_RUN_PROMPT.md)** - Usage guides

## 🤝 Contributing

1. Follow existing code patterns
2. Add tests for new functionality
3. Update documentation
4. Ensure 99.9% success rate maintained
5. Test with dry-run mode first

## 📞 Support

For issues and feature requests:
- Check existing documentation
- Review log files in `data/logs/`
- Test with sample data first
- Follow established patterns

---

**Built for reliability, optimized for scale, designed for the future of algorithmic trading.**