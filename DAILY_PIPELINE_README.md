# Daily Data Pipeline Automation

> **Comprehensive automation script for daily data collection, validation, and reporting**

## 🎯 Overview

The Daily Pipeline Automation system executes a complete 7-step data processing workflow designed to run at 4 AM daily, collecting and validating the previous business day's market data.

## 📋 Pipeline Steps

### 1. **Input Data Refresh** (`collect_us_market_stocks.py`)
- Downloads latest US market data from GitHub repositories
- Filters stocks with market cap > $2B
- Enriches with YFinance data
- **Expected Output**: ~2,075 enriched stocks
- **Duration**: 4-6 minutes

### 2. **Input Data Report Generation**
- Analyzes refresh results
- Generates summary statistics
- **Output**: JSON report with stock counts and file listings

### 3. **Data Collection Service** (Previous business day)
- Collects OHLCV data for all enriched stocks
- Uses previous business day (Friday if run on Monday)
- **Expected Output**: ~1,280 stock data files
- **Duration**: 15-25 minutes

### 4. **Data Collection Report Generation**
- Analyzes collection success rates
- **Output**: JSON report with success metrics and job ID

### 5. **Data Validation Service**
- Validates collected data quality
- Checks completeness, technical indicators, fundamentals
- **Output**: Validation results and quality scores

### 6. **Validation Report Generation**
- Consolidates validation metrics
- **Output**: JSON report with quality assessments

### 7. **Email Reports**
- Sends all reports via email
- Includes detailed execution summary
- **Recipients**: Configured in `.env` file

## 🚀 Usage

### Manual Execution
```bash
# Option 1: Run the full pipeline
python daily_pipeline_automation.py

# Option 2: Use the shell wrapper (recommended)
./run_daily_pipeline.sh

# Option 3: Test the pipeline logic
python test_daily_pipeline.py
```

### Automated Execution (Cron)
```bash
# Edit crontab
crontab -e

# Add this line for 4 AM weekdays execution:
0 4 * * 1-5 /workspaces/data-collection-service/run_daily_pipeline.sh

# Or for immediate testing (every 5 minutes):
*/5 * * * * /workspaces/data-collection-service/run_daily_pipeline.sh
```

## 📊 Expected Output

### Console Output
```
================================================================================
🚀 DAILY PIPELINE AUTOMATION STARTED
================================================================================
📅 Run Date: 2025-09-17 04:00:00
📊 Target Data Date: 2025-09-16
================================================================================

============================================================
📊 STEP 1: REFRESHING INPUT DATA
============================================================
✅ Input data refresh completed successfully
📈 Raw stocks: 7038
📊 Filtered stocks: 2077
✨ Enriched stocks: 2075
⏱️  Duration: 285.4s

[... continuing through all 7 steps ...]

================================================================================
🏁 DAILY PIPELINE EXECUTION COMPLETE
================================================================================
📊 Steps completed: 7/7 (100.0%)
⏱️  Total execution time: 1847.2 seconds
📅 Target data date: 2025-09-16
✅ All steps completed successfully!
📁 Reports available in: /workspaces/data-collection-service/reports/
================================================================================
```

### Generated Files
```
/workspaces/data-collection-service/
├── reports/
│   ├── input_data_report_20250917_040523.json
│   ├── data_collection_report_20250917_041245.json
│   ├── validation_report_20250917_042118.json
│   └── email_report_20250917_042245.txt
├── logs/
│   └── daily_pipeline_20250917_040000.log
└── [Generated data files in /workspaces/data/...]
```

## ⚙️ Configuration

### Environment Variables (`.env`)
```bash
# Email Configuration
EMAIL_PROVIDER=gmail
EMAIL_USER=your_email@gmail.com
EMAIL_PASSWORD=your_app_password
EMAIL_FROM=your_email@gmail.com
EMAIL_TO=recipient@company.com

# Alpaca API (for data collection)
APCA_API_KEY_ID=your_alpaca_key
APCA_API_SECRET_KEY=your_alpaca_secret
APCA_API_BASE_URL=https://api.alpaca.markets
```

### Business Day Logic
- **Monday runs**: Collects Friday's data (skips weekend)
- **Tuesday-Friday runs**: Collects previous day's data
- **Weekend runs**: Collects Friday's data

## 📈 Success Criteria

### Overall Pipeline Success
- **Excellent**: 7/7 steps successful (100%)
- **Good**: 6/7 steps successful (≥85%)
- **Acceptable**: 5/7 steps successful (≥70%)
- **Failure**: <5/7 steps successful

### Individual Step Targets
- **Input Data Refresh**: >99% success rate, ~2,075 stocks
- **Data Collection**: >95% success rate, ~1,280 files
- **Data Validation**: >90% data completeness, >80% quality score

## 🔧 Troubleshooting

### Common Issues

#### 1. Input Data Refresh Fails
```bash
# Check GitHub connectivity
curl -I https://github.com/rreichel3/US-Stock-Symbols

# Check YFinance rate limiting
python -c "import yfinance as yf; print(yf.download('AAPL', period='1d'))"
```

#### 2. Data Collection Low Success Rate
```bash
# Check Alpaca API status
python -c "
import alpaca_trade_api as api
client = api.REST()
print(client.get_account())
"

# Check disk space
df -h /workspaces/data
```

#### 3. Email Sending Fails
```bash
# Test SMTP connection
python -c "
import smtplib
server = smtplib.SMTP('smtp.gmail.com', 587)
server.starttls()
# Use app password, not regular password
server.login('your_email@gmail.com', 'your_app_password')
print('SMTP connection successful')
server.quit()
"
```

#### 4. Data Validation Issues
```bash
# Check if data files exist for target date
find /workspaces/data/historical/daily -name "2025-09-16.json" | wc -l

# Sample data file content
python -c "
import json
with open('/workspaces/data/historical/daily/AAPL/2025/09/2025-09-16.json') as f:
    data = json.load(f)
    print(f'Keys: {list(data.keys())}')
    print(f'Date: {data.get(\"date\")}')
"
```

## 📝 Monitoring

### Log Files
- **Location**: `/workspaces/data-collection-service/logs/`
- **Retention**: Last 30 executions
- **Format**: `daily_pipeline_YYYYMMDD_HHMMSS.log`

### Report Files
- **Location**: `/workspaces/data-collection-service/reports/`
- **Types**: Input data, collection, validation, email reports
- **Format**: JSON with detailed metrics

### Email Notifications
- **Success**: Comprehensive summary with all metrics
- **Failure**: Error details and troubleshooting hints
- **Attachments**: All generated report files

## 🔄 Maintenance

### Regular Tasks
1. **Weekly**: Review log files for patterns
2. **Monthly**: Clean old report files
3. **Quarterly**: Update API credentials if needed

### Monitoring Commands
```bash
# Check recent pipeline runs
ls -la logs/daily_pipeline_*.log | tail -5

# Check recent success rates
grep -h "Steps completed" logs/daily_pipeline_*.log | tail -10

# Check disk usage
du -sh /workspaces/data/historical/daily/
```

## 🚨 Emergency Procedures

### Manual Recovery
If the pipeline fails, you can run individual steps:

```bash
# 1. Manual input refresh
python collect_us_market_stocks.py

# 2. Manual data collection (interactive)
python run_data_collection_with_dates.py

# 3. Manual validation
cd /workspaces/data-validation-service
python run_validation_latest_data.py
```

### Data Integrity Check
```bash
# Count collected files for a specific date
find /workspaces/data/historical/daily -name "2025-09-16.json" | wc -l

# Expected: ~1,280 files (matching enriched stock count)
```

## 📧 Email Setup (Gmail)

### Generate App Password
1. Go to Google Account settings
2. Security → 2-Step Verification
3. App passwords → Generate new password
4. Use the 16-character password in `.env`

### Update `.env`
```bash
EMAIL_USER=your_email@gmail.com
EMAIL_PASSWORD=abcd-efgh-ijkl-mnop  # 16-character app password
EMAIL_TO=recipient@company.com
```

## 🎯 Integration

This pipeline integrates with:
- **Data Collection Service**: Primary data ingestion
- **Data Validation Service**: Quality control
- **AlgoAlchemist Platform**: Downstream ML and trading services
- **GitHub Actions**: Alternative automated execution
- **Monitoring Systems**: Log aggregation and alerting

For questions or issues, check the logs first, then review the individual service documentation in their respective directories.