# GitHub Action Setup for Daily Stock Data Collection

## Overview
This GitHub Action runs daily at 8 PM EST to collect US market stock data from GitHub repositories and enrich it with YFinance data.

## Setup Instructions

### 1. Configure GitHub Secrets
Go to your repository Settings → Secrets and variables → Actions, and add these secrets:

```
EMAIL_USER        # Gmail address (from .env)
EMAIL_PASSWORD    # Gmail app password (from .env)
EMAIL_TO          # Recipient email (from .env)
EMAIL_FROM        # Sender email (from .env)
```

Based on your .env file:
- `EMAIL_USER`: pmjgd.uvapps@gmail.com
- `EMAIL_PASSWORD`: izrsmgauejxjpbnd
- `EMAIL_TO`: uvaraj@gosnowballinvesting.com
- `EMAIL_FROM`: pmjgd.uvapps@gmail.com

### 2. Enable GitHub Actions
1. Go to Settings → Actions → General
2. Under "Actions permissions", select "Allow all actions and reusable workflows"
3. Click Save

### 3. Test the Workflow
You can manually trigger the workflow to test:
1. Go to Actions tab
2. Select "Daily Stock Data Collection"
3. Click "Run workflow"
4. Select branch and click "Run workflow"

## Workflow Details

### Schedule
- **Daily Run**: 8 PM EST (1 AM UTC)
- **Manual Trigger**: Available anytime via workflow_dispatch

### Data Collection Process
1. **Step 1**: Downloads raw stock data from GitHub (Set A)
   - AMEX: ~286 stocks
   - NASDAQ: ~4,020 stocks
   - NYSE: ~2,732 stocks
   - Total: ~7,038 stocks

2. **Step 2**: Extracts required fields (Set B)
   - ticker, market cap, country, industry, sector

3. **Step 3**: Filters stocks with market cap > $2B (Set C)
   - Typically ~2,077 stocks

4. **Step 4**: Queries YFinance for comprehensive data (Set D)
   - ~98% success rate
   - Collects 50+ data points per stock

### Output Files
All data is saved to `/data/input_source/`:
- `set_a_raw_*.json` - Raw data from GitHub
- `set_b_extracted_*.json` - Extracted fields
- `set_c_filtered_2b_*.json` - Filtered stocks > $2B
- `set_d_enriched_yfinance_*.json` - Final enriched data

### Email Notifications
The workflow sends email notifications:
- **Success**: Summary with stock count and file details
- **Failure**: Error details and workflow link

### Data Retention
- Artifacts are kept for 7 days
- Old data files are automatically cleaned up after 7 days

## Monitoring

### Check Workflow Status
1. Go to the Actions tab in your repository
2. Click on "Daily Stock Data Collection"
3. View run history and logs

### Download Data Artifacts
1. Click on a specific workflow run
2. Scroll to "Artifacts" section
3. Download the `stock-data-{run-id}` artifact

## Troubleshooting

### Common Issues

1. **Email not sending**
   - Verify GitHub secrets are set correctly
   - Check Gmail app password is valid
   - Ensure "Less secure app access" is enabled (if needed)

2. **YFinance rate limiting**
   - The workflow includes delays between batches
   - If persistent, reduce batch size in the script

3. **Workflow timeout**
   - Current timeout: 120 minutes
   - Can be increased if needed in the workflow file

### Manual Data Collection
If the automated workflow fails, you can run manually:
```bash
python collect_us_market_stocks.py
```

## Cost Considerations
- GitHub Actions provides 2,000 free minutes/month for private repos
- Each run takes ~30-60 minutes
- Daily runs = ~900-1,800 minutes/month
- Stay within free tier with careful monitoring

## Integration with Data Collection Service
The collected data (Set D) serves as the input source for the Data Collection Service, replacing the Google Sheets dependency.

To use in the service:
1. Load the latest `set_d_enriched_yfinance_*.json`
2. Use the ticker list for data collection
3. Pre-populated market cap and fundamental data available

## Future Improvements
- [ ] Incremental updates (only new/changed stocks)
- [ ] Parallel processing for faster collection
- [ ] Store data in cloud storage (S3/GCS)
- [ ] Add data quality checks
- [ ] Implement retry logic for failed tickers