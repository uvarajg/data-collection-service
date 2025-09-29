# Data Enhancement Utilities

## Generic Data Merger

This utility safely merges any new data points into existing datasets without corrupting existing data or affecting quality/validation metrics. It supports adding new technical indicators, additional fundamental data, company classification data, and any custom data enhancements.

### Features

- **Non-destructive merge**: Preserves all existing data
- **Version tracking**: Each merge operation gets a unique version for debugging
- **Automatic backups**: Creates backups before any modifications
- **Data integrity validation**: Ensures no corruption occurs
- **Batch processing**: Efficient API usage with rate limiting
- **Progress tracking**: Real-time statistics and logging

### Safety Measures

1. **Backup Creation**: Automatic backup of original files before modification
2. **Integrity Validation**: SHA-256 hashing and data structure validation
3. **Rollback Capability**: Automatic restoration if validation fails
4. **Non-destructive Operations**: Only adds new fields, never modifies existing data
5. **Version Tracking**: Complete metadata trail for debugging

### Usage

#### Basic Usage
```bash
# Add company data only
python scripts/utils/data_enhancement/generic_data_merger.py /workspaces/data/historical/daily --providers company

# Add custom technical indicators only
python scripts/utils/data_enhancement/generic_data_merger.py /workspaces/data/historical/daily --providers technical

# Add additional fundamental data only
python scripts/utils/data_enhancement/generic_data_merger.py /workspaces/data/historical/daily --providers fundamental

# Add all available enhancements
python scripts/utils/data_enhancement/generic_data_merger.py /workspaces/data/historical/daily --providers all
```

#### Advanced Options
```bash
# Dry run to see what would be processed
python scripts/utils/data_enhancement/generic_data_merger.py /workspaces/data/historical/daily --dry-run --providers all

# Multiple specific providers
python scripts/utils/data_enhancement/generic_data_merger.py /workspaces/data/historical/daily \
  --providers company technical \
  --batch-size 100

# Process specific date range with company data
python scripts/utils/data_enhancement/generic_data_merger.py /workspaces/data/historical/daily \
  --pattern "**/2025/09/2025-09-*.json" \
  --providers company

# Legacy company data merger (backward compatibility)
python scripts/utils/data_enhancement/merge_company_data.py /workspaces/data/historical/daily
```

### Enhancement Providers

#### 1. Company Data Provider (`--providers company`)
- **Source**: Polygon.io API
- **Adds**: Sector, industry, primary exchange, CIK
- **Target Section**: `company_data`
- **Requirements**: POLYGON_API_KEY environment variable

#### 2. Custom Technical Indicators (`--providers technical`)
- **Source**: Calculated from existing price data
- **Adds**: Bollinger Band bandwidth, price position, custom indicators
- **Target Section**: `technical_indicators`
- **Requirements**: Existing basic_data in files

#### 3. Additional Fundamentals (`--providers fundamental`)
- **Source**: Calculated from existing fundamental data
- **Adds**: Enterprise value, price-to-book ratio, custom ratios
- **Target Section**: `fundamental_data`
- **Requirements**: Existing fundamental_data in files

#### 4. Custom Providers
You can easily extend the system by creating new providers that implement the `DataEnhancementProvider` interface.

### Data Structure Changes

The script adds a new `company_data` section to existing files:

#### Before Enhancement
```json
{
  "record_id": "AAPL_2025-09-10_1234567890",
  "ticker": "AAPL",
  "date": "2025-09-10",
  "basic_data": { ... },
  "technical_indicators": { ... },
  "fundamental_data": { ... },
  "metadata": { ... }
}
```

#### After Enhancement
```json
{
  "record_id": "AAPL_2025-09-10_1234567890",
  "ticker": "AAPL",
  "date": "2025-09-10",
  "basic_data": { ... },
  "technical_indicators": { ... },
  "fundamental_data": { ... },
  "company_data": {
    "sector": "ELECTRONIC COMPUTERS",
    "industry": "CS",
    "primary_exchange": "XNAS",
    "cik": "0000320193"
  },
  "metadata": {
    ...existing metadata...,
    "data_version": "v20250926_201234",
    "last_enhanced": "2025-09-26T20:12:34.567890",
    "enhancement_operation": "company_data_merge"
  }
}
```

### Version Tracking

Each merge operation creates:

1. **Unique Version ID**: `v{YYYYMMDD_HHMMSS}` format
2. **Backup Directory**: `{file_directory}/backups/{version}/`
3. **Metadata File**: `{base_path}/merge_metadata/merge_{version}.json`
4. **File-level Versioning**: Added to each file's metadata section

### Backup Structure

```
/workspaces/data/historical/daily/
├── AAPL/
│   ├── 2025/
│   │   └── 09/
│   │       ├── 2025-09-10.json          # Enhanced file
│   │       └── backups/
│   │           └── v20250926_201234/
│   │               └── 2025-09-10.json  # Original backup
│   └── ...
└── merge_metadata/
    └── merge_v20250926_201234.json      # Operation metadata
```

### Recovery

If issues are detected, backups can be restored:

```bash
# Restore from specific version backup
cp /workspaces/data/historical/daily/AAPL/2025/09/backups/v20250926_201234/2025-09-10.json \
   /workspaces/data/historical/daily/AAPL/2025/09/2025-09-10.json
```

### Monitoring

The script provides comprehensive logging:

- **Real-time Progress**: File processing status and statistics
- **Error Tracking**: Detailed error logs for troubleshooting
- **Performance Metrics**: API efficiency and processing speed
- **Quality Assurance**: Validation results and integrity checks

### Environment Requirements

```bash
# Required environment variable
export POLYGON_API_KEY="your_polygon_api_key"

# Required Python packages (already installed in project)
# - aiohttp
# - asyncio (built-in)
# - pathlib (built-in)
# - json (built-in)
```

### Example Output

```
2025-09-26 20:12:34 - INFO - Starting company data merge - Version: v20250926_201234
2025-09-26 20:12:34 - INFO - Found 1267 files to potentially enhance
2025-09-26 20:12:34 - INFO - Processing 247 unique tickers across 1267 files
2025-09-26 20:12:35 - INFO - Processing batch 1/5
2025-09-26 20:12:45 - INFO - Successfully enhanced /workspaces/data/historical/daily/AAPL/2025/09/2025-09-10.json
...
2025-09-26 20:15:23 - INFO - === MERGE OPERATION SUMMARY ===
2025-09-26 20:15:23 - INFO - Version: v20250926_201234
2025-09-26 20:15:23 - INFO - Files Processed: 1267
2025-09-26 20:15:23 - INFO - Files Enhanced: 1245
2025-09-26 20:15:23 - INFO - Files Skipped: 22 (already enhanced)
2025-09-26 20:15:23 - INFO - API Calls Made: 247
2025-09-26 20:15:23 - INFO - Errors Encountered: 0
2025-09-26 20:15:23 - INFO - Success Rate: 98.3%
2025-09-26 20:15:23 - INFO - Duration: 169.2 seconds
```