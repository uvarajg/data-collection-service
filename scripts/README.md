# Scripts Directory Organization

This directory contains all scripts organized by purpose and usage frequency.

## 📁 Directory Structure

### 🎯 main/
Core production scripts for daily operations:
- `collect_us_market_stocks.py` - Primary US market data collection (7,038 → 2,077 stocks)
- `archive_historical_data.py` - Historical data archiving with compression
- `run_data_collection_with_dates.py` - Interactive collection for specific dates
- `daily_pipeline_automation.py` - Automated daily pipeline orchestration

### 🔧 utils/
Utility and maintenance scripts:
- `compress_old_files.py` - Storage optimization and cleanup
- `monitor_pipeline.py` - Pipeline monitoring and alerts
- `monitor_live.py` - Live data monitoring

### 🗃️ legacy/
Older scripts maintained for compatibility:
- Various run_collection_*.py scripts
- Alternative collection approaches
- Historical implementations

### 🧪 tests/
Testing and demonstration scripts:
- test_*.py - Various system tests
- demo_*.py - Demonstration scripts

## 🚀 Quick Start

**Daily Collection:**
```bash
cd scripts/main
python collect_us_market_stocks.py
```

**Interactive Collection:**
```bash
cd scripts/main
python run_data_collection_with_dates.py
```

**Archive Old Data:**
```bash
cd scripts/main
python archive_historical_data.py --cutoff-date 2025-09-14 --dry-run
```

**Cleanup Storage:**
```bash
cd scripts/utils
python compress_old_files.py
```