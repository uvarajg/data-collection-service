#!/bin/bash
# Daily Pipeline Launcher Script
#
# This script can be run manually or scheduled via cron
# Recommended cron schedule: 0 4 * * 1-5 (4 AM weekdays)

# Set working directory
cd /workspaces/data-collection-service

# Set up environment
export PYTHONPATH="/workspaces/data-collection-service/src:$PYTHONPATH"

# Create logs directory
mkdir -p logs

# Get current date for logging
LOG_DATE=$(date +%Y%m%d_%H%M%S)
LOG_FILE="logs/daily_pipeline_$LOG_DATE.log"

echo "========================================="
echo "Daily Pipeline Starting at $(date)"
echo "Log file: $LOG_FILE"
echo "========================================="

# Run the pipeline with output to both console and log file
python scripts/main/daily_pipeline_automation.py 2>&1 | tee "$LOG_FILE"

# Capture exit code
EXIT_CODE=${PIPESTATUS[0]}

echo ""
echo "========================================="
echo "Daily Pipeline Finished at $(date)"
echo "Exit code: $EXIT_CODE"
echo "Log saved to: $LOG_FILE"
echo "========================================="

# Keep only last 30 log files
find logs/ -name "daily_pipeline_*.log" -type f | sort | head -n -30 | xargs rm -f

exit $EXIT_CODE