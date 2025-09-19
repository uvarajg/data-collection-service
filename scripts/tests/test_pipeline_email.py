#!/usr/bin/env python3
"""
Test the email functionality of the daily pipeline
"""

import os
import sys
from pathlib import Path

# Add the current directory to ensure imports work
sys.path.insert(0, str(Path(__file__).parent))

# Import and test just the email functionality
from daily_pipeline_automation import DailyPipelineRunner

def test_email():
    """Test email functionality"""
    print("📧 Testing email configuration...")

    # Initialize runner (this loads .env)
    runner = DailyPipelineRunner()

    # Create dummy reports
    runner.reports = {
        'input_data_refresh': {
            'status': 'success',
            'duration_seconds': 195.3,
            'stocks_enriched': '2092'
        },
        'data_collection': {
            'status': 'success',
            'success_rate': 99.8,
            'successful_tickers': 1278
        },
        'data_validation': {
            'status': 'success',
            'total_files_found': 1278,
            'data_completeness': 99.69
        }
    }

    # Test email sending
    result = runner.step7_email_reports()

    if result:
        print("✅ Email test successful!")
    else:
        print("❌ Email test failed")

    return result

if __name__ == "__main__":
    success = test_email()
    sys.exit(0 if success else 1)