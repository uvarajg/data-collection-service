#!/usr/bin/env python3
"""
Real-time monitoring script for the daily pipeline
"""

import time
import os
import json
import subprocess
from datetime import datetime
from pathlib import Path

def check_process_status():
    """Check if pipeline processes are running"""
    try:
        result = subprocess.run(
            ["ps", "aux"],
            capture_output=True,
            text=True
        )

        processes = {
            'pipeline': False,
            'collect_us_market': False,
            'data_collection': False,
            'validation': False
        }

        for line in result.stdout.split('\n'):
            if 'daily_pipeline' in line and 'grep' not in line:
                processes['pipeline'] = True
            if 'collect_us_market' in line and 'grep' not in line:
                processes['collect_us_market'] = True
            if 'data_coordinator' in line or 'run_collection' in line:
                processes['data_collection'] = True
            if 'validation' in line and 'data-validation-service' in line:
                processes['validation'] = True

        return processes
    except:
        return {}

def get_latest_files():
    """Get latest created files"""
    today = datetime.now().strftime('%Y%m%d')

    files = {
        'input_source': [],
        'reports': [],
        'validation': []
    }

    # Check input source files
    input_dir = Path('/workspaces/data/input_source')
    if input_dir.exists():
        for pattern in ['raw_combined_*.json', 'enriched_yfinance_*.json', 'input_source_data_job_summary_*.json']:
            for f in input_dir.glob(f'{pattern[:-5]}{today}*.json'):
                files['input_source'].append({
                    'name': f.name,
                    'size_mb': f.stat().st_size / (1024*1024),
                    'modified': datetime.fromtimestamp(f.stat().st_mtime).strftime('%H:%M:%S')
                })

    # Check reports
    reports_dir = Path('/workspaces/data-collection-service/reports')
    if reports_dir.exists():
        for f in reports_dir.glob(f'*{today}*.json'):
            files['reports'].append({
                'name': f.name,
                'modified': datetime.fromtimestamp(f.stat().st_mtime).strftime('%H:%M:%S')
            })

    return files

def estimate_step(processes, files):
    """Estimate which step is currently running"""
    if processes.get('collect_us_market'):
        return 1, "Input Data Refresh", "Downloading and enriching US market stocks"
    elif any('input_data_report' in f['name'] for f in files.get('reports', [])):
        if processes.get('data_collection'):
            return 3, "Data Collection", "Collecting historical data for previous business day"
        elif any('data_collection_report' in f['name'] for f in files.get('reports', [])):
            if processes.get('validation'):
                return 5, "Data Validation", "Validating data quality"
            elif any('validation_report' in f['name'] for f in files.get('reports', [])):
                return 7, "Email Reports", "Sending reports via email"
            else:
                return 6, "Validation Report", "Generating validation report"
        else:
            return 4, "Collection Report", "Generating data collection report"
    else:
        return 2, "Input Report", "Generating input data refresh report"

def monitor_pipeline():
    """Main monitoring loop"""
    print("=" * 80)
    print("📊 DAILY PIPELINE MONITOR")
    print("=" * 80)
    print(f"Started monitoring at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Press Ctrl+C to stop monitoring\n")

    last_step = 0
    start_time = datetime.now()

    while True:
        # Clear screen for update (optional - comment out if you prefer scrolling)
        # os.system('clear' if os.name == 'posix' else 'cls')

        processes = check_process_status()
        files = get_latest_files()

        # Check if pipeline is still running
        if not processes.get('pipeline'):
            print("\n" + "=" * 80)
            print("✅ PIPELINE COMPLETED!")
            print("=" * 80)

            duration = (datetime.now() - start_time).total_seconds()
            print(f"Total duration: {duration:.1f} seconds")

            # Show final files
            print("\n📁 Generated Files:")
            for category, file_list in files.items():
                if file_list:
                    print(f"\n{category.upper()}:")
                    for f in file_list:
                        print(f"  - {f['name']} ({f.get('size_mb', 0):.2f} MB)")

            break

        # Estimate current step
        current_step, step_name, description = estimate_step(processes, files)

        if current_step != last_step:
            print(f"\n{'='*60}")
            print(f"Step {current_step}/7: {step_name}")
            print(f"{'='*60}")
            last_step = current_step

        # Show current status
        elapsed = (datetime.now() - start_time).total_seconds()
        print(f"\r⏱️  Elapsed: {elapsed:.0f}s | 📍 {description}", end="", flush=True)

        # Show active processes
        active = [k for k, v in processes.items() if v and k != 'pipeline']
        if active:
            print(f" | 🔄 Active: {', '.join(active)}", end="", flush=True)

        # Check for new files
        new_files = []
        for file_list in files.values():
            for f in file_list:
                # Files modified in last 30 seconds
                try:
                    mod_time = datetime.strptime(f['modified'], '%H:%M:%S')
                    current_time = datetime.now()
                    if (current_time.hour == mod_time.hour and
                        current_time.minute == mod_time.minute and
                        abs(current_time.second - mod_time.second) < 30):
                        new_files.append(f['name'])
                except:
                    pass

        if new_files:
            print(f"\n📄 New file: {new_files[-1]}")

        # Wait before next check
        time.sleep(5)

    print("\n✨ Monitoring complete!")
    return True

if __name__ == "__main__":
    try:
        monitor_pipeline()
    except KeyboardInterrupt:
        print("\n\n⚠️  Monitoring stopped by user")
    except Exception as e:
        print(f"\n❌ Monitoring error: {e}")