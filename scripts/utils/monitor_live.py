#!/usr/bin/env python3
"""
Live monitoring for the running pipeline
"""

import time
import subprocess
import json
from datetime import datetime
from pathlib import Path

def get_process_info():
    """Get info about running processes"""
    result = subprocess.run(
        ["ps", "aux"],
        capture_output=True,
        text=True
    )

    processes = []
    for line in result.stdout.split('\n'):
        if 'daily_pipeline' in line and 'grep' not in line:
            processes.append('Pipeline Main')
        elif 'run_data_collection_automated' in line and 'grep' not in line:
            processes.append('Data Collection')
        elif 'collect_us_market' in line and 'grep' not in line:
            processes.append('Market Data Refresh')

    return processes

def count_files_created(after_time="08:33"):
    """Count files created after specified time"""
    cmd = f"""find /workspaces/data/historical/daily -name "2025-09-16.json" -newermt "2025-09-17 {after_time}:00" 2>/dev/null | wc -l"""
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return int(result.stdout.strip())

def get_latest_ticker():
    """Get the most recently processed ticker"""
    cmd = """find /workspaces/data/historical/daily -name "2025-09-16.json" -newermt "2025-09-17 08:33:00" 2>/dev/null | tail -1 | xargs dirname | xargs basename"""
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return result.stdout.strip()

def estimate_progress(files_count, total_expected=2092):
    """Estimate progress percentage"""
    return min(100, round((files_count / total_expected) * 100, 1))

def monitor():
    """Main monitoring loop"""
    print("=" * 70)
    print("📊 LIVE PIPELINE MONITOR")
    print("=" * 70)
    print(f"Started at: {datetime.now().strftime('%H:%M:%S')}")
    print("Press Ctrl+C to stop\n")

    start_time = datetime.now()
    last_count = 0

    while True:
        processes = get_process_info()

        if not processes:
            print("\n✅ Pipeline completed!")
            break

        files_count = count_files_created()
        progress = estimate_progress(files_count)
        elapsed = (datetime.now() - start_time).seconds

        # Calculate rate
        if elapsed > 0:
            rate = files_count / (elapsed / 60)  # files per minute
        else:
            rate = 0

        # Estimate remaining time
        if rate > 0 and files_count < 2092:
            remaining_files = 2092 - files_count
            remaining_mins = remaining_files / rate
        else:
            remaining_mins = 0

        # Get latest ticker
        latest = get_latest_ticker()

        # Clear line and print status
        print(f"\r{'=' * 70}", end="")
        print(f"\r⏱️  {elapsed}s | 📁 {files_count}/2092 ({progress}%) | ", end="")
        print(f"📈 {rate:.0f}/min | ⏳ {remaining_mins:.0f}m left | ", end="")
        print(f"🔄 {latest[:10]}...", end="", flush=True)

        # Check if there's progress
        if files_count > last_count:
            last_count = files_count

        time.sleep(5)

    # Final summary
    print("\n" + "=" * 70)
    print(f"✅ Collection Phase Complete!")
    print(f"📊 Total files created: {files_count}")
    print(f"⏱️  Total time: {elapsed} seconds")
    print("=" * 70)

if __name__ == "__main__":
    try:
        monitor()
    except KeyboardInterrupt:
        print("\n\nMonitoring stopped by user")