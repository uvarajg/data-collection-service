#!/usr/bin/env python3
"""
Compress old data files to save space
Keeps raw_combined and enriched_yfinance files forever in compressed format
"""

import os
import gzip
import shutil
from datetime import datetime, timedelta
import glob
import json

def compress_file(filepath):
    """Compress a file using gzip"""
    compressed_filepath = f"{filepath}.gz"

    with open(filepath, 'rb') as f_in:
        with gzip.open(compressed_filepath, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    # Remove original file after compression
    os.remove(filepath)

    # Get file sizes for reporting
    original_size = os.path.getsize(filepath) if os.path.exists(filepath) else 0
    compressed_size = os.path.getsize(compressed_filepath)

    return compressed_filepath, original_size, compressed_size

def get_file_age_days(filepath):
    """Get file age in days"""
    file_time = os.path.getmtime(filepath)
    file_date = datetime.fromtimestamp(file_time)
    age = datetime.now() - file_date
    return age.days

def cleanup_old_files(base_path="/workspaces/data/input_source", days_old=7):
    """
    Cleanup strategy:
    - Keep raw_combined_*.json forever (compress if > 7 days old)
    - Keep enriched_yfinance_*.json forever (compress if > 7 days old)
    - Delete all other files if > 7 days old
    """

    print("=" * 60)
    print("📦 File Compression and Cleanup Utility")
    print("=" * 60)

    if not os.path.exists(base_path):
        print(f"❌ Directory not found: {base_path}")
        return

    # Files to keep forever (compress if old)
    keep_patterns = [
        "raw_combined_*.json",
        "enriched_yfinance_*.json"
    ]

    # Files to delete if old
    delete_patterns = [
        "enriched_intermediate_*.json",
        "failed_tickers_*.json",
        "input_source_data_job_summary_*.json",  # New naming - delete after 7 days
        # Legacy patterns from old naming
        "data_collection_summary_*.json",  # Old naming
        "set_*.json",
        "set_*.csv",
        "*.csv"  # Remove all CSV files
    ]

    compressed_count = 0
    deleted_count = 0
    total_space_saved = 0

    # Process files to keep (compress if old)
    print("\n📊 Processing files to keep forever...")
    for pattern in keep_patterns:
        files = glob.glob(os.path.join(base_path, pattern))

        for filepath in files:
            # Skip if already compressed
            if filepath.endswith('.gz'):
                continue

            age_days = get_file_age_days(filepath)

            if age_days > days_old:
                print(f"  📦 Compressing {os.path.basename(filepath)} ({age_days} days old)...")
                try:
                    original_size = os.path.getsize(filepath)
                    compressed_path, _, compressed_size = compress_file(filepath)
                    space_saved = original_size - compressed_size
                    total_space_saved += space_saved
                    compressed_count += 1

                    print(f"     ✅ Compressed: {original_size/1024/1024:.1f}MB → {compressed_size/1024/1024:.1f}MB")
                    print(f"     💾 Saved: {space_saved/1024/1024:.1f}MB ({(space_saved/original_size)*100:.1f}%)")
                except Exception as e:
                    print(f"     ❌ Error compressing: {e}")

    # Process files to delete if old
    print("\n🗑️ Processing files to delete if old...")
    for pattern in delete_patterns:
        files = glob.glob(os.path.join(base_path, pattern))

        for filepath in files:
            filename = os.path.basename(filepath)

            age_days = get_file_age_days(filepath)

            if age_days > days_old:
                file_size = os.path.getsize(filepath)
                print(f"  🗑️ Deleting {filename} ({age_days} days old, {file_size/1024/1024:.1f}MB)...")
                try:
                    os.remove(filepath)
                    deleted_count += 1
                    total_space_saved += file_size
                    print(f"     ✅ Deleted")
                except Exception as e:
                    print(f"     ❌ Error deleting: {e}")

    # List current files
    print("\n📁 Current files in directory:")
    all_files = glob.glob(os.path.join(base_path, "*"))
    all_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)

    for filepath in all_files[:10]:  # Show latest 10 files
        file_size = os.path.getsize(filepath)
        age_days = get_file_age_days(filepath)
        filename = os.path.basename(filepath)

        if filepath.endswith('.gz'):
            print(f"  📦 {filename} ({file_size/1024/1024:.1f}MB, {age_days} days old)")
        else:
            print(f"  📄 {filename} ({file_size/1024/1024:.1f}MB, {age_days} days old)")

    if len(all_files) > 10:
        print(f"  ... and {len(all_files) - 10} more files")

    # Summary
    print("\n" + "=" * 60)
    print("✅ Cleanup Complete!")
    print(f"  📦 Files compressed: {compressed_count}")
    print(f"  🗑️ Files deleted: {deleted_count}")
    print(f"  💾 Total space saved: {total_space_saved/1024/1024:.1f}MB")
    print("=" * 60)

def decompress_file(compressed_filepath):
    """Decompress a .gz file if needed"""
    if not compressed_filepath.endswith('.gz'):
        print(f"❌ File is not compressed: {compressed_filepath}")
        return None

    output_filepath = compressed_filepath[:-3]  # Remove .gz extension

    print(f"📂 Decompressing {os.path.basename(compressed_filepath)}...")

    with gzip.open(compressed_filepath, 'rb') as f_in:
        with open(output_filepath, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    print(f"✅ Decompressed to: {output_filepath}")
    return output_filepath

def load_latest_enriched_data(base_path="/workspaces/data/input_source"):
    """Load the latest enriched data file (compressed or not)"""

    # Look for both compressed and uncompressed files
    enriched_files = glob.glob(os.path.join(base_path, "enriched_yfinance_*.json"))
    enriched_files += glob.glob(os.path.join(base_path, "enriched_yfinance_*.json.gz"))

    if not enriched_files:
        print("❌ No enriched data files found")
        return None

    # Get the latest file
    latest_file = max(enriched_files, key=os.path.getmtime)

    print(f"📂 Loading latest enriched data: {os.path.basename(latest_file)}")

    # Decompress if needed
    if latest_file.endswith('.gz'):
        with gzip.open(latest_file, 'rt') as f:
            data = json.load(f)
        print(f"✅ Loaded {len(data)} stocks from compressed file")
    else:
        with open(latest_file, 'r') as f:
            data = json.load(f)
        print(f"✅ Loaded {len(data)} stocks from JSON file")

    return data

if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--decompress":
        # Decompress mode
        if len(sys.argv) < 3:
            print("Usage: python compress_old_files.py --decompress <file.gz>")
        else:
            decompress_file(sys.argv[2])
    elif len(sys.argv) > 1 and sys.argv[1] == "--load":
        # Load latest enriched data
        data = load_latest_enriched_data()
        if data:
            print(f"📊 Loaded {len(data)} stocks successfully")
    else:
        # Default: cleanup old files
        cleanup_old_files()