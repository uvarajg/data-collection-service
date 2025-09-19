#!/usr/bin/env python3
"""
Archive Historical Data Script

Archives historical data collected before a specified date.
Compresses data into tar.gz archives organized by month and year.
Maintains data integrity while reducing storage space.
"""

import os
import sys
import json
import tarfile
import shutil
from datetime import datetime, timedelta
from pathlib import Path
import argparse
from collections import defaultdict

class HistoricalDataArchiver:
    def __init__(self, cutoff_date, base_path="/workspaces/data/historical/daily",
                 archive_path="/workspaces/data/archives/historical/daily",
                 dry_run=False, auto_confirm=False):
        """
        Initialize the archiver

        Args:
            cutoff_date: Date string in YYYY-MM-DD format (inclusive - files on or before this date)
            base_path: Path to historical data
            archive_path: Path to store archives
            dry_run: If True, only show what would be archived without actually doing it
            auto_confirm: If True, skip confirmation prompt
        """
        self.cutoff_date = datetime.strptime(cutoff_date, "%Y-%m-%d")
        self.base_path = Path(base_path)
        self.archive_path = Path(archive_path)
        self.dry_run = dry_run
        self.auto_confirm = auto_confirm

        # Statistics
        self.stats = {
            'files_found': 0,
            'files_archived': 0,
            'files_deleted': 0,
            'space_saved_mb': 0,
            'archives_created': 0,
            'errors': []
        }

        # Archive organization: ticker -> year -> month -> files
        self.files_to_archive = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

        print("=" * 70)
        print("📦 HISTORICAL DATA ARCHIVER")
        print("=" * 70)
        print(f"📅 Cutoff Date: {cutoff_date} (inclusive)")
        print(f"📁 Source Path: {self.base_path}")
        print(f"🗄️  Archive Path: {self.archive_path}")
        print(f"🔍 Mode: {'DRY RUN - No changes will be made' if dry_run else 'LIVE - Files will be archived'}")
        print("=" * 70)

    def find_files_to_archive(self):
        """Find all files that need to be archived"""
        print("\n🔍 Scanning for files to archive...")

        for ticker_dir in sorted(self.base_path.iterdir()):
            if not ticker_dir.is_dir():
                continue

            ticker = ticker_dir.name

            # Scan year directories
            for year_dir in ticker_dir.iterdir():
                if not year_dir.is_dir():
                    continue

                year = year_dir.name

                # Scan month directories
                for month_dir in year_dir.iterdir():
                    if not month_dir.is_dir():
                        continue

                    month = month_dir.name

                    # Check JSON files
                    for json_file in month_dir.glob("*.json"):
                        # Parse date from filename (YYYY-MM-DD.json)
                        try:
                            file_date_str = json_file.stem  # Remove .json
                            file_date = datetime.strptime(file_date_str, "%Y-%m-%d")

                            # Check if file should be archived
                            if file_date <= self.cutoff_date:
                                self.files_to_archive[ticker][year][month].append(json_file)
                                self.stats['files_found'] += 1

                        except ValueError:
                            # Skip files that don't match date format
                            continue

        # Print summary
        total_tickers = len(self.files_to_archive)
        print(f"✅ Found {self.stats['files_found']} files to archive")
        print(f"📊 Across {total_tickers} tickers")

        if total_tickers > 0:
            # Show sample
            sample_ticker = list(self.files_to_archive.keys())[0]
            sample_count = sum(
                len(files)
                for year_data in self.files_to_archive[sample_ticker].values()
                for files in year_data.values()
            )
            print(f"📝 Example: {sample_ticker} has {sample_count} files to archive")

    def create_archive(self, ticker, year, month, files):
        """Create a compressed archive for a specific ticker/year/month"""
        archive_name = f"{ticker}_{year}_{month}.tar.gz"
        archive_dir = self.archive_path / ticker / year
        archive_file = archive_dir / archive_name

        if self.dry_run:
            print(f"  [DRY RUN] Would create: {archive_file}")
            return True

        try:
            # Create archive directory
            archive_dir.mkdir(parents=True, exist_ok=True)

            # Calculate original size
            original_size = sum(f.stat().st_size for f in files) / (1024 * 1024)  # MB

            # Create compressed archive
            with tarfile.open(archive_file, "w:gz") as tar:
                for file_path in files:
                    # Add file with relative path
                    arcname = f"{ticker}/{year}/{month}/{file_path.name}"
                    tar.add(file_path, arcname=arcname)

            # Calculate compressed size
            compressed_size = archive_file.stat().st_size / (1024 * 1024)  # MB

            # Update statistics
            self.stats['archives_created'] += 1
            self.stats['space_saved_mb'] += (original_size - compressed_size)

            print(f"  ✅ Created: {archive_name} ({len(files)} files, "
                  f"{original_size:.2f}MB → {compressed_size:.2f}MB, "
                  f"saved {((1 - compressed_size/original_size) * 100):.1f}%)")

            return True

        except Exception as e:
            self.stats['errors'].append(f"Failed to archive {ticker}/{year}/{month}: {str(e)}")
            print(f"  ❌ Error archiving {archive_name}: {e}")
            return False

    def verify_archive(self, archive_file, original_files):
        """Verify that an archive contains all expected files"""
        if self.dry_run:
            return True

        try:
            with tarfile.open(archive_file, "r:gz") as tar:
                archive_members = set(tar.getnames())
                expected_count = len(original_files)
                actual_count = len(archive_members)

                if actual_count != expected_count:
                    raise ValueError(f"Archive has {actual_count} files, expected {expected_count}")

            return True

        except Exception as e:
            self.stats['errors'].append(f"Failed to verify {archive_file}: {str(e)}")
            return False

    def delete_original_files(self, files):
        """Delete original files after successful archiving"""
        if self.dry_run:
            print(f"  [DRY RUN] Would delete {len(files)} original files")
            self.stats['files_deleted'] += len(files)
            return

        for file_path in files:
            try:
                file_path.unlink()
                self.stats['files_deleted'] += 1
            except Exception as e:
                self.stats['errors'].append(f"Failed to delete {file_path}: {str(e)}")

    def cleanup_empty_directories(self):
        """Remove empty directories after archiving"""
        if self.dry_run:
            print("\n[DRY RUN] Would clean up empty directories")
            return

        print("\n🧹 Cleaning up empty directories...")

        for ticker_dir in self.base_path.iterdir():
            if not ticker_dir.is_dir():
                continue

            # Clean up month directories
            for year_dir in ticker_dir.iterdir():
                if not year_dir.is_dir():
                    continue

                for month_dir in year_dir.iterdir():
                    if not month_dir.is_dir():
                        continue

                    # Check if directory is empty
                    if not any(month_dir.iterdir()):
                        month_dir.rmdir()
                        print(f"  Removed empty: {month_dir.relative_to(self.base_path)}")

                # Check if year directory is empty
                if not any(year_dir.iterdir()):
                    year_dir.rmdir()
                    print(f"  Removed empty: {year_dir.relative_to(self.base_path)}")

            # Check if ticker directory is empty
            if not any(ticker_dir.iterdir()):
                ticker_dir.rmdir()
                print(f"  Removed empty: {ticker_dir.name}/")

    def run(self):
        """Execute the archiving process"""
        # Step 1: Find files
        self.find_files_to_archive()

        if self.stats['files_found'] == 0:
            print("\n✅ No files to archive. All data is newer than the cutoff date.")
            return

        # Step 2: Confirm with user
        if not self.dry_run and not self.auto_confirm:
            print(f"\n⚠️  Ready to archive {self.stats['files_found']} files")
            confirm = input("Proceed with archiving? (y/n): ").strip().lower()
            if confirm != 'y':
                print("❌ Archiving cancelled by user")
                return
        elif not self.dry_run and self.auto_confirm:
            print(f"\n✅ Auto-confirming archiving of {self.stats['files_found']} files")

        # Step 3: Create archives
        print(f"\n📦 Creating archives...")

        for ticker, year_data in sorted(self.files_to_archive.items()):
            print(f"\n📊 Processing {ticker}...")

            for year, month_data in sorted(year_data.items()):
                for month, files in sorted(month_data.items()):
                    # Create archive
                    success = self.create_archive(ticker, year, month, sorted(files))

                    if success and not self.dry_run:
                        # Verify archive
                        archive_file = self.archive_path / ticker / year / f"{ticker}_{year}_{month}.tar.gz"
                        if self.verify_archive(archive_file, files):
                            # Delete original files
                            self.delete_original_files(files)
                            self.stats['files_archived'] += len(files)
                        else:
                            print(f"  ⚠️  Skipping deletion - verification failed")

        # Step 4: Clean up empty directories
        self.cleanup_empty_directories()

        # Step 5: Print summary
        self.print_summary()

    def print_summary(self):
        """Print final summary of archiving operation"""
        print("\n" + "=" * 70)
        print("📊 ARCHIVING SUMMARY")
        print("=" * 70)

        if self.dry_run:
            print("🔍 DRY RUN RESULTS (no actual changes made):")
        else:
            print("✅ ARCHIVING COMPLETED:")

        print(f"  • Files found: {self.stats['files_found']}")
        print(f"  • Files archived: {self.stats['files_archived']}")
        print(f"  • Files deleted: {self.stats['files_deleted']}")
        print(f"  • Archives created: {self.stats['archives_created']}")
        print(f"  • Space saved: {self.stats['space_saved_mb']:.2f} MB")

        if self.stats['errors']:
            print(f"\n⚠️  Errors encountered: {len(self.stats['errors'])}")
            for error in self.stats['errors'][:5]:  # Show first 5 errors
                print(f"  • {error}")

        if not self.dry_run and self.stats['files_archived'] > 0:
            print(f"\n✅ Successfully archived {self.stats['files_archived']} files")
            print(f"📁 Archives stored in: {self.archive_path}")

def main():
    parser = argparse.ArgumentParser(
        description="Archive historical data collected before a specified date"
    )
    parser.add_argument(
        "--cutoff-date",
        type=str,
        default="2025-09-14",
        help="Archive files on or before this date (YYYY-MM-DD format)"
    )
    parser.add_argument(
        "--base-path",
        type=str,
        default="/workspaces/data/historical/daily",
        help="Path to historical data directory"
    )
    parser.add_argument(
        "--archive-path",
        type=str,
        default="/workspaces/data/archives/historical/daily",
        help="Path to store archives"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be archived without actually doing it"
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Skip confirmation prompt and proceed automatically"
    )

    args = parser.parse_args()

    # Create archiver and run
    archiver = HistoricalDataArchiver(
        cutoff_date=args.cutoff_date,
        base_path=args.base_path,
        archive_path=args.archive_path,
        dry_run=args.dry_run,
        auto_confirm=args.yes
    )

    archiver.run()

if __name__ == "__main__":
    main()