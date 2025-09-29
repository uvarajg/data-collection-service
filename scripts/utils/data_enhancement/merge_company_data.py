#!/usr/bin/env python3
"""
Safe Data Enhancement Script - Company Data Merger

This script safely merges new company classification data into existing datasets
without corrupting existing data or affecting quality/validation metrics.

Features:
- Non-destructive merge (preserves all existing data)
- Version tracking for debugging
- Backup creation before modifications
- Validation to ensure data integrity
- Company data extraction from Polygon API
- Batch processing with progress tracking
"""

import json
import os
import sys
import asyncio
import aiohttp
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import shutil
import hashlib
import logging

# Add project root to path
sys.path.append(str(Path(__file__).parents[3]))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/workspaces/data-collection-service/logs/data_merger.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CompanyDataMerger:
    """Safely merge company classification data into existing datasets"""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.polygon.io"
        self.session = None
        self.rate_limit_delay = 0.01  # 10ms for unlimited plan

        # Version tracking
        self.merge_version = f"v{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.merge_metadata = {
            "version": self.merge_version,
            "created_at": datetime.now().isoformat(),
            "operation": "company_data_merge",
            "description": "Added company classification data (sector, industry, exchange, CIK)",
            "data_source": "polygon.io_v3_ticker_details",
            "fields_added": ["sector", "industry", "primary_exchange", "cik"],
            "safety_measures": ["backup_created", "validation_checks", "non_destructive_merge"]
        }

        # Statistics tracking
        self.stats = {
            "files_processed": 0,
            "files_enhanced": 0,
            "files_skipped": 0,
            "api_calls_made": 0,
            "errors_encountered": 0,
            "start_time": None,
            "end_time": None
        }

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            connector=aiohttp.TCPConnector(limit=100, limit_per_host=10)
        )
        self.stats["start_time"] = datetime.now()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
        self.stats["end_time"] = datetime.now()

    async def _rate_limit(self):
        """Rate limiting for API calls"""
        await asyncio.sleep(self.rate_limit_delay)

    async def get_company_data(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Get company classification data from Polygon API"""
        url = f"{self.base_url}/v3/reference/tickers/{ticker}"
        params = {'apikey': self.api_key}

        try:
            await self._rate_limit()
            async with self.session.get(url, params=params) as response:
                self.stats["api_calls_made"] += 1

                if response.status == 200:
                    data = await response.json()
                    if data.get('status') == 'OK' and 'results' in data:
                        result = data['results']

                        # Extract company classification data
                        company_data = {
                            'sector': result.get('sic_description'),  # Standard Industrial Classification
                            'industry': result.get('type'),  # Security type/industry
                            'primary_exchange': result.get('primary_exchange'),
                            'cik': result.get('cik')  # Central Index Key for SEC filings
                        }

                        logger.debug(f"Retrieved company data for {ticker}: {company_data}")
                        return company_data
                else:
                    logger.warning(f"API error for {ticker}: HTTP {response.status}")

        except Exception as e:
            logger.warning(f"Could not get company data for {ticker}: {e}")
            self.stats["errors_encountered"] += 1

        return None

    def create_backup(self, file_path: Path) -> Path:
        """Create a backup of the original file"""
        backup_dir = file_path.parent / "backups" / self.merge_version
        backup_dir.mkdir(parents=True, exist_ok=True)

        backup_path = backup_dir / file_path.name
        shutil.copy2(file_path, backup_path)

        logger.debug(f"Created backup: {backup_path}")
        return backup_path

    def calculate_file_hash(self, file_path: Path) -> str:
        """Calculate SHA-256 hash of file for integrity verification"""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def validate_data_integrity(self, original_data: Dict, enhanced_data: Dict) -> bool:
        """Validate that enhanced data preserves all original data"""
        try:
            # Check that all original top-level keys are preserved
            for key in original_data.keys():
                if key not in enhanced_data:
                    logger.error(f"Missing original key: {key}")
                    return False

            # Validate critical data sections are unchanged
            critical_sections = ['basic_data', 'technical_indicators', 'fundamental_data', 'metadata']

            for section in critical_sections:
                if section in original_data:
                    if original_data[section] != enhanced_data.get(section):
                        logger.error(f"Data corruption detected in section: {section}")
                        return False

            # Ensure company_data was added correctly
            if 'company_data' not in enhanced_data:
                logger.error("Company data section not added")
                return False

            # Verify company_data structure
            company_data = enhanced_data['company_data']
            expected_fields = ['sector', 'industry', 'primary_exchange', 'cik']

            for field in expected_fields:
                if field not in company_data:
                    logger.error(f"Missing company data field: {field}")
                    return False

            logger.debug("Data integrity validation passed")
            return True

        except Exception as e:
            logger.error(f"Validation error: {e}")
            return False

    async def enhance_file(self, file_path: Path, ticker: str) -> bool:
        """Safely enhance a single file with company data"""
        try:
            self.stats["files_processed"] += 1

            # Read original file
            with open(file_path, 'r') as f:
                original_data = json.load(f)

            # Skip if already has company_data
            if 'company_data' in original_data:
                logger.debug(f"Skipping {file_path}: already has company_data")
                self.stats["files_skipped"] += 1
                return True

            # Create backup
            backup_path = self.create_backup(file_path)
            original_hash = self.calculate_file_hash(file_path)

            # Get company data from API
            company_data = await self.get_company_data(ticker)
            if not company_data:
                logger.warning(f"Could not retrieve company data for {ticker}")
                return False

            # Create enhanced data (non-destructive merge)
            enhanced_data = original_data.copy()
            enhanced_data['company_data'] = company_data

            # Add version tracking to metadata
            if 'metadata' not in enhanced_data:
                enhanced_data['metadata'] = {}

            enhanced_data['metadata']['data_version'] = self.merge_version
            enhanced_data['metadata']['last_enhanced'] = datetime.now().isoformat()
            enhanced_data['metadata']['enhancement_operation'] = "company_data_merge"

            # Validate data integrity
            if not self.validate_data_integrity(original_data, enhanced_data):
                logger.error(f"Data integrity validation failed for {file_path}")
                return False

            # Write enhanced data
            with open(file_path, 'w') as f:
                json.dump(enhanced_data, f, indent=2, default=str)

            # Verify file was written correctly
            with open(file_path, 'r') as f:
                verification_data = json.load(f)

            if not self.validate_data_integrity(original_data, verification_data):
                logger.error(f"Post-write validation failed for {file_path}")
                # Restore from backup
                shutil.copy2(backup_path, file_path)
                logger.info(f"Restored original file from backup: {file_path}")
                return False

            self.stats["files_enhanced"] += 1
            logger.info(f"Successfully enhanced {file_path}")
            return True

        except Exception as e:
            logger.error(f"Error enhancing {file_path}: {e}")
            self.stats["errors_encountered"] += 1
            return False

    def find_files_to_enhance(self, base_path: str, pattern: str = "**/*.json") -> List[Tuple[Path, str]]:
        """Find files that need company data enhancement"""
        base_path = Path(base_path)
        files_with_tickers = []

        for file_path in base_path.glob(pattern):
            if file_path.is_file() and file_path.suffix == '.json':
                # Extract ticker from file path (assumes structure: .../TICKER/YYYY/MM/YYYY-MM-DD.json)
                try:
                    ticker = file_path.parts[-4]  # Get ticker from path structure
                    files_with_tickers.append((file_path, ticker))
                except IndexError:
                    logger.warning(f"Could not extract ticker from path: {file_path}")
                    continue

        logger.info(f"Found {len(files_with_tickers)} files to potentially enhance")
        return files_with_tickers

    async def merge_company_data(self, base_path: str, pattern: str = "**/*.json", batch_size: int = 50) -> Dict[str, Any]:
        """Main method to merge company data into existing dataset"""
        logger.info(f"Starting company data merge - Version: {self.merge_version}")

        # Find files to enhance
        files_to_process = self.find_files_to_enhance(base_path, pattern)

        if not files_to_process:
            logger.warning("No files found to enhance")
            return self.get_summary()

        # Group files by ticker to minimize API calls
        ticker_files = {}
        for file_path, ticker in files_to_process:
            if ticker not in ticker_files:
                ticker_files[ticker] = []
            ticker_files[ticker].append(file_path)

        logger.info(f"Processing {len(ticker_files)} unique tickers across {len(files_to_process)} files")

        # Process files in batches by ticker
        tickers = list(ticker_files.keys())
        for i in range(0, len(tickers), batch_size):
            batch_tickers = tickers[i:i + batch_size]

            logger.info(f"Processing batch {i//batch_size + 1}/{(len(tickers) + batch_size - 1)//batch_size}")

            # Process each ticker in the batch
            for ticker in batch_tickers:
                # Get company data once per ticker
                company_data = await self.get_company_data(ticker)

                if company_data:
                    # Apply to all files for this ticker
                    for file_path in ticker_files[ticker]:
                        await self.enhance_file_with_data(file_path, ticker, company_data)
                else:
                    logger.warning(f"Skipping all files for ticker {ticker} - no company data available")
                    self.stats["files_skipped"] += len(ticker_files[ticker])

            # Small delay between batches
            await asyncio.sleep(0.5)

        # Save merge metadata
        await self.save_merge_metadata(base_path)

        return self.get_summary()

    async def enhance_file_with_data(self, file_path: Path, ticker: str, company_data: Dict[str, Any]) -> bool:
        """Enhance a file with pre-fetched company data"""
        try:
            self.stats["files_processed"] += 1

            # Read original file
            with open(file_path, 'r') as f:
                original_data = json.load(f)

            # Skip if already has company_data
            if 'company_data' in original_data:
                logger.debug(f"Skipping {file_path}: already has company_data")
                self.stats["files_skipped"] += 1
                return True

            # Create backup
            backup_path = self.create_backup(file_path)

            # Create enhanced data (non-destructive merge)
            enhanced_data = original_data.copy()
            enhanced_data['company_data'] = company_data.copy()

            # Add version tracking to metadata
            if 'metadata' not in enhanced_data:
                enhanced_data['metadata'] = {}

            enhanced_data['metadata']['data_version'] = self.merge_version
            enhanced_data['metadata']['last_enhanced'] = datetime.now().isoformat()
            enhanced_data['metadata']['enhancement_operation'] = "company_data_merge"

            # Validate data integrity
            if not self.validate_data_integrity(original_data, enhanced_data):
                logger.error(f"Data integrity validation failed for {file_path}")
                return False

            # Write enhanced data
            with open(file_path, 'w') as f:
                json.dump(enhanced_data, f, indent=2, default=str)

            # Verify file was written correctly
            with open(file_path, 'r') as f:
                verification_data = json.load(f)

            if not self.validate_data_integrity(original_data, verification_data):
                logger.error(f"Post-write validation failed for {file_path}")
                # Restore from backup
                shutil.copy2(backup_path, file_path)
                logger.info(f"Restored original file from backup: {file_path}")
                return False

            self.stats["files_enhanced"] += 1
            logger.debug(f"Successfully enhanced {file_path}")
            return True

        except Exception as e:
            logger.error(f"Error enhancing {file_path}: {e}")
            self.stats["errors_encountered"] += 1
            return False

    async def save_merge_metadata(self, base_path: str):
        """Save merge operation metadata for debugging and tracking"""
        metadata_dir = Path(base_path) / "merge_metadata"
        metadata_dir.mkdir(exist_ok=True)

        metadata_file = metadata_dir / f"merge_{self.merge_version}.json"

        complete_metadata = {
            **self.merge_metadata,
            "statistics": self.stats,
            "duration_seconds": (self.stats["end_time"] - self.stats["start_time"]).total_seconds() if self.stats["end_time"] else None
        }

        with open(metadata_file, 'w') as f:
            json.dump(complete_metadata, f, indent=2, default=str)

        logger.info(f"Merge metadata saved to: {metadata_file}")

    def get_summary(self) -> Dict[str, Any]:
        """Get operation summary"""
        duration = (self.stats["end_time"] - self.stats["start_time"]).total_seconds() if self.stats["end_time"] else None

        return {
            "version": self.merge_version,
            "operation": "company_data_merge",
            "statistics": self.stats,
            "duration_seconds": duration,
            "success_rate": (self.stats["files_enhanced"] / self.stats["files_processed"] * 100) if self.stats["files_processed"] > 0 else 0,
            "api_efficiency": (self.stats["files_enhanced"] / self.stats["api_calls_made"]) if self.stats["api_calls_made"] > 0 else 0
        }

async def main():
    """Main execution function"""
    import argparse

    parser = argparse.ArgumentParser(description='Safely merge company data into existing datasets')
    parser.add_argument('base_path', help='Base path to search for JSON files')
    parser.add_argument('--pattern', default='**/*.json', help='File pattern to match (default: **/*.json)')
    parser.add_argument('--batch-size', type=int, default=50, help='Batch size for processing (default: 50)')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be processed without making changes')
    args = parser.parse_args()

    # Configuration
    api_key = os.getenv('POLYGON_API_KEY')
    if not api_key and not args.dry_run:
        logger.error("POLYGON_API_KEY environment variable not set")
        sys.exit(1)
    elif not api_key:
        api_key = "dummy_key_for_dry_run"

    if args.dry_run:
        logger.info("DRY RUN MODE - No files will be modified")
        merger = CompanyDataMerger(api_key)
        files_to_process = merger.find_files_to_enhance(args.base_path, args.pattern)

        if files_to_process:
            logger.info(f"Would process {len(files_to_process)} files")
            # Show sample of what would be processed
            for file_path, ticker in files_to_process[:10]:
                logger.info(f"  {ticker}: {file_path}")
            if len(files_to_process) > 10:
                logger.info(f"  ... and {len(files_to_process) - 10} more files")
        else:
            logger.info("No files found to process")
        return

    # Execute merge operation
    async with CompanyDataMerger(api_key) as merger:
        summary = await merger.merge_company_data(
            base_path=args.base_path,
            pattern=args.pattern,
            batch_size=args.batch_size
        )

        # Print summary
        logger.info("=== MERGE OPERATION SUMMARY ===")
        logger.info(f"Version: {summary['version']}")
        logger.info(f"Files Processed: {summary['statistics']['files_processed']}")
        logger.info(f"Files Enhanced: {summary['statistics']['files_enhanced']}")
        logger.info(f"Files Skipped: {summary['statistics']['files_skipped']}")
        logger.info(f"API Calls Made: {summary['statistics']['api_calls_made']}")
        logger.info(f"Errors Encountered: {summary['statistics']['errors_encountered']}")
        logger.info(f"Success Rate: {summary['success_rate']:.1f}%")
        logger.info(f"Duration: {summary['duration_seconds']:.1f} seconds")

if __name__ == "__main__":
    asyncio.run(main())