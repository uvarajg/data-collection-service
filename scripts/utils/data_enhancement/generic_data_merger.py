#!/usr/bin/env python3
"""
Generic Data Enhancement Script

This script safely merges any new data points into existing datasets
without corrupting existing data or affecting quality/validation metrics.

Supports adding:
- New technical indicators
- Additional fundamental data points
- Company classification data
- Custom metadata fields
- Any other structured data

Features:
- Non-destructive merge (preserves all existing data)
- Version tracking for debugging
- Backup creation before modifications
- Validation to ensure data integrity
- Flexible data source configuration
- Batch processing with progress tracking
- Extensible enhancement providers
"""

import json
import os
import sys
import asyncio
import aiohttp
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple, Callable
import shutil
import hashlib
import logging
from abc import ABC, abstractmethod

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

class DataEnhancementProvider(ABC):
    """Abstract base class for data enhancement providers"""

    @abstractmethod
    async def get_enhancement_data(self, ticker: str, existing_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Get enhancement data for a ticker"""
        pass

    @abstractmethod
    def get_target_sections(self) -> List[str]:
        """Get list of target sections this provider enhances"""
        pass

    @abstractmethod
    def get_provider_name(self) -> str:
        """Get provider name for tracking"""
        pass

class PolygonCompanyDataProvider(DataEnhancementProvider):
    """Provider for Polygon company classification data"""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.polygon.io"
        self.session = None
        self.rate_limit_delay = 0.01  # 10ms for unlimited plan

    async def setup_session(self, session: aiohttp.ClientSession):
        """Setup HTTP session"""
        self.session = session

    async def _rate_limit(self):
        """Rate limiting for API calls"""
        await asyncio.sleep(self.rate_limit_delay)

    async def get_enhancement_data(self, ticker: str, existing_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Get company classification data from Polygon API"""
        url = f"{self.base_url}/v3/reference/tickers/{ticker}"
        params = {'apikey': self.api_key}

        try:
            await self._rate_limit()
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get('status') == 'OK' and 'results' in data:
                        result = data['results']

                        # Return enhancement data
                        return {
                            'company_data': {
                                'sector': result.get('sic_description'),
                                'industry': result.get('type'),
                                'primary_exchange': result.get('primary_exchange'),
                                'cik': result.get('cik')
                            }
                        }
                else:
                    logger.warning(f"API error for {ticker}: HTTP {response.status}")

        except Exception as e:
            logger.warning(f"Could not get company data for {ticker}: {e}")

        return None

    def get_target_sections(self) -> List[str]:
        """Company data provider targets the company_data section"""
        return ['company_data']

    def get_provider_name(self) -> str:
        """Provider name"""
        return "polygon_company_data"

class CustomTechnicalIndicatorProvider(DataEnhancementProvider):
    """Provider for custom technical indicators"""

    def __init__(self, indicator_functions: Dict[str, Callable]):
        self.indicator_functions = indicator_functions

    async def setup_session(self, session: aiohttp.ClientSession):
        """No session needed for technical indicators"""
        pass

    async def get_enhancement_data(self, ticker: str, existing_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Calculate custom technical indicators"""
        try:
            # Check if we have existing technical data (either format)
            existing_technical = existing_data.get('technical_indicators') or existing_data.get('technical')
            basic_data = existing_data.get('basic_data', {})

            # Need either basic data or existing technical data
            if not basic_data and not existing_technical:
                return None

            enhancements = {}

            # Calculate each custom indicator
            for indicator_name, calc_function in self.indicator_functions.items():
                try:
                    value = calc_function(existing_data)
                    if value is not None:
                        # Determine target section based on existing data structure
                        if 'technical_indicators' in existing_data:
                            target_section = 'technical_indicators'
                        elif 'technical' in existing_data:
                            target_section = 'technical'
                        else:
                            target_section = 'technical_indicators'  # Default

                        if target_section not in enhancements:
                            enhancements[target_section] = {}
                        enhancements[target_section][indicator_name] = value

                        logger.debug(f"Calculated {indicator_name} = {value} for {ticker}")

                except Exception as e:
                    logger.warning(f"Failed to calculate {indicator_name} for {ticker}: {e}")

            return enhancements if enhancements else None

        except Exception as e:
            logger.warning(f"Could not calculate technical indicators for {ticker}: {e}")
            return None

    def get_target_sections(self) -> List[str]:
        """Technical indicator provider targets technical_indicators or technical section"""
        return ['technical_indicators', 'technical']

    def get_provider_name(self) -> str:
        """Provider name"""
        return "custom_technical_indicators"

class AdditionalFundamentalProvider(DataEnhancementProvider):
    """Provider for additional fundamental data"""

    def __init__(self, api_key: str = None):
        self.api_key = api_key

    async def setup_session(self, session: aiohttp.ClientSession):
        """Setup HTTP session if needed"""
        self.session = session

    async def get_enhancement_data(self, ticker: str, existing_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Calculate additional fundamental metrics"""
        try:
            # Handle both fundamental_data and fundamental sections
            fund_data = existing_data.get('fundamental_data', {}) or existing_data.get('fundamental', {})
            if not fund_data:
                return None

            enhancements = {}

            # Determine target section based on existing data structure
            if 'fundamental_data' in existing_data:
                target_section = 'fundamental_data'
            elif 'fundamental' in existing_data:
                target_section = 'fundamental'
            else:
                target_section = 'fundamental_data'  # Default

            # Calculate additional ratios from existing fundamental data
            if target_section not in enhancements:
                enhancements[target_section] = {}

            # Example: Enterprise Value calculation (if we have market cap and debt)
            market_cap = fund_data.get('market_cap')
            debt_to_equity = fund_data.get('debt_to_equity')
            if market_cap and debt_to_equity:
                # Simplified EV calculation (market cap in millions, debt_to_equity in ratio)
                estimated_debt = market_cap * (debt_to_equity / 100) if debt_to_equity else 0
                enterprise_value = market_cap + estimated_debt
                enhancements[target_section]['enterprise_value'] = enterprise_value
                logger.debug(f"Calculated enterprise_value = {enterprise_value} for {ticker}")

            # Example: Price-to-Book ratio calculation
            book_value = fund_data.get('book_value')
            if market_cap and book_value and book_value > 0:
                # Approximate calculation (market cap in millions, book value per share)
                shares_outstanding = market_cap * 1_000_000 / book_value  # Convert market cap to actual value
                if shares_outstanding > 0:
                    price_per_share = (market_cap * 1_000_000) / shares_outstanding
                    pb_ratio = price_per_share / book_value
                    enhancements[target_section]['price_to_book'] = pb_ratio
                    logger.debug(f"Calculated price_to_book = {pb_ratio} for {ticker}")

            # Example: Current Ratio Quality Score (higher is better, capped at 3.0)
            current_ratio = fund_data.get('current_ratio')
            if current_ratio:
                quality_score = min(current_ratio / 2.0, 1.0)  # Normalize to 0-1, optimal around 2.0
                enhancements[target_section]['liquidity_quality'] = quality_score
                logger.debug(f"Calculated liquidity_quality = {quality_score} for {ticker}")

            return enhancements if enhancements else None

        except Exception as e:
            logger.warning(f"Could not calculate additional fundamentals for {ticker}: {e}")
            return None

    def get_target_sections(self) -> List[str]:
        """Additional fundamental provider targets fundamental_data or fundamental section"""
        return ['fundamental_data', 'fundamental']

    def get_provider_name(self) -> str:
        """Provider name"""
        return "additional_fundamentals"

class GenericDataMerger:
    """Generic data merger supporting multiple enhancement providers"""

    def __init__(self, providers: List[DataEnhancementProvider]):
        self.providers = providers
        self.session = None

        # Version tracking
        self.merge_version = f"v{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.merge_metadata = {
            "version": self.merge_version,
            "created_at": datetime.now().isoformat(),
            "operation": "generic_data_merge",
            "description": "Enhanced dataset with additional data points",
            "providers": [p.get_provider_name() for p in self.providers],
            "target_sections": list(set().union(*[p.get_target_sections() for p in self.providers])),
            "safety_measures": ["backup_created", "validation_checks", "non_destructive_merge"]
        }

        # Statistics tracking
        self.stats = {
            "files_processed": 0,
            "files_enhanced": 0,
            "files_skipped": 0,
            "api_calls_made": 0,
            "errors_encountered": 0,
            "enhancements_by_provider": {p.get_provider_name(): 0 for p in self.providers},
            "start_time": None,
            "end_time": None
        }

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            connector=aiohttp.TCPConnector(limit=100, limit_per_host=10)
        )

        # Setup sessions for all providers
        for provider in self.providers:
            await provider.setup_session(self.session)

        self.stats["start_time"] = datetime.now()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
        self.stats["end_time"] = datetime.now()

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

            # Validate critical data sections are unchanged or properly enhanced
            critical_sections = ['basic_data', 'metadata']

            for section in critical_sections:
                if section in original_data:
                    if section == 'metadata':
                        # Metadata can have additions but original fields must be preserved
                        orig_metadata = original_data[section]
                        new_metadata = enhanced_data.get(section, {})

                        for orig_key, orig_value in orig_metadata.items():
                            if orig_key not in new_metadata or new_metadata[orig_key] != orig_value:
                                logger.error(f"Original metadata field changed: {orig_key}")
                                return False
                    else:
                        # Other critical sections must be unchanged
                        if original_data[section] != enhanced_data.get(section):
                            logger.error(f"Data corruption detected in section: {section}")
                            return False

            # Validate enhanced sections have proper structure
            for section in ['technical_indicators', 'fundamental_data', 'company_data']:
                if section in enhanced_data:
                    if not isinstance(enhanced_data[section], dict):
                        logger.error(f"Invalid structure for section: {section}")
                        return False

            logger.debug("Data integrity validation passed")
            return True

        except Exception as e:
            logger.error(f"Validation error: {e}")
            return False

    def deep_merge_dict(self, base_dict: Dict, merge_dict: Dict) -> Dict:
        """Safely merge dictionaries, preserving existing data"""
        result = base_dict.copy()

        for key, value in merge_dict.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                # Recursively merge nested dictionaries
                result[key] = self.deep_merge_dict(result[key], value)
            else:
                # Add new key or overwrite with new value
                result[key] = value

        return result

    async def enhance_file(self, file_path: Path, ticker: str) -> bool:
        """Safely enhance a single file with data from all providers"""
        try:
            self.stats["files_processed"] += 1

            # Read original file
            with open(file_path, 'r') as f:
                original_data = json.load(f)

            # Create backup
            backup_path = self.create_backup(file_path)

            # Start with original data
            enhanced_data = original_data.copy()
            enhancements_applied = False

            # Apply enhancements from each provider
            for provider in self.providers:
                try:
                    enhancement_data = await provider.get_enhancement_data(ticker, original_data)

                    if enhancement_data:
                        # Check if this would add new data
                        would_enhance = False

                        for section, data in enhancement_data.items():
                            if section not in enhanced_data:
                                would_enhance = True
                                break
                            elif isinstance(data, dict) and isinstance(enhanced_data[section], dict):
                                for field in data.keys():
                                    if field not in enhanced_data[section]:
                                        would_enhance = True
                                        break

                        if would_enhance:
                            # Apply enhancement using deep merge
                            enhanced_data = self.deep_merge_dict(enhanced_data, enhancement_data)
                            enhancements_applied = True
                            self.stats["enhancements_by_provider"][provider.get_provider_name()] += 1
                            logger.debug(f"Applied enhancement from {provider.get_provider_name()} for {ticker}")

                except Exception as e:
                    logger.warning(f"Enhancement failed for provider {provider.get_provider_name()}: {e}")
                    continue

            # Skip if no enhancements were applied
            if not enhancements_applied:
                logger.debug(f"Skipping {file_path}: no new enhancements to apply")
                self.stats["files_skipped"] += 1
                return True

            # Add version tracking to metadata
            if 'metadata' not in enhanced_data:
                enhanced_data['metadata'] = {}

            enhanced_data['metadata']['data_version'] = self.merge_version
            enhanced_data['metadata']['last_enhanced'] = datetime.now().isoformat()
            enhanced_data['metadata']['enhancement_operation'] = "generic_data_merge"
            enhanced_data['metadata']['enhancement_providers'] = [p.get_provider_name() for p in self.providers]

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
        """Find files that need enhancement"""
        base_path = Path(base_path)
        files_with_tickers = []

        for file_path in base_path.glob(pattern):
            if file_path.is_file() and file_path.suffix == '.json':
                # Extract ticker from file path or file content
                try:
                    # Try to extract from path structure first
                    ticker = file_path.parts[-4]  # .../TICKER/YYYY/MM/YYYY-MM-DD.json
                    files_with_tickers.append((file_path, ticker))
                except IndexError:
                    # Try to extract from file content
                    try:
                        with open(file_path, 'r') as f:
                            data = json.load(f)
                            ticker = data.get('ticker')
                            if ticker:
                                files_with_tickers.append((file_path, ticker))
                            else:
                                logger.warning(f"Could not extract ticker from {file_path}")
                    except Exception:
                        logger.warning(f"Could not read ticker from {file_path}")
                        continue

        logger.info(f"Found {len(files_with_tickers)} files to potentially enhance")
        return files_with_tickers

    async def merge_enhancements(self, base_path: str, pattern: str = "**/*.json", batch_size: int = 50) -> Dict[str, Any]:
        """Main method to merge enhancements into existing dataset"""
        logger.info(f"Starting generic data merge - Version: {self.merge_version}")
        logger.info(f"Active providers: {[p.get_provider_name() for p in self.providers]}")

        # Find files to enhance
        files_to_process = self.find_files_to_enhance(base_path, pattern)

        if not files_to_process:
            logger.warning("No files found to enhance")
            return self.get_summary()

        # Group files by ticker to optimize API calls
        ticker_files = {}
        for file_path, ticker in files_to_process:
            if ticker not in ticker_files:
                ticker_files[ticker] = []
            ticker_files[ticker].append(file_path)

        logger.info(f"Processing {len(ticker_files)} unique tickers across {len(files_to_process)} files")

        # Process files in batches
        tickers = list(ticker_files.keys())
        for i in range(0, len(tickers), batch_size):
            batch_tickers = tickers[i:i + batch_size]

            logger.info(f"Processing batch {i//batch_size + 1}/{(len(tickers) + batch_size - 1)//batch_size}")

            # Process each ticker in the batch
            for ticker in batch_tickers:
                # Process all files for this ticker
                for file_path in ticker_files[ticker]:
                    await self.enhance_file(file_path, ticker)

            # Small delay between batches
            await asyncio.sleep(0.1)

        # Save merge metadata
        await self.save_merge_metadata(base_path)

        return self.get_summary()

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
            "operation": "generic_data_merge",
            "providers": [p.get_provider_name() for p in self.providers],
            "statistics": self.stats,
            "duration_seconds": duration,
            "success_rate": (self.stats["files_enhanced"] / self.stats["files_processed"] * 100) if self.stats["files_processed"] > 0 else 0
        }

# Example custom technical indicator functions
def bollinger_bandwidth(data: Dict[str, Any]) -> Optional[float]:
    """Calculate Bollinger Band Bandwidth"""
    try:
        tech = data.get('technical_indicators', {}) or data.get('technical', {})
        bb_upper = tech.get('bb_upper')
        bb_lower = tech.get('bb_lower')
        bb_middle = tech.get('bb_middle')

        if bb_upper and bb_lower and bb_middle and bb_middle > 0:
            bandwidth = (bb_upper - bb_lower) / bb_middle
            return bandwidth
        return None
    except:
        return None

def price_position(data: Dict[str, Any]) -> Optional[float]:
    """Calculate price position within Bollinger Bands (0-1 scale)"""
    try:
        # Handle both basic_data and root-level price data
        basic = data.get('basic_data', {})
        if not basic:
            # Fallback to root level for simpler data structures
            basic = data

        tech = data.get('technical_indicators', {}) or data.get('technical', {})

        close = basic.get('close')
        bb_upper = tech.get('bb_upper')
        bb_lower = tech.get('bb_lower')

        if close and bb_upper and bb_lower and bb_upper != bb_lower:
            position = (close - bb_lower) / (bb_upper - bb_lower)
            return max(0, min(1, position))  # Clamp to 0-1 range
        return None
    except:
        return None

async def main():
    """Main execution function"""
    import argparse

    parser = argparse.ArgumentParser(description='Generic data enhancement merger')
    parser.add_argument('base_path', help='Base path to search for JSON files')
    parser.add_argument('--pattern', default='**/*.json', help='File pattern to match')
    parser.add_argument('--batch-size', type=int, default=50, help='Batch size for processing')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be processed')
    parser.add_argument('--providers', nargs='+', default=['company'],
                       choices=['company', 'technical', 'fundamental', 'all'],
                       help='Enhancement providers to use')
    args = parser.parse_args()

    # Setup providers based on arguments
    providers = []

    if 'company' in args.providers or 'all' in args.providers:
        api_key = os.getenv('POLYGON_API_KEY')
        if api_key or args.dry_run:
            providers.append(PolygonCompanyDataProvider(api_key or "dummy"))

    if 'technical' in args.providers or 'all' in args.providers:
        # Add custom technical indicators
        custom_indicators = {
            'bollinger_bandwidth': bollinger_bandwidth,
            'price_position': price_position
        }
        providers.append(CustomTechnicalIndicatorProvider(custom_indicators))

    if 'fundamental' in args.providers or 'all' in args.providers:
        providers.append(AdditionalFundamentalProvider())

    if not providers:
        logger.error("No valid providers configured")
        sys.exit(1)

    if args.dry_run:
        logger.info("DRY RUN MODE - No files will be modified")
        merger = GenericDataMerger(providers)
        files_to_process = merger.find_files_to_enhance(args.base_path, args.pattern)

        if files_to_process:
            logger.info(f"Would process {len(files_to_process)} files with providers: {[p.get_provider_name() for p in providers]}")
            for file_path, ticker in files_to_process[:10]:
                logger.info(f"  {ticker}: {file_path}")
            if len(files_to_process) > 10:
                logger.info(f"  ... and {len(files_to_process) - 10} more files")
        else:
            logger.info("No files found to process")
        return

    # Execute merge operation
    async with GenericDataMerger(providers) as merger:
        summary = await merger.merge_enhancements(
            base_path=args.base_path,
            pattern=args.pattern,
            batch_size=args.batch_size
        )

        # Print summary
        logger.info("=== ENHANCEMENT OPERATION SUMMARY ===")
        logger.info(f"Version: {summary['version']}")
        logger.info(f"Providers: {summary['providers']}")
        logger.info(f"Files Processed: {summary['statistics']['files_processed']}")
        logger.info(f"Files Enhanced: {summary['statistics']['files_enhanced']}")
        logger.info(f"Files Skipped: {summary['statistics']['files_skipped']}")
        logger.info(f"Success Rate: {summary['success_rate']:.1f}%")
        if summary.get('duration_seconds'):
            logger.info(f"Duration: {summary['duration_seconds']:.1f} seconds")
        else:
            logger.info("Duration: N/A")

        for provider, count in summary['statistics']['enhancements_by_provider'].items():
            logger.info(f"  {provider}: {count} enhancements")

if __name__ == "__main__":
    asyncio.run(main())