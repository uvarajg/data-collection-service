"""
Robust file writer service with JSON validation and integrity checks.
Prevents JSON corruption and ensures data integrity.
"""

import json
import hashlib
import asyncio
import aiofiles
from pathlib import Path
from typing import Dict, Any, Optional, Tuple
from datetime import datetime
import structlog

logger = structlog.get_logger()


class RobustFileWriter:
    """
    Bulletproof file writing service that prevents JSON corruption and data loss.
    
    Features:
    - JSON validation before write
    - File locking to prevent concurrent writes
    - Checksum verification
    - Atomic operations
    - Automatic retry on failure
    - Integrity verification after write
    """
    
    def __init__(self):
        self.logger = logger.bind(service="robust_file_writer")
        # File-specific locks to prevent concurrent writes to same file
        self._file_locks: Dict[str, asyncio.Lock] = {}
        self._write_stats = {
            "total_writes": 0,
            "successful_writes": 0,
            "failed_writes": 0,
            "retry_count": 0,
            "corruption_prevented": 0
        }
    
    def _get_file_lock(self, file_path: str) -> asyncio.Lock:
        """Get or create a lock for a specific file path."""
        if file_path not in self._file_locks:
            self._file_locks[file_path] = asyncio.Lock()
        return self._file_locks[file_path]
    
    def _calculate_checksum(self, data: str) -> str:
        """Calculate SHA256 checksum of data."""
        return hashlib.sha256(data.encode()).hexdigest()
    
    def _validate_json(self, data: Any) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Validate JSON data before writing.
        
        Returns:
            Tuple of (is_valid, json_string, error_message)
        """
        try:
            # If data is a string that looks like invalid JSON, reject it
            if isinstance(data, str):
                # Check for obvious corruption patterns
                if data.startswith("}") or data.endswith("{") or "}{" in data:
                    self.logger.error("Detected corrupted JSON string pattern", data=data[:50])
                    return False, None, "Corrupted JSON string detected"
            
            # Convert to JSON string
            json_str = json.dumps(data, indent=2, default=str)
            
            # Parse it back to validate structure
            parsed = json.loads(json_str)
            
            # Additional validation: check for common corruption patterns
            if json_str.count('}') != json_str.count('{'):
                return False, None, "Mismatched braces detected"
            
            # Check for duplicate keys (shouldn't happen with dict, but defensive)
            if '"metadata"' in json_str and json_str.count('"metadata"') > 1:
                return False, None, "Duplicate metadata sections detected"
            
            # Check for duplicate closing patterns that indicate corruption
            if '}  "' in json_str or '}\n}  "' in json_str:
                return False, None, "Duplicate closing brace pattern detected"
            
            return True, json_str, None
            
        except (json.JSONDecodeError, TypeError) as e:
            self.logger.error("JSON validation failed", error=str(e))
            return False, None, str(e)
    
    async def _verify_written_file(self, file_path: Path, original_checksum: str) -> bool:
        """
        Verify file integrity after writing.
        
        Args:
            file_path: Path to the written file
            original_checksum: Expected checksum
            
        Returns:
            True if file is valid and matches checksum
        """
        try:
            async with aiofiles.open(file_path, 'r') as f:
                content = await f.read()
            
            # Verify it's valid JSON
            parsed = json.loads(content)
            
            # Verify checksum matches
            file_checksum = self._calculate_checksum(content)
            if file_checksum != original_checksum:
                self.logger.error("Checksum mismatch after write",
                                file=str(file_path),
                                expected=original_checksum[:8],
                                actual=file_checksum[:8])
                return False
            
            return True
            
        except Exception as e:
            self.logger.error("File verification failed",
                            file=str(file_path), error=str(e))
            return False
    
    async def write_json_file(
        self, 
        file_path: Path, 
        data: Any,
        max_retries: int = 3,
        verify_after_write: bool = True
    ) -> bool:
        """
        Write JSON data to file with validation and integrity checks.
        
        Args:
            file_path: Target file path
            data: Data to write (dict or object with to_dict method)
            max_retries: Maximum retry attempts on failure
            verify_after_write: Whether to verify file after writing
            
        Returns:
            True if write was successful
        """
        self._write_stats["total_writes"] += 1
        
        # Get file-specific lock
        file_lock = self._get_file_lock(str(file_path))
        
        async with file_lock:
            # Convert data to dict if needed
            if hasattr(data, 'to_dict'):
                data_dict = data.to_dict()
            else:
                data_dict = data
            
            # Validate JSON before writing
            is_valid, json_str, error_msg = self._validate_json(data_dict)
            
            if not is_valid:
                self.logger.error("JSON validation failed, preventing corruption",
                                file=str(file_path), error=error_msg)
                self._write_stats["corruption_prevented"] += 1
                self._write_stats["failed_writes"] += 1
                return False
            
            # Calculate checksum for verification
            checksum = self._calculate_checksum(json_str)
            
            # Attempt to write with retries
            for attempt in range(max_retries):
                try:
                    # Ensure directory exists
                    file_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    # Write to temporary file first (atomic operation)
                    temp_path = file_path.with_suffix('.json.tmp')
                    
                    async with aiofiles.open(temp_path, 'w') as f:
                        await f.write(json_str)
                        # Force flush to disk
                        await f.flush()
                    
                    # Atomic rename
                    temp_path.rename(file_path)
                    
                    # Verify if requested
                    if verify_after_write:
                        if await self._verify_written_file(file_path, checksum):
                            self.logger.debug("File written and verified successfully",
                                           file=str(file_path),
                                           size=len(json_str),
                                           checksum=checksum[:8])
                            self._write_stats["successful_writes"] += 1
                            return True
                        else:
                            self.logger.warning("File verification failed, retrying",
                                             file=str(file_path),
                                             attempt=attempt + 1)
                            if attempt < max_retries - 1:
                                self._write_stats["retry_count"] += 1
                                await asyncio.sleep(0.1 * (attempt + 1))  # Exponential backoff
                                continue
                    else:
                        self._write_stats["successful_writes"] += 1
                        return True
                    
                except Exception as e:
                    self.logger.warning("Write attempt failed",
                                      file=str(file_path),
                                      attempt=attempt + 1,
                                      error=str(e))
                    
                    if attempt < max_retries - 1:
                        self._write_stats["retry_count"] += 1
                        await asyncio.sleep(0.1 * (attempt + 1))  # Exponential backoff
                    else:
                        self.logger.error("All write attempts failed",
                                        file=str(file_path),
                                        error=str(e))
                        self._write_stats["failed_writes"] += 1
                        return False
            
            self._write_stats["failed_writes"] += 1
            return False
    
    async def write_batch(
        self, 
        file_data_pairs: list[Tuple[Path, Any]],
        max_concurrent: int = 10
    ) -> Dict[str, int]:
        """
        Write multiple files with controlled concurrency.
        
        Args:
            file_data_pairs: List of (file_path, data) tuples
            max_concurrent: Maximum concurrent writes
            
        Returns:
            Dictionary with success/failure counts
        """
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def write_with_semaphore(file_path: Path, data: Any):
            async with semaphore:
                return await self.write_json_file(file_path, data)
        
        results = await asyncio.gather(
            *[write_with_semaphore(fp, data) for fp, data in file_data_pairs],
            return_exceptions=True
        )
        
        successful = sum(1 for r in results if r is True)
        failed = len(results) - successful
        
        self.logger.info("Batch write completed",
                       total=len(file_data_pairs),
                       successful=successful,
                       failed=failed)
        
        return {"successful": successful, "failed": failed}
    
    def get_stats(self) -> Dict[str, Any]:
        """Get write statistics."""
        return {
            **self._write_stats,
            "success_rate": (
                self._write_stats["successful_writes"] / 
                max(self._write_stats["total_writes"], 1) * 100
            ),
            "corruption_prevention_rate": (
                self._write_stats["corruption_prevented"] /
                max(self._write_stats["total_writes"], 1) * 100
            )
        }
    
    async def cleanup_locks(self):
        """Clean up file locks (call during shutdown)."""
        self._file_locks.clear()
        self.logger.info("File locks cleaned up")