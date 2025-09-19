"""
Technical Indicator Validator Service

Validates technical indicators are within expected bounds and logs violations.
Invalid records are moved to error_records to prevent corrupted data in the valid dataset.
"""

import structlog
import numpy as np
from typing import List, Optional, Dict, Tuple, Any
from datetime import datetime
from pathlib import Path
import json

from ..models.data_models import StockDataRecord, TechnicalIndicators

logger = structlog.get_logger()


class TechnicalIndicatorValidator:
    """
    Validates technical indicators for data quality.
    Ensures all indicators are within expected bounds.
    """
    
    # Define valid bounds for each indicator
    INDICATOR_BOUNDS = {
        'rsi_14': (0.0, 100.0),  # RSI must be between 0 and 100
        'macd_line': (-50.0, 50.0),  # MACD typically within ±50
        'macd_signal': (-50.0, 50.0),  # MACD signal typically within ±50
        'macd_histogram': (-25.0, 25.0),  # MACD histogram smaller range
        'sma_50': (0.0, float('inf')),  # Must be positive
        'sma_200': (0.0, float('inf')),  # Must be positive
        'ema_12': (0.0, float('inf')),  # Must be positive
        'ema_26': (0.0, float('inf')),  # Must be positive
        'bb_upper': (0.0, float('inf')),  # Must be positive
        'bb_middle': (0.0, float('inf')),  # Must be positive
        'bb_lower': (0.0, float('inf')),  # Must be positive
        'atr_14': (0.0, float('inf')),  # Must be positive
        'volatility': (0.0, 100.0)  # Volatility as percentage
    }
    
    # Define reasonable price-relative bounds (RELAXED THRESHOLDS for production)
    PRICE_RELATIVE_BOUNDS = {
        'sma_50': (0.4, 2.5),  # Within 40-250% of current price (relaxed)
        'sma_200': (0.2, 4.0),  # Within 20-400% of current price (relaxed)
        'ema_12': (0.7, 1.4),  # Within 70-140% of current price (relaxed)
        'ema_26': (0.6, 1.5),  # Within 60-150% of current price (relaxed)
        'bb_upper': (0.9, 2.0),  # Upper band 90-200% of price (much more flexible)
        'bb_middle': (0.7, 1.4),  # Middle band 70-140% of price (FIXED: was too strict)
        'bb_lower': (0.3, 1.2),  # Lower band 30-120% of price (much more flexible)
    }
    
    def __init__(self):
        self.logger = logger.bind(service="technical_indicator_validator")
        self.violation_log = []
        self.correction_frequency = {}
        
    def validate_record(self, record: StockDataRecord) -> Tuple[bool, List[str], Dict[str, Any]]:
        """
        Validate technical indicators for a single record.
        
        Args:
            record: Stock data record with technical indicators
            
        Returns:
            Tuple of (is_valid, error_messages, violations_dict)
        """
        if not record.technical:
            return True, [], {}  # No indicators to validate
            
        violations = []
        violation_details = {}
        tech = record.technical
        
        # Check absolute bounds
        for indicator, (min_val, max_val) in self.INDICATOR_BOUNDS.items():
            value = getattr(tech, indicator, None)
            if value is not None and not np.isnan(value):
                if value < min_val or value > max_val:
                    violations.append(f"{indicator}={value:.2f} outside bounds [{min_val}, {max_val}]")
                    violation_details[indicator] = {
                        'value': value,
                        'min_bound': min_val,
                        'max_bound': max_val,
                        'type': 'absolute_bounds'
                    }
        
        # Check price-relative bounds for moving averages and Bollinger Bands
        if record.close and record.close > 0:
            for indicator, (min_ratio, max_ratio) in self.PRICE_RELATIVE_BOUNDS.items():
                value = getattr(tech, indicator, None)
                if value is not None and not np.isnan(value):
                    ratio = value / record.close
                    if ratio < min_ratio or ratio > max_ratio:
                        violations.append(f"{indicator}={value:.2f} unusual ratio {ratio:.2f} to price {record.close:.2f}")
                        violation_details[indicator] = {
                            'value': value,
                            'price': record.close,
                            'ratio': ratio,
                            'min_ratio': min_ratio,
                            'max_ratio': max_ratio,
                            'type': 'price_relative'
                        }
        
        # Check for NaN or Inf values
        for field in tech.__dict__:
            if not field.startswith('_'):
                value = getattr(tech, field, None)
                if value is not None:
                    if np.isnan(value):
                        violations.append(f"{field} is NaN")
                        violation_details[field] = {'type': 'nan_value'}
                    elif np.isinf(value):
                        violations.append(f"{field} is Inf")
                        violation_details[field] = {'type': 'inf_value'}
        
        # Check Bollinger Band logic (upper > middle > lower)
        if tech.bb_upper and tech.bb_middle and tech.bb_lower:
            if not (tech.bb_upper > tech.bb_middle > tech.bb_lower):
                violations.append(f"Bollinger Band order violation: upper={tech.bb_upper:.2f}, middle={tech.bb_middle:.2f}, lower={tech.bb_lower:.2f}")
                violation_details['bollinger_bands'] = {
                    'upper': tech.bb_upper,
                    'middle': tech.bb_middle,
                    'lower': tech.bb_lower,
                    'type': 'logical_error'
                }
        
        # Check SMA logic (SMA_200 should be smoother than SMA_50)
        if tech.volatility and tech.volatility > 50:  # High volatility stock
            if tech.sma_50 and tech.sma_200:
                diff_ratio = abs(tech.sma_50 - tech.sma_200) / tech.sma_200
                if diff_ratio > 0.5:  # More than 50% difference
                    self.logger.warning(
                        "Large SMA divergence detected",
                        ticker=record.ticker,
                        date=record.date,
                        sma_50=tech.sma_50,
                        sma_200=tech.sma_200,
                        diff_ratio=diff_ratio
                    )
        
        is_valid = len(violations) == 0
        
        # Log violations
        if violations:
            self.logger.warning(
                "Technical indicator validation failed",
                ticker=record.ticker,
                date=record.date,
                violations=violations,
                violation_count=len(violations)
            )
            
            # Track violation frequency
            for violation in violations:
                indicator_name = violation.split('=')[0] if '=' in violation else violation.split()[0]
                self.correction_frequency[indicator_name] = self.correction_frequency.get(indicator_name, 0) + 1
            
            # Add to violation log
            self.violation_log.append({
                'timestamp': datetime.now().isoformat(),
                'ticker': record.ticker,
                'date': record.date,
                'violations': violations,
                'details': violation_details
            })
        
        return is_valid, violations, violation_details
    
    def validate_batch(self, records: List[StockDataRecord]) -> Tuple[List[StockDataRecord], List[StockDataRecord]]:
        """
        Validate a batch of records and separate valid from invalid.
        
        Args:
            records: List of stock data records to validate
            
        Returns:
            Tuple of (valid_records, invalid_records)
        """
        valid_records = []
        invalid_records = []
        
        for record in records:
            is_valid, violations, details = self.validate_record(record)
            
            if is_valid:
                valid_records.append(record)
            else:
                # Add validation error to metadata
                if not record.metadata:
                    record.metadata = {}
                
                record.metadata.validation_errors = violations
                record.metadata.validation_details = details
                record.metadata.validation_failed_at = datetime.now().isoformat()
                
                invalid_records.append(record)
        
        self.logger.info(
            "Batch validation completed",
            total_records=len(records),
            valid_count=len(valid_records),
            invalid_count=len(invalid_records),
            validation_rate=f"{len(valid_records)/len(records)*100:.1f}%" if records else "0%"
        )
        
        return valid_records, invalid_records
    
    def correct_indicators(self, record: StockDataRecord, force: bool = False) -> StockDataRecord:
        """
        Attempt to correct invalid indicators by clamping to valid bounds.
        
        Args:
            record: Record with potentially invalid indicators
            force: If True, apply corrections even if validation passes
            
        Returns:
            Record with corrected indicators
        """
        if not record.technical:
            return record
            
        is_valid, violations, details = self.validate_record(record)
        
        if is_valid and not force:
            return record
        
        tech = record.technical
        corrections_made = []
        
        # Apply corrections for absolute bounds
        for indicator, (min_val, max_val) in self.INDICATOR_BOUNDS.items():
            value = getattr(tech, indicator, None)
            if value is not None and not np.isnan(value):
                if value < min_val:
                    setattr(tech, indicator, min_val)
                    corrections_made.append(f"{indicator}: {value:.2f} -> {min_val}")
                elif value > max_val:
                    setattr(tech, indicator, max_val)
                    corrections_made.append(f"{indicator}: {value:.2f} -> {max_val}")
        
        # Fix Bollinger Band logic if needed
        if tech.bb_upper and tech.bb_middle and tech.bb_lower:
            if not (tech.bb_upper > tech.bb_middle > tech.bb_lower):
                # Recalculate based on middle band
                if tech.bb_middle:
                    band_width = abs(tech.bb_upper - tech.bb_lower) / 4  # Estimate
                    tech.bb_upper = tech.bb_middle + band_width
                    tech.bb_lower = tech.bb_middle - band_width
                    corrections_made.append("Bollinger Bands reordered")
        
        if corrections_made:
            self.logger.info(
                "Technical indicators corrected",
                ticker=record.ticker,
                date=record.date,
                corrections=corrections_made,
                correction_count=len(corrections_made)
            )
            
            # Update metadata
            if not record.metadata:
                record.metadata = {}
            record.metadata.indicators_corrected = True
            record.metadata.corrections_applied = corrections_made
        
        return record
    
    def move_to_error_records(self, invalid_records: List[StockDataRecord], job_id: str):
        """
        Move invalid records to error_records directory.
        
        Args:
            invalid_records: List of records that failed validation
            job_id: Job ID for tracking
        """
        if not invalid_records:
            return
        
        base_path = Path("/workspaces/data/error_records/technical_validation")
        
        for record in invalid_records:
            # Create path: error_records/technical_validation/{ticker}/{date}.json
            ticker_path = base_path / record.ticker
            ticker_path.mkdir(parents=True, exist_ok=True)
            
            file_path = ticker_path / f"{record.date}.json"
            
            # Prepare error record
            # Convert StockDataRecord to dict using dataclasses.asdict
            from dataclasses import asdict
            error_data = {
                'original_record': asdict(record),
                'validation_errors': getattr(record.metadata, 'validation_errors', []),
                'validation_details': getattr(record.metadata, 'validation_details', {}),
                'job_id': job_id,
                'moved_at': datetime.now().isoformat(),
                'reason': 'technical_indicator_validation_failed'
            }
            
            # Write to error records
            with open(file_path, 'w') as f:
                json.dump(error_data, f, indent=2, default=str)
            
            self.logger.info(
                "Invalid record moved to error_records",
                ticker=record.ticker,
                date=record.date,
                file=str(file_path),
                errors=len(getattr(record.metadata, 'validation_errors', []))
            )
    
    def get_validation_report(self) -> Dict[str, Any]:
        """
        Get a summary report of validation activities.
        
        Returns:
            Dictionary with validation statistics
        """
        return {
            'total_violations': len(self.violation_log),
            'correction_frequency': dict(self.correction_frequency),
            'recent_violations': self.violation_log[-10:],  # Last 10 violations
            'most_common_issue': max(self.correction_frequency.items(), key=lambda x: x[1])[0] 
                if self.correction_frequency else None,
            'timestamp': datetime.now().isoformat()
        }
    
    def should_reject_record(self, record: StockDataRecord, strict: bool = True) -> bool:
        """
        Determine if a record should be rejected entirely.
        
        Args:
            record: Record to evaluate
            strict: If True, reject on any violation. If False, only reject critical errors.
            
        Returns:
            True if record should be rejected
        """
        is_valid, violations, details = self.validate_record(record)
        
        if strict:
            return not is_valid
        
        # In non-strict mode, only reject for critical errors
        critical_errors = [
            'nan_value',
            'inf_value', 
            'logical_error'
        ]
        
        for detail in details.values():
            if detail.get('type') in critical_errors:
                return True
        
        # Also reject if RSI is way out of bounds (indicates calculation error)
        if record.technical and record.technical.rsi_14:
            if record.technical.rsi_14 < -10 or record.technical.rsi_14 > 110:
                return True
        
        return False