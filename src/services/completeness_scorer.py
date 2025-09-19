"""
Data completeness scoring service for reliability assessment.
Tracks field coverage and provides visibility into data quality.
"""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass
import structlog

from ..models.data_models import StockDataRecord

logger = structlog.get_logger()


@dataclass
class CompletenessScore:
    """Data completeness score with detailed breakdown."""
    overall_score: float  # 0-100
    ohlcv_score: float  # Core price data score
    technical_score: float  # Technical indicators score
    fundamental_score: float  # Fundamental data score
    missing_fields: List[str]  # List of missing/null fields
    field_coverage: Dict[str, float]  # Per-field coverage percentage
    completeness_level: str  # 'excellent', 'good', 'fair', 'poor'


class CompletenessScorer:
    """
    Calculate data completeness scores for stock records.
    
    Provides detailed scoring and tracking of missing fields to improve
    data quality visibility and drive targeted improvements.
    """
    
    # Field importance weights
    WEIGHTS = {
        'ohlcv': 0.40,  # 40% - Critical price data
        'technical': 0.35,  # 35% - Important for trading
        'fundamental': 0.25  # 25% - Nice to have
    }
    
    # Individual field importance within categories
    FIELD_IMPORTANCE = {
        'ohlcv': {
            'open': 1.0,
            'high': 1.0,
            'low': 1.0,
            'close': 1.0,
            'volume': 0.8  # Slightly less critical
        },
        'technical': {
            'rsi_14': 1.0,
            'macd_line': 0.9,
            'macd_signal': 0.9,
            'macd_histogram': 0.8,
            'sma_50': 1.0,
            'sma_200': 0.7,  # Often missing, less critical
            'ema_12': 0.8,
            'ema_26': 0.8,
            'bb_upper': 0.7,
            'bb_middle': 0.7,
            'bb_lower': 0.7,
            'atr_14': 0.8,
            'volatility': 0.6
        },
        'fundamental': {
            'market_cap': 1.0,
            'pe_ratio': 0.9,
            'debt_to_equity': 0.7,
            'roe_percent': 0.8,
            'current_ratio': 0.7,
            'operating_margin_percent': 0.6,
            'revenue_growth_percent': 0.8,
            'profit_margin_percent': 0.7,
            'dividend_yield_percent': 0.5,  # Many stocks don't pay dividends
            'book_value': 0.6
        }
    }
    
    def __init__(self):
        self.logger = logger.bind(service="completeness_scorer")
        self._score_cache: Dict[str, CompletenessScore] = {}
    
    def calculate_score(self, record: StockDataRecord) -> CompletenessScore:
        """
        Calculate comprehensive completeness score for a record.
        
        Args:
            record: StockDataRecord to score
            
        Returns:
            CompletenessScore with detailed breakdown
        """
        # Check cache
        cache_key = f"{record.ticker}_{record.date}"
        if cache_key in self._score_cache:
            return self._score_cache[cache_key]
        
        missing_fields = []
        field_coverage = {}
        
        # Score OHLCV data (always present, check for validity)
        ohlcv_score, ohlcv_missing = self._score_ohlcv(record)
        missing_fields.extend(ohlcv_missing)
        
        # Score technical indicators
        technical_score, tech_missing = self._score_technical(record)
        missing_fields.extend(tech_missing)
        
        # Score fundamental data
        fundamental_score, fund_missing = self._score_fundamental(record)
        missing_fields.extend(fund_missing)
        
        # Calculate field coverage percentages
        field_coverage = {
            'ohlcv': ohlcv_score,
            'technical': technical_score,
            'fundamental': fundamental_score
        }
        
        # Calculate weighted overall score
        overall_score = (
            ohlcv_score * self.WEIGHTS['ohlcv'] +
            technical_score * self.WEIGHTS['technical'] +
            fundamental_score * self.WEIGHTS['fundamental']
        )
        
        # Determine completeness level
        if overall_score >= 95:
            completeness_level = 'excellent'
        elif overall_score >= 85:
            completeness_level = 'good'
        elif overall_score >= 70:
            completeness_level = 'fair'
        else:
            completeness_level = 'poor'
        
        score = CompletenessScore(
            overall_score=round(overall_score, 2),
            ohlcv_score=round(ohlcv_score, 2),
            technical_score=round(technical_score, 2),
            fundamental_score=round(fundamental_score, 2),
            missing_fields=missing_fields,
            field_coverage=field_coverage,
            completeness_level=completeness_level
        )
        
        # Cache the score
        self._score_cache[cache_key] = score
        
        return score
    
    def _score_ohlcv(self, record: StockDataRecord) -> tuple[float, List[str]]:
        """Score OHLCV data completeness."""
        missing = []
        total_weight = sum(self.FIELD_IMPORTANCE['ohlcv'].values())
        achieved_weight = 0
        
        # Check each OHLCV field
        if record.open is None or record.open <= 0:
            missing.append('open')
        else:
            achieved_weight += self.FIELD_IMPORTANCE['ohlcv']['open']
        
        if record.high is None or record.high <= 0:
            missing.append('high')
        else:
            achieved_weight += self.FIELD_IMPORTANCE['ohlcv']['high']
        
        if record.low is None or record.low <= 0:
            missing.append('low')
        else:
            achieved_weight += self.FIELD_IMPORTANCE['ohlcv']['low']
        
        if record.close is None or record.close <= 0:
            missing.append('close')
        else:
            achieved_weight += self.FIELD_IMPORTANCE['ohlcv']['close']
        
        if record.volume is None or record.volume < 0:
            missing.append('volume')
        else:
            achieved_weight += self.FIELD_IMPORTANCE['ohlcv']['volume']
        
        score = (achieved_weight / total_weight) * 100 if total_weight > 0 else 0
        return score, missing
    
    def _score_technical(self, record: StockDataRecord) -> tuple[float, List[str]]:
        """Score technical indicators completeness."""
        missing = []
        total_weight = sum(self.FIELD_IMPORTANCE['technical'].values())
        achieved_weight = 0
        
        if not record.technical:
            return 0, ['all_technical_indicators']
        
        tech = record.technical
        
        # Check each technical indicator
        indicators = {
            'rsi_14': tech.rsi_14,
            'macd_line': tech.macd_line,
            'macd_signal': tech.macd_signal,
            'macd_histogram': tech.macd_histogram,
            'sma_50': tech.sma_50,
            'sma_200': tech.sma_200,
            'ema_12': tech.ema_12,
            'ema_26': tech.ema_26,
            'bb_upper': tech.bb_upper,
            'bb_middle': tech.bb_middle,
            'bb_lower': tech.bb_lower,
            'atr_14': tech.atr_14,
            'volatility': tech.volatility
        }
        
        for name, value in indicators.items():
            if value is None:
                missing.append(name)
            else:
                achieved_weight += self.FIELD_IMPORTANCE['technical'][name]
        
        score = (achieved_weight / total_weight) * 100 if total_weight > 0 else 0
        return score, missing
    
    def _score_fundamental(self, record: StockDataRecord) -> tuple[float, List[str]]:
        """Score fundamental data completeness."""
        missing = []
        total_weight = sum(self.FIELD_IMPORTANCE['fundamental'].values())
        achieved_weight = 0
        
        if not record.fundamental:
            return 0, ['all_fundamental_data']
        
        fund = record.fundamental
        
        # Check each fundamental field
        fundamentals = {
            'market_cap': fund.market_cap,
            'pe_ratio': fund.pe_ratio,
            'debt_to_equity': fund.debt_to_equity,
            'roe_percent': fund.roe_percent,
            'current_ratio': fund.current_ratio,
            'operating_margin_percent': fund.operating_margin_percent,
            'revenue_growth_percent': fund.revenue_growth_percent,
            'profit_margin_percent': fund.profit_margin_percent,
            'dividend_yield_percent': fund.dividend_yield_percent,
            'book_value': fund.book_value
        }
        
        for name, value in fundamentals.items():
            if value is None:
                missing.append(name)
            else:
                achieved_weight += self.FIELD_IMPORTANCE['fundamental'][name]
        
        score = (achieved_weight / total_weight) * 100 if total_weight > 0 else 0
        return score, missing
    
    def score_batch(self, records: List[StockDataRecord]) -> Dict[str, Any]:
        """
        Score a batch of records and provide aggregate statistics.
        
        Args:
            records: List of StockDataRecord objects
            
        Returns:
            Dictionary with aggregate scores and statistics
        """
        if not records:
            return {
                'total_records': 0,
                'average_score': 0,
                'completeness_distribution': {}
            }
        
        scores = [self.calculate_score(record) for record in records]
        
        # Calculate statistics
        avg_overall = sum(s.overall_score for s in scores) / len(scores)
        avg_ohlcv = sum(s.ohlcv_score for s in scores) / len(scores)
        avg_technical = sum(s.technical_score for s in scores) / len(scores)
        avg_fundamental = sum(s.fundamental_score for s in scores) / len(scores)
        
        # Count completeness levels
        level_counts = {
            'excellent': sum(1 for s in scores if s.completeness_level == 'excellent'),
            'good': sum(1 for s in scores if s.completeness_level == 'good'),
            'fair': sum(1 for s in scores if s.completeness_level == 'fair'),
            'poor': sum(1 for s in scores if s.completeness_level == 'poor')
        }
        
        # Find most commonly missing fields
        all_missing = []
        for score in scores:
            all_missing.extend(score.missing_fields)
        
        missing_frequency = {}
        for field in all_missing:
            missing_frequency[field] = missing_frequency.get(field, 0) + 1
        
        # Sort by frequency
        top_missing = sorted(missing_frequency.items(), key=lambda x: x[1], reverse=True)[:10]
        
        return {
            'total_records': len(records),
            'average_score': round(avg_overall, 2),
            'average_ohlcv_score': round(avg_ohlcv, 2),
            'average_technical_score': round(avg_technical, 2),
            'average_fundamental_score': round(avg_fundamental, 2),
            'completeness_distribution': level_counts,
            'top_missing_fields': top_missing,
            'improvement_potential': round(100 - avg_overall, 2)
        }
    
    def add_completeness_to_metadata(self, record: StockDataRecord) -> StockDataRecord:
        """
        Add completeness score to record metadata.
        
        Args:
            record: StockDataRecord to enhance
            
        Returns:
            Record with completeness score in metadata
        """
        score = self.calculate_score(record)
        
        # Add to metadata
        if not hasattr(record.metadata, 'completeness_score'):
            record.metadata.__dict__['completeness_score'] = score.overall_score
            record.metadata.__dict__['completeness_level'] = score.completeness_level
            record.metadata.__dict__['missing_fields_count'] = len(score.missing_fields)
        
        return record
    
    def get_improvement_recommendations(self, records: List[StockDataRecord]) -> List[str]:
        """
        Analyze records and provide targeted improvement recommendations.
        
        Args:
            records: List of records to analyze
            
        Returns:
            List of actionable recommendations
        """
        stats = self.score_batch(records)
        recommendations = []
        
        # Check overall score
        if stats['average_score'] < 70:
            recommendations.append("🔴 Critical: Overall completeness below 70% - immediate attention required")
        elif stats['average_score'] < 85:
            recommendations.append("🟡 Warning: Overall completeness below 85% - improvements needed")
        
        # Check category scores
        if stats['average_ohlcv_score'] < 95:
            recommendations.append("📊 Investigate OHLCV data collection - core price data incomplete")
        
        if stats['average_technical_score'] < 80:
            recommendations.append("📈 Technical indicators need attention - consider extending historical data fetch")
        
        if stats['average_fundamental_score'] < 60:
            recommendations.append("📋 Fundamental data coverage low - consider additional data sources")
        
        # Check top missing fields
        if stats['top_missing_fields']:
            top_field = stats['top_missing_fields'][0]
            if top_field[1] > len(records) * 0.5:  # Missing in >50% of records
                recommendations.append(f"⚠️ Field '{top_field[0]}' missing in {top_field[1]} records - investigate data source")
        
        # Check for SMA_200 specifically
        missing_sma200 = sum(1 for r in records if r.technical and r.technical.sma_200 is None)
        if missing_sma200 > len(records) * 0.3:
            recommendations.append(f"📊 SMA_200 missing in {missing_sma200}/{len(records)} records - extend historical data to 250+ days")
        
        return recommendations