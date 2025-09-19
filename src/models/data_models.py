from dataclasses import dataclass, field
from typing import Optional, Dict, Any
from datetime import datetime
import uuid


@dataclass
class TechnicalIndicators:
    rsi_14: Optional[float] = None
    macd_line: Optional[float] = None
    macd_signal: Optional[float] = None
    macd_histogram: Optional[float] = None
    sma_50: Optional[float] = None
    sma_200: Optional[float] = None
    ema_12: Optional[float] = None
    ema_26: Optional[float] = None
    bb_upper: Optional[float] = None
    bb_middle: Optional[float] = None
    bb_lower: Optional[float] = None
    atr_14: Optional[float] = None
    volatility: Optional[float] = None


@dataclass
class FundamentalData:
    market_cap: Optional[float] = None
    pe_ratio: Optional[float] = None
    debt_to_equity: Optional[float] = None
    roe_percent: Optional[float] = None
    current_ratio: Optional[float] = None
    operating_margin_percent: Optional[float] = None
    revenue_growth_percent: Optional[float] = None
    profit_margin_percent: Optional[float] = None
    dividend_yield_percent: Optional[float] = None
    book_value: Optional[float] = None


@dataclass
class RecordMetadata:
    record_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    collection_timestamp: datetime = field(default_factory=datetime.utcnow)
    data_source: str = "alpaca"
    collection_job_id: Optional[str] = None
    technical_indicators_calculated: bool = False
    fundamental_data_available: bool = False
    error_message: Optional[str] = None
    processing_status: str = "pending"  # pending, processing, completed, error


@dataclass
class StockDataRecord:
    record_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    ticker: str = ""
    date: str = ""  # YYYY-MM-DD format
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    close: float = 0.0
    volume: int = 0
    technical: TechnicalIndicators = field(default_factory=TechnicalIndicators)
    fundamental: Optional[FundamentalData] = None
    metadata: RecordMetadata = field(default_factory=RecordMetadata)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        result = {
            "record_id": self.record_id,
            "ticker": self.ticker,
            "date": self.date,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "technical": {
                "rsi_14": self.technical.rsi_14,
                "macd_line": self.technical.macd_line,
                "macd_signal": self.technical.macd_signal,
                "macd_histogram": self.technical.macd_histogram,
                "sma_50": self.technical.sma_50,
                "sma_200": self.technical.sma_200,
                "ema_12": self.technical.ema_12,
                "ema_26": self.technical.ema_26,
                "bb_upper": self.technical.bb_upper,
                "bb_middle": self.technical.bb_middle,
                "bb_lower": self.technical.bb_lower,
                "atr_14": self.technical.atr_14,
                "volatility": self.technical.volatility
            },
            "fundamental": {
                "market_cap": self.fundamental.market_cap if self.fundamental else None,
                "pe_ratio": self.fundamental.pe_ratio if self.fundamental else None,
                "debt_to_equity": self.fundamental.debt_to_equity if self.fundamental else None,
                "roe_percent": self.fundamental.roe_percent if self.fundamental else None,
                "current_ratio": self.fundamental.current_ratio if self.fundamental else None,
                "operating_margin_percent": self.fundamental.operating_margin_percent if self.fundamental else None,
                "revenue_growth_percent": self.fundamental.revenue_growth_percent if self.fundamental else None,
                "profit_margin_percent": self.fundamental.profit_margin_percent if self.fundamental else None,
                "dividend_yield_percent": self.fundamental.dividend_yield_percent if self.fundamental else None,
                "book_value": self.fundamental.book_value if self.fundamental else None
            } if self.fundamental else None,
            "metadata": {
                "record_id": self.metadata.record_id,
                "collection_timestamp": self.metadata.collection_timestamp.isoformat(),
                "data_source": self.metadata.data_source,
                "collection_job_id": self.metadata.collection_job_id,
                "technical_indicators_calculated": self.metadata.technical_indicators_calculated,
                "fundamental_data_available": self.metadata.fundamental_data_available,
                "error_message": self.metadata.error_message,
                "processing_status": self.metadata.processing_status
            }
        }
        return result


@dataclass
class CollectionJob:
    job_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    tickers: list[str] = field(default_factory=list)
    start_date: str = ""
    end_date: str = ""
    job_status: str = "pending"  # pending, running, completed, failed
    created_at: datetime = field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    total_records: int = 0
    successful_records: int = 0
    failed_records: int = 0
    error_summary: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            "job_id": self.job_id,
            "tickers": self.tickers,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "job_status": self.job_status,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "total_records": self.total_records,
            "successful_records": self.successful_records,
            "failed_records": self.failed_records,
            "error_summary": self.error_summary
        }