import structlog
import numpy as np
import pandas as pd
import ta
from typing import List, Optional, Dict, Any
from ..models.data_models import StockDataRecord, TechnicalIndicators

logger = structlog.get_logger()


class TechnicalIndicatorsService:
    """
    Technical indicators calculation service using TA-Lib.
    Replicates the functionality from AlgoAlchemist's technical indicators.
    """
    
    def __init__(self):
        self.logger = logger.bind(service="technical_indicators")
    
    async def calculate_indicators_with_history(
        self, 
        ticker: str, 
        current_records: List[StockDataRecord],
        alpaca_service
    ) -> List[StockDataRecord]:
        """
        Calculate technical indicators by fetching additional historical data if needed.
        This matches AlgoAlchemist's approach of getting 200+ days of history.
        
        Args:
            ticker: Stock symbol
            current_records: Current records that need indicators
            alpaca_service: Alpaca service instance for fetching more data
            
        Returns:
            List of current records with calculated technical indicators
        """
        if not current_records:
            return current_records
        
        # Get earliest date from current records
        earliest_date = min(record.date for record in current_records)
        
        # Calculate start date for historical data (250 days back like AlgoAlchemist)
        from datetime import datetime, timedelta
        earliest_dt = datetime.strptime(earliest_date, "%Y-%m-%d")
        start_date = (earliest_dt - timedelta(days=250)).strftime("%Y-%m-%d")
        
        self.logger.info(f"Fetching 250 days of historical data for {ticker} to calculate indicators")
        
        # Fetch comprehensive historical data
        historical_records = await alpaca_service.get_daily_bars(
            ticker=ticker,
            start_date=start_date,
            end_date=earliest_date
        )
        
        # Combine historical + current records
        all_records = historical_records + current_records
        all_records = sorted(all_records, key=lambda r: r.date)
        
        self.logger.info(f"Using {len(all_records)} total records for technical indicators calculation")
        
        # Calculate indicators on the full dataset
        all_records_with_indicators = self.calculate_indicators(all_records)
        
        # Return only the current records with indicators
        result_records = []
        current_dates = {record.date for record in current_records}
        
        for record in all_records_with_indicators:
            if record.date in current_dates:
                result_records.append(record)
        
        return sorted(result_records, key=lambda r: r.date)
    
    def calculate_indicators(self, records: List[StockDataRecord]) -> List[StockDataRecord]:
        """
        Calculate technical indicators for a list of stock data records.
        
        Args:
            records: List of StockDataRecord objects with OHLCV data
            
        Returns:
            List of StockDataRecord objects with populated technical indicators
        """
        if not records or len(records) < 50:  # Need at least 50 periods for indicators
            self.logger.warning("Insufficient data for technical indicators", record_count=len(records))
            return records
        
        # CRITICAL: Sort records by date to ensure proper chronological order
        # Technical indicators are extremely sensitive to data sequence
        records = sorted(records, key=lambda r: r.date)
        
        # Verify chronological order (defensive check)
        dates = [r.date for r in records]
        for i in range(1, len(dates)):
            if dates[i] < dates[i-1]:
                self.logger.error("Critical error: Data not in chronological order!", 
                                previous_date=dates[i-1], current_date=dates[i], 
                                index=i, ticker=records[0].ticker if records else "unknown")
                raise ValueError(f"Data sequence error: {dates[i-1]} followed by {dates[i]}")
        
        self.logger.debug("Data verified in chronological order", 
                         first_date=dates[0] if dates else None,
                         last_date=dates[-1] if dates else None,
                         record_count=len(records))
        
        # Extract OHLCV data into numpy arrays
        close_prices = np.array([r.close for r in records])
        high_prices = np.array([r.high for r in records])
        low_prices = np.array([r.low for r in records])
        volume_data = np.array([r.volume for r in records])
        
        # Calculate all technical indicators
        indicators = self._calculate_all_indicators(
            close_prices, high_prices, low_prices, volume_data
        )
        
        # Populate indicators back into records
        for i, record in enumerate(records):
            record.technical = TechnicalIndicators(
                rsi_14=self._safe_get_indicator(indicators['rsi_14'], i),
                macd_line=self._safe_get_indicator(indicators['macd_line'], i),
                macd_signal=self._safe_get_indicator(indicators['macd_signal'], i),
                macd_histogram=self._safe_get_indicator(indicators['macd_histogram'], i),
                sma_50=self._safe_get_indicator(indicators['sma_50'], i),
                sma_200=self._safe_get_indicator(indicators['sma_200'], i),
                ema_12=self._safe_get_indicator(indicators['ema_12'], i),
                ema_26=self._safe_get_indicator(indicators['ema_26'], i),
                bb_upper=self._safe_get_indicator(indicators['bb_upper'], i),
                bb_middle=self._safe_get_indicator(indicators['bb_middle'], i),
                bb_lower=self._safe_get_indicator(indicators['bb_lower'], i),
                atr_14=self._safe_get_indicator(indicators['atr_14'], i),
                volatility=self._safe_get_indicator(indicators['volatility'], i)
            )
            
            # Update metadata to indicate technical indicators are calculated
            record.metadata.technical_indicators_calculated = True
            record.metadata.processing_status = "indicators_calculated"
        
        self.logger.info("Technical indicators calculated", 
                        ticker=records[0].ticker if records else "unknown",
                        record_count=len(records))
        
        return records
    
    def _calculate_all_indicators(
        self, 
        close: np.ndarray, 
        high: np.ndarray, 
        low: np.ndarray, 
        volume: np.ndarray
    ) -> Dict[str, np.ndarray]:
        """Calculate all technical indicators using TA library"""
        
        indicators = {}
        
        try:
            # Create DataFrame for TA library
            df = pd.DataFrame({
                'close': close,
                'high': high,
                'low': low,
                'volume': volume
            })
            
            # RSI (14-period)
            indicators['rsi_14'] = ta.momentum.RSIIndicator(df['close'], window=14).rsi().values
            
            # MACD (12, 26, 9)
            macd = ta.trend.MACD(df['close'], window_fast=12, window_slow=26, window_sign=9)
            indicators['macd_line'] = macd.macd().values
            indicators['macd_signal'] = macd.macd_signal().values
            indicators['macd_histogram'] = macd.macd_diff().values
            
            # Simple Moving Averages
            indicators['sma_50'] = ta.trend.SMAIndicator(df['close'], window=50).sma_indicator().values
            indicators['sma_200'] = ta.trend.SMAIndicator(df['close'], window=200).sma_indicator().values
            
            # Exponential Moving Averages
            indicators['ema_12'] = ta.trend.EMAIndicator(df['close'], window=12).ema_indicator().values
            indicators['ema_26'] = ta.trend.EMAIndicator(df['close'], window=26).ema_indicator().values
            
            # Bollinger Bands (20-period, 2 standard deviations)
            bb = ta.volatility.BollingerBands(df['close'], window=20, window_dev=2)
            indicators['bb_upper'] = bb.bollinger_hband().values
            indicators['bb_middle'] = bb.bollinger_mavg().values
            indicators['bb_lower'] = bb.bollinger_lband().values
            
            # Average True Range (14-period)
            indicators['atr_14'] = ta.volatility.AverageTrueRange(df['high'], df['low'], df['close'], window=14).average_true_range().values
            
            # Volatility calculation (20-period standard deviation)
            indicators['volatility'] = df['close'].rolling(window=20).std().values
            
            self.logger.debug("All technical indicators calculated successfully")
            
        except Exception as e:
            self.logger.error("Error calculating technical indicators", error=str(e))
            # Return empty arrays if calculation fails
            array_length = len(close)
            for key in ['rsi_14', 'macd_line', 'macd_signal', 'macd_histogram', 
                       'sma_50', 'sma_200', 'ema_12', 'ema_26', 
                       'bb_upper', 'bb_middle', 'bb_lower', 'atr_14', 'volatility']:
                indicators[key] = np.full(array_length, np.nan)
        
        return indicators
    
    def _safe_get_indicator(self, indicator_array: np.ndarray, index: int) -> Optional[float]:
        """Safely get indicator value, handling NaN and out-of-bounds"""
        try:
            if index < len(indicator_array):
                value = indicator_array[index]
                if np.isnan(value) or np.isinf(value):
                    return None
                return float(value)
            return None
        except (IndexError, ValueError, TypeError):
            return None
    
    def calculate_single_record_indicators(
        self, 
        records: List[StockDataRecord], 
        target_record: StockDataRecord
    ) -> StockDataRecord:
        """
        Calculate indicators for a single record using historical data.
        Used when adding new data to existing time series.
        
        Args:
            records: Historical records for context (sorted by date)
            target_record: The record to calculate indicators for
            
        Returns:
            Target record with calculated indicators
        """
        # Find the target record in the list
        all_records = records + [target_record]
        all_records = sorted(all_records, key=lambda r: r.date)
        
        # Calculate indicators for all records
        records_with_indicators = self.calculate_indicators(all_records)
        
        # Find and return the target record with indicators
        for record in records_with_indicators:
            if (record.ticker == target_record.ticker and 
                record.date == target_record.date):
                return record
        
        # If not found, return original record
        return target_record
    
    def get_indicator_summary(self, record: StockDataRecord) -> Dict[str, Any]:
        """
        Get a summary of technical indicators for a record.
        
        Args:
            record: StockDataRecord with technical indicators
            
        Returns:
            Dictionary with indicator summary
        """
        if not record.technical:
            return {"status": "no_indicators"}
        
        tech = record.technical
        
        # RSI interpretation
        rsi_signal = "neutral"
        if tech.rsi_14:
            if tech.rsi_14 > 70:
                rsi_signal = "overbought"
            elif tech.rsi_14 < 30:
                rsi_signal = "oversold"
        
        # MACD interpretation
        macd_signal = "neutral"
        if tech.macd_line and tech.macd_signal:
            if tech.macd_line > tech.macd_signal:
                macd_signal = "bullish"
            else:
                macd_signal = "bearish"
        
        # Moving averages trend
        trend_signal = "neutral"
        if tech.sma_50 and tech.sma_200:
            if tech.sma_50 > tech.sma_200:
                trend_signal = "uptrend"
            else:
                trend_signal = "downtrend"
        
        return {
            "status": "calculated",
            "rsi_14": tech.rsi_14,
            "rsi_signal": rsi_signal,
            "macd_signal": macd_signal,
            "trend_signal": trend_signal,
            "volatility": tech.volatility,
            "atr_14": tech.atr_14,
            "bollinger_position": self._get_bollinger_position(record.close, tech)
        }
    
    def _get_bollinger_position(self, close_price: float, tech: TechnicalIndicators) -> str:
        """Determine position relative to Bollinger Bands"""
        if not (tech.bb_upper and tech.bb_lower):
            return "unknown"
        
        if close_price > tech.bb_upper:
            return "above_upper"
        elif close_price < tech.bb_lower:
            return "below_lower"
        else:
            return "within_bands"
    
    async def calculate_sma_200_with_fallback(
        self, 
        ticker: str, 
        records: List[StockDataRecord],
        yfinance_service=None
    ) -> Optional[float]:
        """
        Calculate SMA_200 with fallback options:
        1. Calculate from historical data if we have 200+ days
        2. Fetch from Yahoo Finance if calculation fails
        3. Return None if all sources fail
        
        Args:
            ticker: Stock symbol
            records: Historical price records
            yfinance_service: Optional YFinance service instance for fallback
            
        Returns:
            SMA_200 value or None if unavailable
        """
        self.logger.debug(f"Attempting to get SMA_200 for {ticker} with {len(records)} records")
        
        # Attempt 1: Calculate from historical data
        if len(records) >= 200:
            try:
                # Sort records by date
                sorted_records = sorted(records, key=lambda r: r.date)
                close_prices = [r.close for r in sorted_records[-200:]]  # Last 200 days
                
                if len(close_prices) == 200:
                    sma_200 = np.mean(close_prices)
                    
                    if sma_200 and not np.isnan(sma_200) and sma_200 > 0:
                        self.logger.info(f"SMA_200 calculated from historical data for {ticker}: {sma_200:.2f}")
                        return float(sma_200)
            except Exception as e:
                self.logger.warning(f"Failed to calculate SMA_200 for {ticker}: {e}")
        else:
            self.logger.debug(f"Insufficient data for SMA_200 calculation: {len(records)} records (need 200)")
        
        # Attempt 2: Fetch from Yahoo Finance if service is provided
        if yfinance_service:
            try:
                self.logger.info(f"Attempting Yahoo Finance fallback for SMA_200 ({ticker})")
                yf_sma_200 = await yfinance_service.get_sma_200_direct(ticker)
                
                if yf_sma_200 and yf_sma_200 > 0:
                    self.logger.info(f"SMA_200 fetched from Yahoo Finance for {ticker}: {yf_sma_200:.2f}")
                    return float(yf_sma_200)
                else:
                    self.logger.debug(f"Yahoo Finance returned no SMA_200 for {ticker}")
            except Exception as e:
                self.logger.warning(f"Failed to fetch SMA_200 from Yahoo for {ticker}: {e}")
        else:
            self.logger.debug("No YFinance service provided for SMA_200 fallback")
        
        # All attempts failed
        self.logger.warning(f"SMA_200 unavailable for {ticker} after all attempts")
        return None
    
    async def enrich_with_fallback_sma(
        self,
        records: List[StockDataRecord],
        ticker: str,
        yfinance_service=None
    ) -> List[StockDataRecord]:
        """
        Enrich records with SMA_200 using fallback if needed.
        
        Args:
            records: List of stock data records
            ticker: Stock symbol
            yfinance_service: Optional YFinance service for fallback
            
        Returns:
            Records with SMA_200 populated where possible
        """
        # Check if any records are missing SMA_200
        missing_sma = [r for r in records if r.technical and r.technical.sma_200 is None]
        
        if not missing_sma:
            self.logger.debug(f"All records for {ticker} already have SMA_200")
            return records
        
        self.logger.info(f"Found {len(missing_sma)} records missing SMA_200 for {ticker}")
        
        # Try to get SMA_200 using fallback
        sma_200 = await self.calculate_sma_200_with_fallback(ticker, records, yfinance_service)
        
        if sma_200:
            # Apply the SMA_200 to all records that need it
            for record in missing_sma:
                if record.technical:
                    record.technical.sma_200 = sma_200
                    # Track the source in metadata
                    if not hasattr(record.metadata, 'sma_200_source'):
                        record.metadata.sma_200_source = 'yahoo_finance_fallback'
            
            self.logger.info(f"Applied SMA_200={sma_200:.2f} to {len(missing_sma)} records for {ticker}")
        
        return records