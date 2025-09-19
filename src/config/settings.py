from functools import lru_cache
from pydantic_settings import BaseSettings
from typing import Dict, Any


class Settings(BaseSettings):
    """
    Application settings with environment variable support.
    
    All settings can be overridden via environment variables.
    For nested settings, use double underscores (e.g., API_RETRY__MAX_ATTEMPTS).
    """
    
    # Application settings
    debug: bool = False
    port: int = 8000
    log_level: str = "INFO"
    environment: str = "development"  # development, staging, production
    
    # Data paths
    data_base_path: str = "/workspaces/data"
    cache_base_path: str = "/workspaces/data/cache"
    
    # Alpaca API settings - using standard environment variable names
    apca_api_key_id: str = ""
    apca_api_secret_key: str = ""
    apca_api_base_url: str = "https://api.alpaca.markets"
    
    # External API keys
    gemini_api_key: str = ""
    google_api_key: str = ""
    
    # Google Sheets credentials
    google_service_account_email: str = ""
    google_private_key: str = ""
    
    # Google Sheets spreadsheet IDs
    active_stocks_spreadsheet_id: str = ""
    active_stocks_sheet_name: str = "MostActive"
    
    # API retry configuration
    api_retry_max_attempts: int = 3
    api_retry_base_delay: float = 0.1
    api_retry_max_delay: float = 10.0
    
    # Rate limiting configuration (requests per second)
    alpaca_rate_limit: float = 6.0
    yfinance_rate_limit: float = 10.0
    google_sheets_rate_limit: float = 1.0
    
    # Data collection configuration
    collection_delay_seconds: float = 0.1  # Delay between ticker processing
    max_concurrent_tickers: int = 1  # Sequential processing only
    
    # Technical indicator configuration
    technical_indicators_enabled: bool = True
    sma_200_fallback_enabled: bool = True
    
    # Fundamental data configuration
    fundamentals_enabled: bool = True
    fundamentals_cache_hours: int = 4
    debt_to_equity_fallback_enabled: bool = True
    current_ratio_fallback_enabled: bool = True
    profit_margin_fallback_enabled: bool = True
    
    # Data validation configuration
    technical_validation_enabled: bool = True
    validation_move_to_errors: bool = True
    
    # Validation thresholds (relaxed for production)
    validation_thresholds: Dict[str, Dict[str, float]] = {
        "price_relative_bounds": {
            "sma_50_min_ratio": 0.4,
            "sma_50_max_ratio": 2.5,
            "sma_200_min_ratio": 0.2,
            "sma_200_max_ratio": 4.0,
            "ema_12_min_ratio": 0.7,
            "ema_12_max_ratio": 1.4,
            "ema_26_min_ratio": 0.6,
            "ema_26_max_ratio": 1.5,
            "bb_upper_min_ratio": 0.9,
            "bb_upper_max_ratio": 2.0,
            "bb_middle_min_ratio": 0.7,
            "bb_middle_max_ratio": 1.4,
            "bb_lower_min_ratio": 0.3,
            "bb_lower_max_ratio": 1.2
        }
    }
    
    # Error monitoring configuration
    error_rate_threshold: float = 0.02  # 2% error rate threshold
    error_monitoring_window_hours: int = 24
    
    # Cache configuration
    yfinance_cache_duration_hours: int = 4
    cache_cleanup_enabled: bool = True
    
    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "ignore"
    }


@lru_cache()
def get_settings() -> Settings:
    return Settings()