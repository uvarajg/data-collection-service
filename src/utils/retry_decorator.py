"""
API Retry Decorator Utility

Provides centralized retry logic for all external API calls with exponential backoff,
configurable attempts, and proper error handling.

Key Features:
- Exponential backoff with jitter
- Configurable max attempts and delays
- Specific exception handling for different API types
- Structured logging integration
- Rate limiting compliance

Usage:
    @api_retry(max_attempts=3, base_delay=0.1)
    async def fetch_data(ticker: str):
        # API call here
        return data
"""

import asyncio
import random
from functools import wraps
from typing import Callable, Any, Optional, Tuple, List
import structlog

logger = structlog.get_logger()


def api_retry(
    max_attempts: int = 3,
    base_delay: float = 0.1,
    max_delay: float = 10.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
    retryable_exceptions: Optional[Tuple[Exception, ...]] = None,
    service_name: str = "api"
):
    """
    Decorator for API calls with intelligent retry logic.
    
    Args:
        max_attempts: Maximum number of retry attempts
        base_delay: Initial delay between retries in seconds
        max_delay: Maximum delay between retries in seconds
        exponential_base: Base for exponential backoff calculation
        jitter: Add random jitter to prevent thundering herd
        retryable_exceptions: Tuple of exception types to retry on
        service_name: Service name for logging context
        
    Returns:
        Decorated function with retry capability
        
    Raises:
        The last exception if all retries are exhausted
    """
    
    # Default retryable exceptions for common API issues
    if retryable_exceptions is None:
        retryable_exceptions = (
            ConnectionError,
            TimeoutError,
            OSError,  # Includes network-related errors
            Exception  # Catch-all for API-specific errors
        )
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            
            for attempt in range(1, max_attempts + 1):
                try:
                    # Execute the function
                    result = await func(*args, **kwargs)
                    
                    # Log successful attempt if it wasn't the first
                    if attempt > 1:
                        logger.info(
                            f"{service_name} API call succeeded after retry",
                            service=service_name,
                            attempt=attempt,
                            function=func.__name__,
                            args_count=len(args)
                        )
                    
                    return result
                    
                except retryable_exceptions as e:
                    last_exception = e
                    
                    # Don't sleep after the last attempt
                    if attempt < max_attempts:
                        # Calculate delay with exponential backoff
                        delay = min(
                            base_delay * (exponential_base ** (attempt - 1)),
                            max_delay
                        )
                        
                        # Add jitter to prevent thundering herd
                        if jitter:
                            delay *= (0.5 + 0.5 * random.random())
                        
                        logger.warning(
                            f"{service_name} API call failed, retrying",
                            service=service_name,
                            attempt=attempt,
                            max_attempts=max_attempts,
                            delay=f"{delay:.2f}s",
                            error=str(e),
                            function=func.__name__
                        )
                        
                        await asyncio.sleep(delay)
                    else:
                        # Final attempt failed
                        logger.error(
                            f"{service_name} API call failed after all retries",
                            service=service_name,
                            attempts=max_attempts,
                            final_error=str(e),
                            function=func.__name__,
                            error_type=type(e).__name__
                        )
            
            # Re-raise the last exception if all attempts failed
            raise last_exception
            
        return wrapper
    return decorator


def alpaca_retry(max_attempts: int = 3):
    """
    Specialized retry decorator for Alpaca API calls.
    
    Handles Alpaca-specific exceptions and rate limits.
    """
    alpaca_exceptions = (
        ConnectionError,
        TimeoutError,
        Exception  # Alpaca SDK may raise various exceptions
    )
    
    return api_retry(
        max_attempts=max_attempts,
        base_delay=0.16,  # Alpaca rate limit: ~6 requests/second
        max_delay=5.0,
        service_name="alpaca",
        retryable_exceptions=alpaca_exceptions
    )


def yfinance_retry(max_attempts: int = 3):
    """
    Specialized retry decorator for Yahoo Finance API calls.
    
    Handles YFinance-specific exceptions and rate limits.
    """
    yfinance_exceptions = (
        ConnectionError,
        TimeoutError,
        ValueError,  # YFinance may raise ValueError for invalid data
        KeyError,    # YFinance may raise KeyError for missing data
        Exception
    )
    
    return api_retry(
        max_attempts=max_attempts,
        base_delay=0.1,  # Yahoo Finance is more permissive
        max_delay=3.0,
        service_name="yfinance",
        retryable_exceptions=yfinance_exceptions
    )


def google_sheets_retry(max_attempts: int = 3):
    """
    Specialized retry decorator for Google Sheets API calls.
    
    Handles Google Sheets API exceptions and quota limits.
    """
    sheets_exceptions = (
        ConnectionError,
        TimeoutError,
        Exception  # Google API client may raise various exceptions
    )
    
    return api_retry(
        max_attempts=max_attempts,
        base_delay=1.0,  # Google Sheets has stricter rate limits
        max_delay=10.0,
        service_name="google_sheets",
        retryable_exceptions=sheets_exceptions
    )


# Rate limiting utilities
class RateLimiter:
    """Simple rate limiter for API calls."""
    
    def __init__(self, calls_per_second: float):
        self.min_interval = 1.0 / calls_per_second
        self.last_call = 0.0
    
    async def wait_if_needed(self):
        """Wait if necessary to respect rate limits."""
        import time
        current_time = time.time()
        time_since_last = current_time - self.last_call
        
        if time_since_last < self.min_interval:
            wait_time = self.min_interval - time_since_last
            await asyncio.sleep(wait_time)
        
        self.last_call = time.time()


# Lazy-loaded rate limiters using configuration
_rate_limiters = {}

def get_rate_limiter(service_name: str) -> RateLimiter:
    """Get or create a rate limiter for the specified service."""
    if service_name not in _rate_limiters:
        from ..config.settings import get_settings
        settings = get_settings()
        
        rates = {
            'alpaca': settings.alpaca_rate_limit,
            'yfinance': settings.yfinance_rate_limit,
            'google_sheets': settings.google_sheets_rate_limit
        }
        
        rate = rates.get(service_name, 1.0)  # Default to 1 RPS
        _rate_limiters[service_name] = RateLimiter(rate)
    
    return _rate_limiters[service_name]

# Predefined rate limiters for common APIs (now configuration-driven)
def get_alpaca_rate_limiter():
    return get_rate_limiter('alpaca')

def get_yfinance_rate_limiter():
    return get_rate_limiter('yfinance')

def get_google_sheets_rate_limiter():
    return get_rate_limiter('google_sheets')

# Backward compatibility
ALPACA_RATE_LIMITER = get_alpaca_rate_limiter()
YFINANCE_RATE_LIMITER = get_yfinance_rate_limiter() 
GOOGLE_SHEETS_RATE_LIMITER = get_google_sheets_rate_limiter()