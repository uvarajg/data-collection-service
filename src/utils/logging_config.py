"""
Centralized logging configuration for the Data Collection Service.

Provides structured logging with consistent formatting, log levels,
and context enrichment across all service components.

Key Features:
- Structured JSON logging for production
- Human-readable console logging for development  
- Automatic context enrichment (service, timestamp, etc.)
- Performance logging for API calls
- Error tracking with stack traces
- Log rotation and retention policies

Usage:
    from src.utils.logging_config import setup_logging, get_logger
    
    # Setup logging (call once at application startup)
    setup_logging()
    
    # Get a logger for your module
    logger = get_logger(__name__)
    
    # Use structured logging
    logger.info("Processing ticker", ticker="AAPL", records=42)
"""

import structlog
import logging
import logging.config
from typing import Any, Dict, Optional
from datetime import datetime
import json
import sys
from pathlib import Path


def setup_logging(
    log_level: str = "INFO",
    environment: str = "development",
    log_file: Optional[str] = None,
    service_name: str = "data-collection-service"
) -> None:
    """
    Configure structured logging for the entire application.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        environment: Environment name (development, staging, production)
        log_file: Optional log file path for file output
        service_name: Service name for log enrichment
    """
    
    # Configure standard library logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, log_level.upper(), logging.INFO),
    )
    
    # Determine if we're in production
    is_production = environment.lower() == "production"
    
    # Configure structlog processors
    processors = [
        # Add standard fields
        structlog.stdlib.filter_by_level,
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        
        # Add service context
        structlog.processors.CallsiteParameterAdder(
            parameters=[
                structlog.processors.CallsiteParameter.FILENAME,
                structlog.processors.CallsiteParameter.FUNC_NAME,
                structlog.processors.CallsiteParameter.LINENO,
            ]
        ),
        
        # Add service metadata
        lambda _, __, event_dict: {
            **event_dict,
            "service": service_name,
            "environment": environment,
            "timestamp": datetime.utcnow().isoformat(),
        },
    ]
    
    if is_production:
        # Production: JSON format for log aggregation
        processors.extend([
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer()
        ])
    else:
        # Development: Human-readable format
        processors.extend([
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer(
                colors=True,
                exception_formatter=structlog.dev.rich_traceback
            )
        ])
    
    # Configure structlog
    structlog.configure(
        processors=processors,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    # Setup file logging if specified
    if log_file:
        setup_file_logging(log_file, log_level, service_name)
    
    # Log the configuration
    logger = get_logger(__name__)
    logger.info(
        "Logging configured successfully",
        log_level=log_level,
        environment=environment,
        is_production=is_production,
        log_file=log_file
    )


def setup_file_logging(log_file: str, log_level: str, service_name: str) -> None:
    """Setup file-based logging with rotation."""
    from logging.handlers import RotatingFileHandler
    
    # Ensure log directory exists
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Configure file handler with rotation
    file_handler = RotatingFileHandler(
        filename=log_file,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    
    # JSON formatter for file logs
    file_handler.setFormatter(
        logging.Formatter('%(message)s')
    )
    file_handler.setLevel(getattr(logging, log_level.upper()))
    
    # Add to root logger
    root_logger = logging.getLogger()
    root_logger.addHandler(file_handler)


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """
    Get a structured logger for the specified module.
    
    Args:
        name: Module name (typically __name__)
        
    Returns:
        Configured structlog logger
    """
    return structlog.get_logger(name)


class LoggingMixin:
    """Mixin class to add logging capabilities to any class."""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = get_logger(self.__class__.__module__)


def log_api_call(
    service_name: str,
    endpoint: str,
    method: str = "GET",
    ticker: Optional[str] = None,
    **kwargs
):
    """
    Decorator for logging API calls with timing and context.
    
    Args:
        service_name: Name of the external service (alpaca, yfinance, etc.)
        endpoint: API endpoint or operation name
        method: HTTP method or operation type
        ticker: Stock ticker symbol if applicable
        **kwargs: Additional context to log
    """
    def decorator(func):
        import time
        from functools import wraps
        
        @wraps(func)
        async def wrapper(*args, **func_kwargs):
            logger = get_logger(func.__module__)
            
            # Build context
            context = {
                "service": service_name,
                "endpoint": endpoint,
                "method": method,
                "function": func.__name__,
                **kwargs
            }
            
            if ticker:
                context["ticker"] = ticker
            
            # Extract ticker from function args if not provided
            if not ticker and args and hasattr(args[0], '__dict__'):
                # Try to find ticker in function arguments
                for arg in args[1:]:  # Skip self
                    if isinstance(arg, str) and len(arg) <= 5 and arg.isupper():
                        context["ticker"] = arg
                        break
            
            start_time = time.time()
            
            try:
                logger.info("API call started", **context)
                result = await func(*args, **func_kwargs)
                
                duration = time.time() - start_time
                logger.info(
                    "API call completed successfully",
                    duration_ms=round(duration * 1000, 2),
                    **context
                )
                
                return result
                
            except Exception as e:
                duration = time.time() - start_time
                logger.error(
                    "API call failed",
                    duration_ms=round(duration * 1000, 2),
                    error=str(e),
                    error_type=type(e).__name__,
                    **context
                )
                raise
                
        return wrapper
    return decorator


def log_performance(operation_name: str, **context):
    """
    Decorator for logging operation performance.
    
    Args:
        operation_name: Name of the operation being timed
        **context: Additional context to include in logs
    """
    def decorator(func):
        import time
        from functools import wraps
        
        @wraps(func)
        async def wrapper(*args, **kwargs):
            logger = get_logger(func.__module__)
            
            start_time = time.time()
            
            try:
                result = await func(*args, **kwargs)
                duration = time.time() - start_time
                
                logger.info(
                    f"{operation_name} completed",
                    operation=operation_name,
                    duration_ms=round(duration * 1000, 2),
                    function=func.__name__,
                    **context
                )
                
                return result
                
            except Exception as e:
                duration = time.time() - start_time
                logger.error(
                    f"{operation_name} failed",
                    operation=operation_name,
                    duration_ms=round(duration * 1000, 2),
                    function=func.__name__,
                    error=str(e),
                    error_type=type(e).__name__,
                    **context
                )
                raise
                
        return wrapper
    return decorator


def setup_logging_from_settings():
    """Setup logging using application settings."""
    from ..config.settings import get_settings
    
    settings = get_settings()
    
    # Determine log file path if in production
    log_file = None
    if settings.environment == "production":
        log_dir = Path(settings.data_base_path) / "logs"
        log_file = str(log_dir / "data-collection-service.log")
    
    setup_logging(
        log_level=settings.log_level,
        environment=settings.environment,
        log_file=log_file,
        service_name="data-collection-service"
    )


# Context managers for enriching logs
class LogContext:
    """Context manager for adding structured context to logs."""
    
    def __init__(self, **context):
        self.context = context
        
    def __enter__(self):
        structlog.contextvars.bind_contextvars(**self.context)
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        structlog.contextvars.unbind_contextvars(*self.context.keys())


def ticker_context(ticker: str):
    """Context manager for ticker-specific logging."""
    return LogContext(ticker=ticker)


def job_context(job_id: str):
    """Context manager for job-specific logging."""
    return LogContext(job_id=job_id)


def collection_context(ticker: str, job_id: str, date_range: str):
    """Context manager for data collection logging."""
    return LogContext(
        ticker=ticker,
        job_id=job_id,
        date_range=date_range,
        operation="data_collection"
    )