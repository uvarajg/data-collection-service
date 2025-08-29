from fastapi import APIRouter
from pydantic import BaseModel
import structlog
from datetime import datetime
import sys

logger = structlog.get_logger()
router = APIRouter()


class HealthResponse(BaseModel):
    service: str = "data-collection-service"
    status: str = "healthy"
    version: str = "0.1.0"
    timestamp: datetime
    python_version: str
    environment: str = "development"


@router.get("/health", response_model=HealthResponse)
async def health_check():
    logger.info("Health check requested")
    
    return HealthResponse(
        timestamp=datetime.utcnow(),
        python_version=f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    )


@router.get("/health/ready")
async def readiness_check():
    logger.info("Readiness check requested")
    return {"status": "ready"}


@router.get("/health/live")
async def liveness_check():
    logger.info("Liveness check requested")
    return {"status": "alive"}