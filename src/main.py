import structlog
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .api.health import router as health_router
from .config.settings import get_settings

logger = structlog.get_logger()

@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    logger.info("Starting Data Collection Service", version="0.1.0", port=settings.port)
    yield
    logger.info("Shutting down Data Collection Service")

def create_app() -> FastAPI:
    settings = get_settings()
    
    app = FastAPI(
        title="Data Collection Service",
        description="Single source of truth for external data ingestion in the AlgoAlchemist trading platform",
        version="0.1.0",
        lifespan=lifespan
    )
    
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    app.include_router(health_router, tags=["health"])
    
    return app

app = create_app()

if __name__ == "__main__":
    import uvicorn
    settings = get_settings()
    uvicorn.run(
        "src.main:app",
        host="0.0.0.0",
        port=settings.port,
        reload=settings.debug
    )