"""
ATABOT 2.0 - Universal Adaptive Business Intelligence
Main FastAPI Application with Database Storage
"""
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from loguru import logger
import sys

from app.core.config import settings
from app.core.database import db_pool
from app.core.initializer import app_initializer
from app.api.v1.api import api_router
# Cache persistence removed - using database storage only
from app.services.embedding_queue import embedding_queue

# Configure Loguru
logger.remove()
logger.add(
    sys.stdout,
    level=settings.LOG_LEVEL,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan> - <level>{message}</level>"
)

if settings.DEBUG:
    logger.add("logs/atabot_{time}.log", rotation="500 MB", retention="7 days")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle manager"""
    logger.info(f"Starting {settings.APP_NAME} v{settings.APP_VERSION}")
    
    # Background tasks
    background_tasks = []
    
    try:
        # Initialize database pool
        await db_pool.init_pool()
        
        # Check dependencies
        deps_status = await app_initializer.check_dependencies()
        if not deps_status["pgvector"]:
            raise RuntimeError("pgvector extension required")
        
        # Initialize database schema
        await app_initializer.initialize_database()
        
        # Cache persistence disabled - embeddings stored in main database
        # No need for separate cache persistence when we have full sync
        logger.info("Cache persistence disabled - using database storage only")
        
        # Real-time sync disabled - using manual sync only
        # This provides better control and reliability
        
        logger.info(f"{settings.APP_NAME} started successfully!")
        logger.info(f"Monitoring available at: http://localhost:{settings.PORT}/api/v1/monitoring/embeddings/stats")
        
        yield
        
    except Exception as e:
        logger.error(f"Startup failed: {e}")
        raise
        
    finally:
        # Cleanup on shutdown
        logger.info("Shutting down application...")
        
        # Cancel background tasks
        for task in background_tasks:
            task.cancel()
        
        # Cache persistence disabled - no need to save cache
        logger.info("Cache persistence disabled - no cache saving needed")
        
        # Close database pool
        await db_pool.close_pool()
        
        logger.info("Application shut down complete")


# Create FastAPI app
app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description=settings.APP_DESCRIPTION,
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=settings.CORS_ALLOW_CREDENTIALS,
    allow_methods=settings.CORS_ALLOW_METHODS,
    allow_headers=settings.CORS_ALLOW_HEADERS,
)

# Include routers
app.include_router(api_router)

# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error occurred"}
    )

# Health check endpoint
@app.get("/")
async def root():
    return {
        "name": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "status": "running",
        "docs": "/docs",
        "monitoring": "/api/v1/monitoring/embeddings/stats"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=settings.PORT,
        reload=settings.DEBUG,
        log_level=settings.LOG_LEVEL.lower()
    )