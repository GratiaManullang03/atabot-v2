"""
ATABOT 2.0 - Universal Adaptive Business Intelligence
Main FastAPI Application with Monitoring and Cache Persistence
"""
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from loguru import logger
import sys
import asyncio

from app.core.config import settings
from app.core.database import db_pool
from app.core.initializer import app_initializer
from app.api.v1.api import api_router
from app.services.cache_persistence import cache_persistence, periodic_cache_persistence
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
    """Application lifecycle manager with cache persistence"""
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
        
        # Initialize cache persistence
        await cache_persistence.initialize()
        
        # Load cached embeddings from database
        if settings.ENABLE_CACHE:
            logger.info("Loading embeddings from persistent cache...")
            cached_embeddings = await cache_persistence.bulk_load(
                limit=settings.EMBEDDING_CACHE_SIZE if hasattr(settings, 'EMBEDDING_CACHE_SIZE') else 1000
            )
            
            # Merge with embedding queue cache
            for text_hash, embedding in cached_embeddings.items():
                if text_hash not in embedding_queue.cache:
                    embedding_queue.cache[text_hash] = embedding
            
            logger.info(f"Loaded {len(cached_embeddings)} embeddings from persistent storage")
        
        # Start background tasks
        if settings.ENABLE_CACHE:
            # Start periodic cache persistence
            task = asyncio.create_task(periodic_cache_persistence())
            background_tasks.append(task)
            logger.info("Started periodic cache persistence task")
        
        # Start real-time listener if enabled
        if settings.ENABLE_REALTIME_SYNC:
            await app_initializer.start_realtime_listener()
        
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
        
        # Save cache before shutdown
        if settings.ENABLE_CACHE and embedding_queue.cache:
            logger.info("Saving cache to persistent storage...")
            await cache_persistence.bulk_save(embedding_queue.cache)
        
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