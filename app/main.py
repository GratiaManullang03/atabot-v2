"""
ATABOT 2.0 - Universal Adaptive Business Intelligence
Main FastAPI Application
"""
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from loguru import logger
import sys
from typing import Dict, Any

from app.core.config import settings
from app.core.database import db_pool
from app.api.v1 import health, schemas, sync, chat


# Configure Loguru
logger.remove()
logger.add(
    sys.stdout,
    level=settings.LOG_LEVEL,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>"
)
logger.add(
    "logs/atabot_{time}.log",
    rotation="500 MB",
    retention="7 days",
    level="INFO"
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifecycle manager
    """
    logger.info(f"Starting {settings.APP_NAME} v{settings.APP_VERSION}")
    
    try:
        # Initialize database pool
        await db_pool.init_pool()
        logger.info("Database pool initialized")
        
        # Check pgvector extension
        has_vector = await db_pool.check_vector_extension()
        if not has_vector:
            logger.error("pgvector extension not installed! Please run: CREATE EXTENSION vector;")
            raise RuntimeError("pgvector extension required")
        
        # Run database initialization
        await initialize_database()
        
        # Start real-time sync listener if enabled
        if settings.ENABLE_REALTIME_SYNC:
            await start_realtime_sync_listener()
        
        logger.info(f"{settings.APP_NAME} started successfully!")
        
        yield
        
    except Exception as e:
        logger.error(f"Failed to start application: {e}")
        raise
    finally:
        # Cleanup
        logger.info("Shutting down application...")
        await db_pool.close_pool()
        logger.info("Application shutdown complete")


# Create FastAPI app
app = FastAPI(
    title=settings.APP_NAME,
    description=settings.APP_DESCRIPTION,
    version=settings.APP_VERSION,
    lifespan=lifespan,
    docs_url="/docs" if settings.DEBUG else None,
    redoc_url="/redoc" if settings.DEBUG else None
)


# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=settings.CORS_ALLOW_CREDENTIALS,
    allow_methods=settings.CORS_ALLOW_METHODS,
    allow_headers=settings.CORS_ALLOW_HEADERS
)


# Include API routers
app.include_router(health.router, prefix="/api/v1", tags=["Health"])
app.include_router(schemas.router, prefix="/api/v1/schemas", tags=["Schemas"])
app.include_router(sync.router, prefix="/api/v1/sync", tags=["Synchronization"])
app.include_router(chat.router, prefix="/api/v1/chat", tags=["Chat"])


# Root endpoint
@app.get("/", tags=["Root"])
async def root() -> Dict[str, Any]:
    """
    Root endpoint with application information
    """
    return {
        "name": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "description": settings.APP_DESCRIPTION,
        "status": "running",
        "features": {
            "streaming": settings.ENABLE_STREAMING,
            "realtime_sync": settings.ENABLE_REALTIME_SYNC,
            "query_decomposition": settings.ENABLE_QUERY_DECOMPOSITION,
            "hybrid_search": settings.ENABLE_HYBRID_SEARCH
        },
        "endpoints": {
            "docs": "/docs" if settings.DEBUG else None,
            "health": "/api/v1/health",
            "schemas": "/api/v1/schemas",
            "chat": "/api/v1/chat"
        }
    }


# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """
    Global exception handler for unhandled errors
    """
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "message": str(exc) if settings.DEBUG else "An unexpected error occurred",
            "path": request.url.path
        }
    )


async def initialize_database() -> None:
    """
    Initialize database schema and tables
    """
    try:
        # Check if atabot schema exists
        query = """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.schemata 
                WHERE schema_name = 'atabot'
            )
        """
        schema_exists = await db_pool.fetchval(query)
        
        if not schema_exists:
            logger.info("Creating ATABOT schema and tables...")
            
            # Read and execute init SQL
            with open('init.sql', 'r') as f:
                init_sql = f.read()
            
            # Execute initialization SQL
            await db_pool.execute(init_sql)
            
            logger.info("Database initialization complete")
        else:
            logger.info("ATABOT schema already exists")
            
    except FileNotFoundError:
        logger.warning("init.sql not found, assuming database is already initialized")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        raise


async def start_realtime_sync_listener() -> None:
    """
    Start listening for real-time data changes
    """
    try:
        async def handle_data_change(connection, pid, channel, payload):
            """Handle data change notifications"""
            logger.debug(f"Data change notification: {payload}")
            # TODO: Process the change and update embeddings
            # This will be implemented in the sync service
        
        await db_pool.listen_to_channel('atabot_data_change', handle_data_change)
        logger.info("Real-time sync listener started")
        
    except Exception as e:
        logger.error(f"Failed to start real-time sync listener: {e}")


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=settings.PORT,
        reload=settings.DEBUG,
        log_level=settings.LOG_LEVEL.lower(),
        access_log=True,
        loop="uvloop"
    )