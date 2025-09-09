"""
Health Check and System Status API
"""
from fastapi import APIRouter, HTTPException
from typing import Dict, Any
from datetime import datetime
from loguru import logger
import psutil

from app.core.config import settings
from app.core.database import db_pool
from app.core.embeddings import embedding_service

router = APIRouter()

@router.get("/health")
async def health_check() -> Dict[str, Any]:
    """
    Comprehensive health check endpoint
    """
    start_time = datetime.now()
    health_status = {
        "status": "healthy",
        "timestamp": start_time.isoformat(),
        "version": settings.APP_VERSION,
        "checks": {}
    }
    
    # Check database connectivity
    try:
        query = "SELECT 1"
        result = await db_pool.fetchval(query)
        
        # Check pgvector extension
        vector_enabled = await db_pool.check_vector_extension()
        
        health_status["checks"]["database"] = {
            "status": "healthy",
            "connected": True,
            "pgvector_enabled": vector_enabled,
            "pool_size": db_pool.pool._size if db_pool.pool else 0
        }
    except Exception as e:
        health_status["checks"]["database"] = {
            "status": "unhealthy",
            "error": str(e)
        }
        health_status["status"] = "degraded"
    
    # Check embedding service
    try:
        # Test with a simple embedding
        test_embedding = await embedding_service.generate_embedding("test", "query")
        health_status["checks"]["embedding_service"] = {
            "status": "healthy",
            "model": settings.VOYAGE_MODEL,
            "dimensions": len(test_embedding),
            "cache_size": embedding_service._cache_size
        }
    except Exception as e:
        health_status["checks"]["embedding_service"] = {
            "status": "unhealthy",
            "error": str(e)
        }
        health_status["status"] = "degraded"
    
    # System metrics
    health_status["metrics"] = {
        "cpu_percent": psutil.cpu_percent(interval=0.1),
        "memory": {
            "percent": psutil.virtual_memory().percent,
            "available_mb": psutil.virtual_memory().available / 1024 / 1024,
            "total_mb": psutil.virtual_memory().total / 1024 / 1024
        },
        "disk": {
            "percent": psutil.disk_usage('/').percent,
            "free_gb": psutil.disk_usage('/').free / 1024 / 1024 / 1024
        }
    }
    
    # Response time
    response_time_ms = (datetime.now() - start_time).total_seconds() * 1000
    health_status["response_time_ms"] = round(response_time_ms, 2)
    
    return health_status

@router.get("/ready")
async def readiness_check() -> Dict[str, Any]:
    """
    Kubernetes readiness probe endpoint
    """
    try:
        # Check if database is accessible
        await db_pool.fetchval("SELECT 1")
        
        return {
            "ready": True,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Readiness check failed: {e}")
        raise HTTPException(status_code=503, detail="Service not ready")

@router.get("/live")
async def liveness_check() -> Dict[str, Any]:
    """
    Kubernetes liveness probe endpoint
    """
    return {
        "alive": True,
        "timestamp": datetime.now().isoformat()
    }

@router.get("/metrics")
async def get_metrics() -> Dict[str, Any]:
    """
    Get application metrics for monitoring
    """
    try:
        # Get database metrics
        db_metrics = {}
        if db_pool.pool:
            db_metrics = {
                "pool_size": db_pool.pool._size,
                "pool_free": db_pool.pool._free_size,
                "pool_max": settings.DATABASE_POOL_SIZE
            }
        
        # Get schema metrics
        schemas_query = """
            SELECT 
                COUNT(*) as total_schemas,
                SUM(total_tables) as total_tables,
                SUM(total_rows) as total_rows
            FROM atabot.managed_schemas
            WHERE is_active = true
        """
        schema_stats = await db_pool.fetchrow(schemas_query)
        
        # Get embedding metrics
        embeddings_query = """
            SELECT 
                COUNT(*) as total_embeddings,
                COUNT(DISTINCT schema_name) as unique_schemas,
                COUNT(DISTINCT table_name) as unique_tables
            FROM atabot.embeddings
        """
        embedding_stats = await db_pool.fetchrow(embeddings_query)
        
        # Get query metrics
        query_metrics_query = """
            SELECT 
                COUNT(*) as total_queries,
                AVG(response_time_ms) as avg_response_time,
                MAX(response_time_ms) as max_response_time,
                MIN(response_time_ms) as min_response_time
            FROM atabot.query_logs
            WHERE created_at > NOW() - INTERVAL '1 hour'
        """
        query_stats = await db_pool.fetchrow(query_metrics_query)
        
        return {
            "timestamp": datetime.now().isoformat(),
            "application": {
                "version": settings.APP_VERSION,
                "uptime_seconds": (datetime.now() - start_time).total_seconds() if 'start_time' in globals() else 0
            },
            "database": db_metrics,
            "schemas": dict(schema_stats) if schema_stats else {},
            "embeddings": dict(embedding_stats) if embedding_stats else {},
            "queries": dict(query_stats) if query_stats else {},
            "system": {
                "cpu_percent": psutil.cpu_percent(interval=0.1),
                "memory_percent": psutil.virtual_memory().percent,
                "disk_percent": psutil.disk_usage('/').percent
            }
        }
        
    except Exception as e:
        logger.error(f"Failed to get metrics: {e}")
        return {
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

# Track application start time
start_time = datetime.now()