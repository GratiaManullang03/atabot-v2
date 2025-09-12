"""
Monitoring API endpoints for embedding system health
"""
from fastapi import APIRouter, HTTPException
from typing import Dict, Any
from loguru import logger

from app.core.database import db_pool
from app.services.embedding_queue import embedding_queue
from app.services.cache_persistence import cache_persistence

router = APIRouter()

@router.get("/embeddings/stats")
async def get_embedding_stats() -> Dict[str, Any]:
    """Get comprehensive embedding statistics"""
    try:
        # Get queue stats
        queue_stats = embedding_queue.get_cache_stats()
        
        # Get database stats
        db_stats = await db_pool.fetchrow("""
            SELECT 
                COUNT(*) as total_embeddings,
                COUNT(CASE WHEN embedding != array_fill(0::float, ARRAY[1024]) THEN 1 END) as valid_embeddings,
                COUNT(CASE WHEN embedding = array_fill(0::float, ARRAY[1024]) THEN 1 END) as zero_embeddings,
                COUNT(DISTINCT schema_name) as schemas_count,
                COUNT(DISTINCT table_name) as tables_count,
                MIN(created_at) as oldest_embedding,
                MAX(created_at) as newest_embedding
            FROM atabot.embeddings
        """)
        
        # Get persistent cache stats
        persistent_stats = await cache_persistence.get_cache_stats()
        
        # Calculate health score
        total = db_stats['total_embeddings'] or 1
        valid = db_stats['valid_embeddings'] or 0
        health_score = (valid / total) * 100 if total > 0 else 0
        
        return {
            "queue": queue_stats,
            "database": dict(db_stats) if db_stats else {},
            "persistent_cache": persistent_stats,
            "health": {
                "score": round(health_score, 2),
                "status": "healthy" if health_score > 90 else "degraded" if health_score > 50 else "critical",
                "zero_vector_percentage": round((db_stats['zero_embeddings'] / total * 100) if total > 0 else 0, 2)
            }
        }
        
    except Exception as e:
        logger.error(f"Failed to get embedding stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/embeddings/failures")
async def get_embedding_failures() -> Dict[str, Any]:
    """Get details about failed embeddings"""
    try:
        # Get recent zero embeddings
        zero_embeddings = await db_pool.fetch("""
            SELECT 
                schema_name,
                table_name,
                COUNT(*) as count,
                MIN(created_at) as first_failure,
                MAX(created_at) as last_failure
            FROM atabot.embeddings
            WHERE embedding = array_fill(0::float, ARRAY[1024])
            GROUP BY schema_name, table_name
            ORDER BY count DESC
            LIMIT 20
        """)
        
        # Get failed batch IDs from queue
        failed_batches = [
            batch_id for batch_id, status in embedding_queue.batch_status.items() 
            if status == 'failed'
        ]
        
        return {
            "zero_embeddings_by_table": [dict(row) for row in zero_embeddings],
            "failed_batch_ids": failed_batches[-10:],  # Last 10 failed batches
            "total_failures": len(failed_batches)
        }
        
    except Exception as e:
        logger.error(f"Failed to get failure details: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/embeddings/retry-failed")
async def retry_failed_embeddings(schema_name: str = None, table_name: str = None) -> Dict[str, Any]:
    """Retry generation for failed embeddings"""
    try:
        # Build query
        query = """
            SELECT id, schema_name, table_name, row_id, content
            FROM atabot.embeddings
            WHERE embedding = array_fill(0::float, ARRAY[1024])
        """
        params = []
        
        if schema_name:
            query += f" AND schema_name = ${len(params) + 1}"
            params.append(schema_name)
            
        if table_name:
            query += f" AND table_name = ${len(params) + 1}"
            params.append(table_name)
            
        query += " LIMIT 100"  # Process in batches
        
        # Get failed embeddings
        failed_rows = await db_pool.fetch(query, *params)
        
        if not failed_rows:
            return {"message": "No failed embeddings found", "retried": 0}
        
        # Prepare for retry
        texts = [row['content'] for row in failed_rows]
        metadata = [
            {
                "id": str(row['id']),
                "schema": row['schema_name'],
                "table": row['table_name'],
                "row_id": row['row_id']
            }
            for row in failed_rows
        ]
        
        # Add to queue for retry
        batch_id = await embedding_queue.add_batch(texts, metadata)
        
        return {
            "message": "Retry initiated",
            "batch_id": batch_id,
            "embeddings_to_retry": len(failed_rows)
        }
        
    except Exception as e:
        logger.error(f"Failed to retry embeddings: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/cache/persist")
async def persist_cache() -> Dict[str, str]:
    """Manually trigger cache persistence"""
    try:
        await cache_persistence.bulk_save(embedding_queue.cache)
        return {"status": "success", "message": f"Persisted {len(embedding_queue.cache)} embeddings"}
        
    except Exception as e:
        logger.error(f"Failed to persist cache: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/cache/load")
async def load_cache(limit: int = 1000) -> Dict[str, Any]:
    """Load cache from persistent storage"""
    try:
        loaded = await cache_persistence.bulk_load(limit)
        
        # Merge with existing cache
        for text_hash, embedding in loaded.items():
            if text_hash not in embedding_queue.cache:
                embedding_queue.cache[text_hash] = embedding
                
        return {
            "status": "success",
            "loaded": len(loaded),
            "total_cached": len(embedding_queue.cache)
        }
        
    except Exception as e:
        logger.error(f"Failed to load cache: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/cache/cleanup")
async def cleanup_cache(days: int = 30) -> Dict[str, str]:
    """Clean up old cache entries"""
    try:
        await cache_persistence.cleanup_old_cache(days)
        return {"status": "success", "message": f"Cleaned up cache older than {days} days"}
        
    except Exception as e:
        logger.error(f"Failed to cleanup cache: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/queue/status")
async def get_queue_status() -> Dict[str, Any]:
    """Get current queue processing status"""
    return {
        "processing": embedding_queue.processing,
        "queue_size": len(embedding_queue.queue),
        "cache_size": len(embedding_queue.cache),
        "batch_status": {
            status: count 
            for status, count in [
                ("pending", len([b for b in embedding_queue.batch_status.values() if b == 'pending'])),
                ("processing", len([b for b in embedding_queue.batch_status.values() if b == 'processing'])),
                ("completed", len([b for b in embedding_queue.batch_status.values() if b == 'completed'])),
                ("failed", len([b for b in embedding_queue.batch_status.values() if b == 'failed']))
            ]
        }
    }