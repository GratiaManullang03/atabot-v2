"""
Monitoring API Endpoints
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Dict, Optional, List
from datetime import datetime
from loguru import logger

from app.core.database import db_pool
from app.services.embedding_queue import embedding_queue
from app.services.cache_persistence import cache_persistence

router = APIRouter()

@router.get("/embeddings/stats")
async def get_embedding_stats() -> Dict:
    """Get comprehensive embedding statistics"""
    try:
        # Get queue stats
        queue_stats = embedding_queue.get_cache_stats()
        
        # Get database stats
        db_stats = await db_pool.fetchrow("""
            SELECT 
                COUNT(*) as total_embeddings,
                COUNT(DISTINCT schema_name) as unique_schemas,
                COUNT(DISTINCT table_name) as unique_tables,
                COUNT(CASE WHEN embedding = ARRAY_FILL(0::float, ARRAY[1024]) THEN 1 END) as zero_embeddings,
                COUNT(CASE WHEN created_at > NOW() - INTERVAL '1 hour' THEN 1 END) as recent_embeddings
            FROM atabot.embeddings
        """)
        
        # Get cache persistence stats
        cache_stats = await cache_persistence.get_cache_stats()
        
        return {
            "queue": queue_stats,
            "database": dict(db_stats) if db_stats else {},
            "cache_persistence": cache_stats,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to get embedding stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/embeddings/health")
async def check_embedding_health() -> Dict:
    """Check health of embedding system"""
    try:
        issues = []
        warnings = []
        
        # Check for zero embeddings
        zero_count = await db_pool.fetchval("""
            SELECT COUNT(*) 
            FROM atabot.embeddings 
            WHERE embedding = ARRAY_FILL(0::float, ARRAY[1024])
        """)
        
        if zero_count > 0:
            issues.append(f"Found {zero_count} zero embeddings in database")
        
        # Check for stuck batches
        queue_stats = embedding_queue.get_cache_stats()
        if queue_stats['processing_batches'] > 5:
            warnings.append(f"Too many processing batches: {queue_stats['processing_batches']}")
        
        if queue_stats['failed_batches'] > 0:
            issues.append(f"Failed batches detected: {queue_stats['failed_batches']}")
        
        # Check processing status
        if queue_stats['queue_size'] > 10 and not queue_stats['is_processing']:
            issues.append("Queue has items but processing is not running")
        
        # Check recent activity
        recent_count = await db_pool.fetchval("""
            SELECT COUNT(*) 
            FROM atabot.embeddings 
            WHERE created_at > NOW() - INTERVAL '10 minutes'
        """)
        
        if queue_stats['queue_size'] > 0 and recent_count == 0:
            warnings.append("No new embeddings in last 10 minutes despite queue activity")
        
        health_status = "healthy"
        if issues:
            health_status = "unhealthy"
        elif warnings:
            health_status = "degraded"
        
        return {
            "status": health_status,
            "issues": issues,
            "warnings": warnings,
            "metrics": {
                "queue_size": queue_stats['queue_size'],
                "processing": queue_stats['is_processing'],
                "zero_embeddings": zero_count,
                "recent_embeddings": recent_count
            }
        }
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "error",
            "error": str(e)
        }

@router.post("/embeddings/fix-zeros")
async def fix_zero_embeddings(
    schema_name: Optional[str] = None,
    table_name: Optional[str] = None,
    limit: int = Query(default=100, le=1000)
) -> Dict:
    """Fix zero embeddings by regenerating them"""
    try:
        # Find zero embeddings
        query = """
            SELECT id, schema_name, table_name, row_id, content
            FROM atabot.embeddings
            WHERE embedding = ARRAY_FILL(0::float, ARRAY[1024])
        """
        params = []
        
        if schema_name:
            query += f" AND schema_name = ${len(params) + 1}"
            params.append(schema_name)
            
        if table_name:
            query += f" AND table_name = ${len(params) + 1}"
            params.append(table_name)
            
        query += f" LIMIT ${len(params) + 1}"
        params.append(limit)
        
        # Get zero embeddings
        zero_rows = await db_pool.fetch(query, *params)
        
        if not zero_rows:
            return {"message": "No zero embeddings found", "fixed": 0}
        
        # Prepare for regeneration
        texts = []
        metadata = []
        row_ids = []
        
        for row in zero_rows:
            texts.append(row['content'])
            metadata.append({
                "id": str(row['id']),
                "schema": row['schema_name'],
                "table": row['table_name'],
                "row_id": row['row_id']
            })
            row_ids.append(row['id'])
        
        # Add to queue for regeneration
        batch_id = await embedding_queue.add_batch(texts, metadata)
        
        # Wait for processing
        success = await embedding_queue.wait_for_batch(batch_id, timeout=300)
        
        if success:
            # Update database with new embeddings
            fixed_count = 0
            for text, meta, row_id in zip(texts, metadata, row_ids):
                text_hash = hashlib.md5(text.encode()).hexdigest()
                embedding = embedding_queue.cache.get(text_hash)
                
                if embedding and len(embedding) > 0:
                    # Validate embedding
                    non_zero_count = sum(1 for v in embedding if v != 0)
                    if non_zero_count > len(embedding) * 0.1:
                        # Update embedding
                        await db_pool.execute("""
                            UPDATE atabot.embeddings
                            SET embedding = $1::vector, updated_at = NOW()
                            WHERE id = $2
                        """, embedding, row_id)
                        fixed_count += 1
            
            return {
                "message": "Zero embeddings fixed",
                "batch_id": batch_id,
                "total_found": len(zero_rows),
                "fixed": fixed_count
            }
        else:
            return {
                "message": "Failed to regenerate embeddings",
                "batch_id": batch_id,
                "error": "Batch processing failed"
            }
        
    except Exception as e:
        logger.error(f"Failed to fix zero embeddings: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/embeddings/sample")
async def get_sample_embeddings(
    schema_name: Optional[str] = None,
    table_name: Optional[str] = None,
    limit: int = Query(default=5, le=20)
) -> List[Dict]:
    """Get sample embeddings for inspection"""
    try:
        query = """
            SELECT 
                id,
                schema_name,
                table_name,
                row_id,
                LEFT(content, 200) as content_preview,
                CARDINALITY(embedding) as embedding_dims,
                (SELECT COUNT(*) FROM unnest(embedding) e WHERE e != 0) as non_zero_values,
                created_at,
                updated_at
            FROM atabot.embeddings
        """
        params = []
        
        if schema_name:
            query += f" WHERE schema_name = ${len(params) + 1}"
            params.append(schema_name)
            
        if table_name:
            connector = " WHERE" if not schema_name else " AND"
            query += f"{connector} table_name = ${len(params) + 1}"
            params.append(table_name)
            
        query += f" ORDER BY created_at DESC LIMIT ${len(params) + 1}"
        params.append(limit)
        
        rows = await db_pool.fetch(query, *params)
        
        results = []
        for row in rows:
            results.append({
                "id": str(row['id']),
                "schema": row['schema_name'],
                "table": row['table_name'],
                "row_id": row['row_id'],
                "content_preview": row['content_preview'],
                "embedding_dims": row['embedding_dims'],
                "non_zero_values": row['non_zero_values'],
                "zero_percentage": round((1 - row['non_zero_values'] / row['embedding_dims']) * 100, 2) if row['embedding_dims'] > 0 else 100,
                "created_at": row['created_at'].isoformat() if row['created_at'] else None,
                "updated_at": row['updated_at'].isoformat() if row['updated_at'] else None
            })
        
        return results
        
    except Exception as e:
        logger.error(f"Failed to get sample embeddings: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/embeddings/clear-cache")
async def clear_embedding_cache() -> Dict:
    """Clear embedding cache (use with caution)"""
    try:
        # Clear in-memory cache
        cache_size_before = len(embedding_queue.cache)
        embedding_queue.clear_cache()
        
        return {
            "message": "Cache cleared successfully",
            "cleared_embeddings": cache_size_before
        }
        
    except Exception as e:
        logger.error(f"Failed to clear cache: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Import hashlib for the fix-zeros endpoint
import hashlib