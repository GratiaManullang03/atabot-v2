"""
Data Synchronization API Endpoints
File: app/api/v1/endpoints/sync.py
"""
from fastapi import APIRouter, HTTPException, BackgroundTasks
from typing import Dict, Any, Optional
from datetime import datetime
from loguru import logger
import uuid

from app.services.sync_service import sync_service
from app.schemas.sync_models import (
    SyncRequest,
    BatchSyncRequest, 
    SyncResponse,
    SyncStatusResponse
)

router = APIRouter()


@router.post("/table", response_model=SyncResponse)
async def sync_single_table(
    request: SyncRequest,
    background_tasks: BackgroundTasks
):
    """
    Synchronize a single table to vector store
    """
    try:
        # Generate job ID
        job_id = str(uuid.uuid4())
        
        # Start sync in background
        background_tasks.add_task(
            sync_service.sync_table,
            schema=request.schema_name,
            table=request.table_name,
            mode="full" if request.force_full else "incremental"
        )
        
        return SyncResponse(
            success=True,
            message=f"Sync started for {request.schema_name}.{request.table_name}",
            job_id=job_id,
            status="started"
        )
        
    except Exception as e:
        logger.error(f"Failed to start sync: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/batch", response_model=SyncResponse)
async def sync_batch_tables(
    request: BatchSyncRequest,
    background_tasks: BackgroundTasks
):
    """
    Synchronize multiple tables in batch
    """
    try:
        job_id = str(uuid.uuid4())
        
        # Start batch sync in background
        background_tasks.add_task(
            sync_service.sync_schema,
            schema=request.schema_name,
            tables=request.tables,
            mode="full" if request.force_full else "incremental"
        )
        
        return SyncResponse(
            success=True,
            message=f"Batch sync started for {len(request.tables)} tables in {request.schema_name}",
            job_id=job_id,
            status="started"
        )
        
    except Exception as e:
        logger.error(f"Failed to start batch sync: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/initial")
async def initial_sync(
    schema_name: str,
    background_tasks: BackgroundTasks
):
    """
    Perform initial full sync for a schema
    """
    try:
        job_id = str(uuid.uuid4())
        
        # Start full schema sync
        background_tasks.add_task(
            sync_service.sync_schema,
            schema=schema_name,
            mode="full"
        )
        
        return {
            "success": True,
            "message": f"Initial sync started for schema {schema_name}",
            "job_id": job_id,
            "estimated_time": "Depends on data volume"
        }
        
    except Exception as e:
        logger.error(f"Failed to start initial sync: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/realtime/enable")
async def enable_realtime_sync(
    schema_name: str,
    table_name: str
):
    """
    Enable real-time synchronization for a table
    """
    try:
        result = await sync_service.enable_realtime_sync(schema_name, table_name)
        return result
        
    except Exception as e:
        logger.error(f"Failed to enable realtime sync: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/realtime/disable")
async def disable_realtime_sync(
    schema_name: str,
    table_name: str
):
    """
    Disable real-time synchronization for a table
    """
    try:
        # Update sync status
        from app.core.database import db_pool
        
        query = """
            UPDATE atabot.sync_status
            SET realtime_enabled = false
            WHERE schema_name = $1 AND table_name = $2
        """
        await db_pool.execute(query, schema_name, table_name)
        
        return {
            "success": True,
            "message": f"Real-time sync disabled for {schema_name}.{table_name}"
        }
        
    except Exception as e:
        logger.error(f"Failed to disable realtime sync: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status")
async def get_sync_status(
    schema_name: Optional[str] = None,
    table_name: Optional[str] = None
):
    """
    Get synchronization status for schema/table
    """
    try:
        from app.core.database import db_pool
        
        # Build query based on parameters
        if schema_name and table_name:
            query = """
                SELECT * FROM atabot.sync_status
                WHERE schema_name = $1 AND table_name = $2
            """
            result = await db_pool.fetchrow(query, schema_name, table_name)
            if result:
                return dict(result)
            else:
                raise HTTPException(status_code=404, detail="Table sync status not found")
                
        elif schema_name:
            query = """
                SELECT * FROM atabot.sync_status
                WHERE schema_name = $1
                ORDER BY table_name
            """
            results = await db_pool.fetch(query, schema_name)
            return [dict(row) for row in results]
            
        else:
            # Get all sync statuses
            query = """
                SELECT 
                    schema_name,
                    COUNT(*) as total_tables,
                    SUM(CASE WHEN sync_status = 'completed' THEN 1 ELSE 0 END) as synced_tables,
                    SUM(rows_synced) as total_rows_synced,
                    MAX(last_sync_completed) as last_sync
                FROM atabot.sync_status
                GROUP BY schema_name
                ORDER BY schema_name
            """
            results = await db_pool.fetch(query)
            return [dict(row) for row in results]
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get sync status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/jobs")
async def get_active_jobs():
    """
    Get list of active sync jobs
    """
    try:
        jobs = sync_service.get_active_jobs()
        return {
            "success": True,
            "active_jobs": jobs,
            "total": len(jobs)
        }
        
    except Exception as e:
        logger.error(f"Failed to get active jobs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/jobs/{job_id}", response_model=SyncStatusResponse)
async def get_job_status(job_id: str):
    """
    Get status of a specific sync job
    """
    try:
        job_status = sync_service.get_job_status(job_id)
        
        if not job_status:
            raise HTTPException(status_code=404, detail="Job not found")
        
        return SyncStatusResponse(
            job_id=job_id,
            status=job_status.get("status"),
            started_at=job_status.get("started_at"),
            completed_at=job_status.get("completed_at"),
            progress=job_status.get("progress", {}),
            error=job_status.get("error"),
            result=job_status.get("result")
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get job status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/cache")
async def clear_sync_cache(
    schema_name: Optional[str] = None,
    table_name: Optional[str] = None
):
    """
    Clear cached embeddings for schema/table
    """
    try:
        from app.core.database import db_pool
        
        if schema_name and table_name:
            query = """
                DELETE FROM atabot.embeddings
                WHERE schema_name = $1 AND table_name = $2
            """
            await db_pool.execute(query, schema_name, table_name)
            message = f"Cleared embeddings for {schema_name}.{table_name}"
            
        elif schema_name:
            query = """
                DELETE FROM atabot.embeddings
                WHERE schema_name = $1
            """
            await db_pool.execute(query, schema_name)
            message = f"Cleared all embeddings for schema {schema_name}"
            
        else:
            raise HTTPException(
                status_code=400,
                detail="Please specify schema_name and optionally table_name"
            )
        
        return {
            "success": True,
            "message": message
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to clear cache: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/statistics")
async def get_sync_statistics():
    """
    Get overall synchronization statistics
    """
    try:
        from app.core.database import db_pool
        
        stats_query = """
            SELECT 
                COUNT(DISTINCT schema_name) as total_schemas,
                COUNT(*) as total_tables,
                SUM(rows_synced) as total_rows_synced,
                COUNT(CASE WHEN sync_status = 'completed' THEN 1 END) as completed_syncs,
                COUNT(CASE WHEN sync_status = 'failed' THEN 1 END) as failed_syncs,
                COUNT(CASE WHEN realtime_enabled = true THEN 1 END) as realtime_enabled_tables,
                MIN(last_sync_completed) as earliest_sync,
                MAX(last_sync_completed) as latest_sync
            FROM atabot.sync_status
        """
        
        stats = await db_pool.fetchrow(stats_query)
        
        # Get embedding statistics
        embedding_query = """
            SELECT 
                COUNT(*) as total_embeddings,
                COUNT(DISTINCT schema_name) as schemas_with_embeddings,
                COUNT(DISTINCT table_name) as tables_with_embeddings,
                pg_size_pretty(pg_total_relation_size('atabot.embeddings')) as storage_size
            FROM atabot.embeddings
        """
        
        embedding_stats = await db_pool.fetchrow(embedding_query)
        
        return {
            "sync_stats": dict(stats) if stats else {},
            "embedding_stats": dict(embedding_stats) if embedding_stats else {},
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to get statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e))