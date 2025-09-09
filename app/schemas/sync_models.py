"""
Pydantic models for sync-related API requests and responses
"""
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any

class SyncRequest(BaseModel):
    """Request for single table sync"""
    schema_name: str = Field(..., description="Schema name")
    table_name: str = Field(..., description="Table name to sync")
    force_full: bool = Field(default=False, description="Force full sync instead of incremental")

class BatchSyncRequest(BaseModel):
    """Request for batch table sync"""
    schema_name: str = Field(..., description="Schema name")
    tables: List[str] = Field(..., description="List of tables to sync")
    force_full: bool = Field(default=False, description="Force full sync for all tables")

class SyncResponse(BaseModel):
    """Response for sync initiation"""
    success: bool
    message: str
    job_id: str
    status: str

class SyncStatusResponse(BaseModel):
    """Response for sync status check"""
    job_id: str
    status: str
    started_at: str
    completed_at: Optional[str] = None
    progress: Dict[str, Any] = {}
    error: Optional[str] = None
    result: Optional[Dict[str, Any]] = None

class SyncResult(BaseModel):
    """Result of a sync operation"""
    mode: str
    rows_processed: int
    duration_seconds: float
    rows_per_second: float
    status: str