"""
Pydantic models for schema-related API requests and responses
"""
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime

class SchemaInfo(BaseModel):
    """Basic schema information"""
    name: str
    display_name: str
    is_managed: bool = False
    is_active: bool = False
    total_tables: int = 0
    total_rows: int = 0
    business_domain: Optional[str] = None
    last_synced_at: Optional[datetime] = None

class SchemaListResponse(BaseModel):
    """Response for schema list endpoint"""
    success: bool
    schemas: List[SchemaInfo]
    total: int

class TableColumn(BaseModel):
    """Table column information"""
    name: str
    type: str
    nullable: bool
    is_primary_key: bool = False
    is_foreign_key: bool = False

class TableInfo(BaseModel):
    """Table information"""
    name: str
    row_count: int
    column_count: int
    sync_status: str = "not_synced"
    last_synced: Optional[datetime] = None
    synced_rows: int = 0
    columns: List[Dict[str, Any]]

class SchemaAnalysis(BaseModel):
    """Schema analysis result"""
    schema_name: str
    business_domain: str
    total_tables: int
    total_rows: int
    table_analyses: Dict[str, Any]
    relationships: List[Dict[str, Any]]
    terminology: Dict[str, List[str]]
    entity_graph: Dict[str, Any]
    discovered_at: str