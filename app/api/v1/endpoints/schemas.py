"""
Schema Management API Endpoints
"""
from fastapi import APIRouter, HTTPException
from typing import Dict, Any, Optional
from datetime import datetime
from loguru import logger

from app.core.database import db_pool
from app.core.mcp import mcp_orchestrator
from app.schemas.schema_models import (
    SchemaInfo,
    SchemaListResponse
)

router = APIRouter()

@router.get("/", response_model=SchemaListResponse)
async def list_schemas():
    """
    List all available database schemas with their status
    """
    try:
        # Get all schemas from database
        all_schemas = await db_pool.get_schemas()
        
        # Get managed schemas info
        query = """
            SELECT 
                schema_name,
                display_name,
                is_active,
                total_tables,
                total_rows,
                business_domain,
                last_synced_at
            FROM atabot.managed_schemas
        """
        
        managed = await db_pool.fetch(query)
        managed_dict = {row["schema_name"]: dict(row) for row in managed}
        
        # Build response
        schemas = []
        for schema_name in all_schemas:
            if schema_name == "atabot":  # Skip our system schema
                continue
                
            schema_info = managed_dict.get(schema_name, {})
            schemas.append(SchemaInfo(
                name=schema_name,
                display_name=schema_info.get("display_name", schema_name.replace("_", " ").title()),
                is_managed=schema_name in managed_dict,
                is_active=schema_info.get("is_active", False),
                total_tables=schema_info.get("total_tables", 0),
                total_rows=schema_info.get("total_rows", 0),
                business_domain=schema_info.get("business_domain"),
                last_synced_at=schema_info.get("last_synced_at")
            ))
        
        return SchemaListResponse(
            success=True,
            schemas=schemas,
            total=len(schemas)
        )
        
    except Exception as e:
        logger.error(f"Failed to list schemas: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/{schema_name}/analyze")
async def analyze_schema(schema_name: str) -> Dict[str, Any]:
    """
    Analyze a schema to understand its structure and patterns
    """
    try:
        logger.info(f"Starting analysis of schema: {schema_name}")
        
        # Use MCP to execute schema analysis
        result = await mcp_orchestrator.process_request(
            {
                "action": "execute_tool",
                "tool": "analyze_schema",
                "params": {"schema": schema_name}
            },
            session_id=f"analyze_{schema_name}_{datetime.now().timestamp()}"
        )
        
        if not result.get("success"):
            raise HTTPException(status_code=500, detail="Analysis failed")
        
        analysis = result["result"]
        
        return {
            "success": True,
            "message": f"Schema {schema_name} analyzed successfully",
            "analysis": analysis
        }
        
    except Exception as e:
        logger.error(f"Failed to analyze schema {schema_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/{schema_name}/activate")
async def activate_schema(schema_name: str, session_id: Optional[str] = None):
    """
    Activate a schema for the current session
    """
    try:
        # Verify schema exists
        schemas = await db_pool.get_schemas()
        if schema_name not in schemas:
            raise HTTPException(status_code=404, detail=f"Schema {schema_name} not found")
        
        # Update managed schemas
        query = """
            INSERT INTO atabot.managed_schemas (schema_name, display_name, is_active)
            VALUES ($1, $2, true)
            ON CONFLICT (schema_name) 
            DO UPDATE SET is_active = true
        """
        
        await db_pool.execute(
            query,
            schema_name,
            schema_name.replace("_", " ").title()
        )
        
        # Set in MCP context if session provided
        if session_id:
            await mcp_orchestrator.process_request(
                {
                    "action": "set_schema",
                    "schema": schema_name
                },
                session_id=session_id
            )
        
        return {
            "success": True,
            "message": f"Schema {schema_name} activated",
            "schema": schema_name
        }
        
    except Exception as e:
        logger.error(f"Failed to activate schema {schema_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{schema_name}/tables")
async def get_schema_tables(schema_name: str):
    """
    Get detailed information about tables in a schema
    """
    try:
        # Get tables with row counts
        tables = await db_pool.get_tables(schema_name)
        
        # Get sync status for each table
        sync_query = """
            SELECT 
                table_name,
                sync_status,
                last_sync_completed,
                rows_synced
            FROM atabot.sync_status
            WHERE schema_name = $1
        """
        
        sync_status = await db_pool.fetch(sync_query, schema_name)
        sync_dict = {row["table_name"]: dict(row) for row in sync_status}
        
        # Build response
        table_list = []
        for table in tables:
            table_name = table["table_name"]
            sync_info = sync_dict.get(table_name, {})
            
            # Get column info
            columns = await db_pool.get_table_info(schema_name, table_name)
            
            table_list.append({
                "name": table_name,
                "row_count": table["estimated_row_count"],
                "column_count": len(columns),
                "sync_status": sync_info.get("sync_status", "not_synced"),
                "last_synced": sync_info.get("last_sync_completed"),
                "synced_rows": sync_info.get("rows_synced", 0),
                "columns": [
                    {
                        "name": col["column_name"],
                        "type": col["data_type"],
                        "nullable": col["is_nullable"]
                    }
                    for col in columns[:10]  # Limit to first 10 columns
                ]
            })
        
        return {
            "success": True,
            "schema": schema_name,
            "tables": table_list,
            "total_tables": len(table_list)
        }
        
    except Exception as e:
        logger.error(f"Failed to get tables for schema {schema_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{schema_name}/statistics")
async def get_schema_statistics(schema_name: str):
    """
    Get detailed statistics about a schema
    """
    try:
        # Get managed schema info
        query = """
            SELECT *
            FROM atabot.managed_schemas
            WHERE schema_name = $1
        """
        
        schema_info = await db_pool.fetchrow(query, schema_name)
        
        if not schema_info:
            # Schema not analyzed yet
            return {
                "success": False,
                "message": f"Schema {schema_name} has not been analyzed yet"
            }
        
        # Get embedding statistics
        embedding_query = """
            SELECT 
                COUNT(*) as total_embeddings,
                COUNT(DISTINCT table_name) as tables_with_embeddings,
                MAX(created_at) as latest_embedding,
                MIN(created_at) as oldest_embedding
            FROM atabot.embeddings
            WHERE schema_name = $1
        """
        
        embedding_stats = await db_pool.fetchrow(embedding_query, schema_name)
        
        # Get query statistics
        query_stats_query = """
            SELECT 
                COUNT(*) as total_queries,
                AVG(response_time_ms) as avg_response_time,
                COUNT(DISTINCT session_id) as unique_sessions
            FROM atabot.query_logs
            WHERE created_at > NOW() - INTERVAL '7 days'
        """
        
        query_stats = await db_pool.fetchrow(query_stats_query)
        
        # Handle learned_patterns safely (JSONB is already parsed)
        learned_patterns = schema_info["learned_patterns"]
        if learned_patterns and isinstance(learned_patterns, str):
            # Only parse if it's a string
            import json
            try:
                learned_patterns = json.loads(learned_patterns)
            except json.JSONDecodeError:
                learned_patterns = {}
        elif not learned_patterns:
            learned_patterns = {}
        
        return {
            "success": True,
            "schema": schema_name,
            "general": {
                "display_name": schema_info["display_name"],
                "business_domain": schema_info["business_domain"],
                "total_tables": schema_info["total_tables"],
                "total_rows": schema_info["total_rows"],
                "is_active": schema_info["is_active"],
                "discovered_at": schema_info.get("discovered_at"),
                "last_synced_at": schema_info["last_synced_at"]
            },
            "embeddings": dict(embedding_stats) if embedding_stats else {},
            "queries": dict(query_stats) if query_stats else {},
            "learned_patterns": learned_patterns
        }
        
    except Exception as e:
        logger.error(f"Failed to get statistics for schema {schema_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
@router.delete("/{schema_name}")
async def deactivate_schema(schema_name: str):
    """
    Deactivate a schema (does not delete data)
    """
    try:
        query = """
            UPDATE atabot.managed_schemas
            SET is_active = false
            WHERE schema_name = $1
        """
        
        await db_pool.execute(query, schema_name)
        
        return {
            "success": True,
            "message": f"Schema {schema_name} deactivated"
        }
        
    except Exception as e:
        logger.error(f"Failed to deactivate schema {schema_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{schema_name}/relationships")
async def get_schema_relationships(schema_name: str):
    """
    Get relationship graph for a schema
    """
    try:
        # Get foreign keys
        foreign_keys = await db_pool.get_foreign_keys(schema_name)
        
        # Get learned patterns if available
        query = """
            SELECT learned_patterns
            FROM atabot.managed_schemas
            WHERE schema_name = $1
        """
        
        result = await db_pool.fetchrow(query, schema_name)
        
        relationships = []
        
        # Add explicit foreign keys
        for fk in foreign_keys:
            relationships.append({
                "from": f"{fk['table_name']}.{fk['column_name']}",
                "to": f"{fk['foreign_table_name']}.{fk['foreign_column_name']}",
                "type": "foreign_key"
            })
        
        # Add learned relationships if available
        if result and result["learned_patterns"]:
            import json
            patterns = json.loads(result["learned_patterns"])
            if "relationships" in patterns:
                for rel in patterns["relationships"]:
                    if rel["type"] == "implicit":
                        relationships.append(rel)
        
        return {
            "success": True,
            "schema": schema_name,
            "relationships": relationships,
            "total": len(relationships)
        }
        
    except Exception as e:
        logger.error(f"Failed to get relationships for schema {schema_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))