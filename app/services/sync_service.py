"""
Data Synchronization Service
Handles bulk and real-time sync of data to vector store
"""
from typing import Dict, List, Any, Optional
from datetime import datetime, date
import asyncio
import hashlib
import json
from loguru import logger
from decimal import Decimal
import uuid

from app.core.database import db_pool
from app.core.embeddings import embedding_service
from app.core.config import settings
from app.services.embedding_queue import embedding_queue

def quote_ident(name: str) -> str:
    """
    Quote PostgreSQL identifier
    """
    return f'"{name}"'

class SyncService:
    """
    Service for synchronizing database data to vector embeddings
    """
    
    def __init__(self):
        self.active_jobs: Dict[str, Dict[str, Any]] = {}
        self.sync_lock = asyncio.Lock()
    
    async def _ensure_schema_analyzed(self, schema: str) -> None:
        """
        Ensure schema has been analyzed before syncing
        """
        query = """
            SELECT COUNT(*) FROM atabot.managed_schemas
            WHERE schema_name = $1
        """
        count = await db_pool.fetchval(query, schema)
        
        if count == 0:
            logger.info(f"Schema {schema} not analyzed yet, performing quick analysis...")
            
            # Perform basic schema registration without full analysis
            # This prevents the string index error by ensuring proper structure
            from app.services.schema_analyzer import schema_analyzer
            
            try:
                # Perform full analysis
                await schema_analyzer.analyze_schema(schema)
            except Exception as e:
                logger.warning(f"Full schema analysis failed: {e}, using basic registration")
                
                # Fallback: Just register the schema with basic info
                tables = await db_pool.get_tables(schema)
                
                # Create basic metadata structure
                metadata = {}
                for table in tables:
                    table_name = table['table_name']
                    table_info = await db_pool.get_table_info(schema, table_name)
                    
                    # Create basic table metadata
                    metadata[table_name] = {
                        'entity_type': 'record',
                        'row_count': table.get('estimated_row_count', 0),
                        'columns': {},
                        'display_fields': [],
                        'searchable_fields': [],
                        'primary_key': None,
                        'foreign_keys': []
                    }
                    
                    # Extract column info
                    for col in table_info[:10]:  # Limit to first 10 columns for basic analysis
                        col_name = col['column_name']
                        metadata[table_name]['columns'][col_name] = {
                            'type': col['data_type'],
                            'nullable': col['is_nullable']
                        }
                        
                        # Identify primary key
                        if 'id' in col_name.lower() or col_name.lower() == 'uuid':
                            metadata[table_name]['primary_key'] = col_name
                        
                        # Add searchable text fields
                        if 'char' in col['data_type'].lower() or 'text' in col['data_type'].lower():
                            metadata[table_name]['searchable_fields'].append(col_name)
                            if len(metadata[table_name]['display_fields']) < 3:
                                metadata[table_name]['display_fields'].append(col_name)
                
                # Store basic schema info
                insert_query = """
                    INSERT INTO atabot.managed_schemas 
                    (schema_name, display_name, metadata, total_tables, is_active)
                    VALUES ($1, $2, $3, $4, true)
                    ON CONFLICT (schema_name) 
                    DO UPDATE SET
                        metadata = EXCLUDED.metadata,
                        total_tables = EXCLUDED.total_tables,
                        is_active = true
                """
                
                await db_pool.execute(
                    insert_query,
                    schema,
                    schema.replace('_', ' ').title(),
                    json.dumps(metadata),
                    len(tables)
                )
                
                logger.info(f"Basic schema registration completed for {schema}")
        
    async def sync_table(
        self,
        schema: str,
        table: str,
        mode: str = "incremental"
    ) -> Dict[str, Any]:
        """
        Sync a table to vector store
        
        Args:
            schema: Schema name
            table: Table name
            mode: 'full' for complete resync, 'incremental' for changes only
            
        Returns:
            Sync result with statistics
        """
        job_id = str(uuid.uuid4())
        start_time = datetime.now()
        
        # Register job
        self.active_jobs[job_id] = {
            "status": "running",
            "started_at": start_time.isoformat(),
            "schema": schema,
            "table": table,
            "mode": mode
        }
        
        try:
            logger.info(f"Starting {mode} sync for {schema}.{table}")
            
            # Ensure schema has been analyzed first
            await self._ensure_schema_analyzed(schema)
            
            # Get table info
            table_info = await db_pool.get_table_info(schema, table)
            if not table_info:
                raise ValueError(f"Table {schema}.{table} not found")
            
            # Check if sync tracking exists
            await self._ensure_sync_tracking(schema, table)
            
            if mode == "incremental":
                result = await self._incremental_sync(schema, table, table_info)
            else:
                result = await self._full_sync(schema, table, table_info)
            
            # Update job status
            self.active_jobs[job_id]["status"] = "completed"
            self.active_jobs[job_id]["completed_at"] = datetime.now().isoformat()
            self.active_jobs[job_id]["result"] = result
            
            return result
            
        except Exception as e:
            logger.error(f"Sync failed for {schema}.{table}: {e}")
            self.active_jobs[job_id]["status"] = "failed"
            self.active_jobs[job_id]["error"] = str(e)
            raise
    
    async def sync_table_with_job_id(
        self,
        job_id: str,
        schema: str,
        table: str,
        mode: str = "incremental"
    ) -> Dict[str, Any]:
        """
        Sync a table to vector store with pre-generated job_id
        
        Args:
            job_id: Pre-generated job ID
            schema: Schema name
            table: Table name
            mode: 'full' for complete resync, 'incremental' for changes only
            
        Returns:
            Sync result with statistics
        """
        start_time = datetime.now()
        
        # Register job with provided job_id
        self.active_jobs[job_id] = {
            "status": "running",
            "started_at": start_time.isoformat(),
            "schema": schema,
            "table": table,
            "mode": mode
        }
        
        try:
            logger.info(f"Starting {mode} sync for {schema}.{table} with job_id: {job_id}")
            
            # Ensure schema has been analyzed first
            await self._ensure_schema_analyzed(schema)
            
            # Get table info
            table_info = await db_pool.get_table_info(schema, table)
            if not table_info:
                raise ValueError(f"Table {schema}.{table} not found")
            
            # Check if sync tracking exists
            await self._ensure_sync_tracking(schema, table)
            
            if mode == "incremental":
                result = await self._incremental_sync(schema, table, table_info)
            else:
                result = await self._full_sync(schema, table, table_info)
            
            # Update job status
            self.active_jobs[job_id]["status"] = "completed"
            self.active_jobs[job_id]["completed_at"] = datetime.now().isoformat()
            self.active_jobs[job_id]["result"] = result
            
            return result
            
        except Exception as e:
            logger.error(f"Sync failed for {schema}.{table}: {e}")
            self.active_jobs[job_id]["status"] = "failed"
            self.active_jobs[job_id]["error"] = str(e)
            raise
    
    async def _full_sync(
        self,
        schema: str,
        table: str,
        table_info: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Perform full table synchronization
        """
        start_time = datetime.now()
        
        # Clear existing embeddings for this table
        await self._clear_table_embeddings(schema, table)
        
        # Get total row count
        count_query = f"""
            SELECT COUNT(*) as total 
            FROM {quote_ident(schema)}.{quote_ident(table)}
        """
        total_rows = await db_pool.fetchval(count_query)
        
        if total_rows == 0:
            logger.info(f"No data to sync in {schema}.{table}")
            return {
                "mode": "full",
                "rows_processed": 0,
                "duration_seconds": 0,
                "status": "completed"
            }
        
        # Process in batches
        batch_size = settings.SYNC_BATCH_SIZE
        rows_processed = 0
        
        # Get primary key column
        pk_column = self._get_primary_key(table_info)
        
        for offset in range(0, total_rows, batch_size):
            # Fetch batch
            if pk_column:
                query = f"""
                    SELECT * FROM {quote_ident(schema)}.{quote_ident(table)}
                    ORDER BY {quote_ident(pk_column)}
                    LIMIT $1 OFFSET $2
                """
            else:
                query = f"""
                    SELECT * FROM {quote_ident(schema)}.{quote_ident(table)}
                    LIMIT $1 OFFSET $2
                """
            
            rows = await db_pool.fetch(query, batch_size, offset)
            
            # Process batch
            await self._process_batch(schema, table, rows, table_info)
            
            rows_processed += len(rows)
            
            # Log progress
            progress = (rows_processed / total_rows) * 100
            logger.info(f"Sync progress for {schema}.{table}: {progress:.1f}% ({rows_processed}/{total_rows})")
        
        # Update sync status
        await self._update_sync_status(schema, table, "completed", rows_processed)
        
        duration = (datetime.now() - start_time).total_seconds()
        
        return {
            "mode": "full",
            "rows_processed": rows_processed,
            "duration_seconds": duration,
            "rows_per_second": rows_processed / duration if duration > 0 else 0,
            "status": "completed"
        }
    
    async def _incremental_sync(
        self,
        schema: str,
        table: str,
        table_info: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Perform incremental synchronization (only changed data)
        """
        start_time = datetime.now()
        
        # Get last sync timestamp
        last_sync = await self._get_last_sync_time(schema, table)
        
        # Find timestamp column
        timestamp_columns = [
            col['column_name'] for col in table_info
            if 'timestamp' in col['data_type'].lower() or 'date' in col['data_type'].lower()
        ]
        
        # Look for updated_at, modified_at, or created_at columns
        update_column = None
        for col in ['updated_at', 'modified_at', 'changed_at', 'last_modified', 'created_at']:
            if col in [c['column_name'] for c in table_info]:
                update_column = col
                break
        
        if not update_column and timestamp_columns:
            # Use first timestamp column
            update_column = timestamp_columns[0]
        
        if not update_column:
            logger.warning(f"No timestamp column found for incremental sync, falling back to full sync")
            return await self._full_sync(schema, table, table_info)
        
        # Query for changed rows
        if last_sync:
            query = f"""
                SELECT * FROM {quote_ident(schema)}.{quote_ident(table)}
                WHERE {quote_ident(update_column)} > $1
                ORDER BY {quote_ident(update_column)}
            """
            rows = await db_pool.fetch(query, last_sync)
        else:
            # First sync, get all rows
            query = f"""
                SELECT * FROM {quote_ident(schema)}.{quote_ident(table)}
            """
            rows = await db_pool.fetch(query)
        
        if not rows:
            logger.info(f"No changes to sync for {schema}.{table}")
            return {
                "mode": "incremental",
                "rows_processed": 0,
                "duration_seconds": 0,
                "status": "no_changes"
            }
        
        # Process rows in batches
        batch_size = settings.SYNC_BATCH_SIZE
        total_rows = len(rows)
        
        for i in range(0, total_rows, batch_size):
            batch = rows[i:i+batch_size]
            await self._process_batch(schema, table, batch, table_info)
        
        # Update sync status
        await self._update_sync_status(schema, table, "completed", total_rows)
        
        duration = (datetime.now() - start_time).total_seconds()
        
        return {
            "mode": "incremental",
            "rows_processed": total_rows,
            "duration_seconds": duration,
            "rows_per_second": total_rows / duration if duration > 0 else 0,
            "status": "completed"
        }
    
    async def _process_batch(
        self,
        schema: str,
        table: str,
        rows: List[Any],
        table_info: List[Dict[str, Any]]
    ) -> None:
        """
        Process a batch of rows using embedding queue
        """
        if not rows:
            return
        
        # Get learned patterns
        patterns = await self._get_learned_patterns(schema, table)
        
        # Prepare texts and metadata
        texts = []
        metadata_list = []
        ids = []
        
        pk_column = self._get_primary_key(table_info)
        
        for row in rows:
            row_dict = dict(row)
            text = self._generate_searchable_text(row_dict, table, patterns)
            texts.append(text)
            
            metadata = self._sanitize_metadata(row_dict)
            metadata['_schema'] = schema
            metadata['_table'] = table
            metadata_list.append(metadata)
            
            if pk_column and pk_column in row_dict:
                row_id = f"{schema}_{table}_{row_dict[pk_column]}"
            else:
                row_id = hashlib.md5(f"{schema}_{table}_{text}".encode()).hexdigest()
            ids.append(row_id)
        
        # USE EMBEDDING QUEUE instead of direct call!
        from app.services.embedding_queue import embedding_queue
        
        # Add to queue
        batch_id = await embedding_queue.add_batch(texts, metadata_list)
        logger.info(f"Added batch {batch_id} to embedding queue ({len(texts)} texts)")
        
        # Wait for processing (optional, atau bisa async)
        # Untuk sync operation, kita tunggu selesai
        while batch_id in embedding_queue.queue:
            await asyncio.sleep(1)
        
        # Get embeddings from cache
        embeddings = []
        for text in texts:
            text_hash = hashlib.md5(text.encode()).hexdigest()
            embedding = embedding_queue.cache.get(text_hash)
            if embedding:
                embeddings.append(embedding)
            else:
                # Fallback jika tidak ada di cache
                logger.warning(f"Embedding not found in cache for text hash: {text_hash}")
                embeddings.append([0.0] * settings.EMBEDDING_DIMENSIONS)
        
        # Store embeddings
        await self._store_embeddings(
            schema, table, ids, texts, embeddings, metadata_list
        )
    
    def _generate_searchable_text(
        self,
        row: Dict[str, Any],
        table: str,
        patterns: Dict[str, Any]
    ) -> str:
        """
        Generate natural language representation of row data
        """
        parts = []
        
        # Ensure patterns is a dictionary
        if not isinstance(patterns, dict):
            patterns = {}
        
        # Add table context
        entity_type = patterns.get('entity_type', 'record')
        if entity_type != 'unknown':
            parts.append(f"This is a {entity_type} from {table}")
        
        # Get display fields from patterns (with safe defaults)
        display_fields = patterns.get('display_fields', [])
        searchable_fields = patterns.get('searchable_fields', [])
        terminology = patterns.get('terminology', {})
        
        # Ensure fields are lists
        if not isinstance(display_fields, list):
            display_fields = []
        if not isinstance(searchable_fields, list):
            searchable_fields = []
        if not isinstance(terminology, dict):
            terminology = {}
        
        # Prioritize display fields
        important_fields = display_fields + searchable_fields
        
        # Process fields
        for key, value in row.items():
            if value is None or key.startswith('_'):
                continue
            
            # Skip binary or very long fields
            if isinstance(value, bytes):
                continue
            if isinstance(value, str) and len(value) > 1000:
                value = value[:1000] + "..."
            
            # Format based on type
            if isinstance(value, (datetime, date)):
                value = value.isoformat()
            elif isinstance(value, Decimal):
                value = float(value)
            
            # Use learned terminology or fallback to formatted key
            field_label = terminology.get(key, key.replace('_', ' '))
            
            # Add to text with priority
            if key in important_fields[:5]:  # Top 5 important fields
                parts.insert(1, f"{field_label}: {value}")
            else:
                parts.append(f"{field_label}: {value}")
        
        return ". ".join(parts[:20])  # Limit to 20 fields
    
    def _sanitize_metadata(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Sanitize data for JSON storage
        """
        sanitized = {}
        
        for key, value in data.items():
            if value is None:
                sanitized[key] = None
            elif isinstance(value, (str, int, float, bool)):
                sanitized[key] = value
            elif isinstance(value, (datetime, date)):
                sanitized[key] = value.isoformat()
            elif isinstance(value, Decimal):
                sanitized[key] = float(value)
            elif isinstance(value, bytes):
                sanitized[key] = f"<binary:{len(value)}>"
            elif isinstance(value, (list, dict)):
                sanitized[key] = json.dumps(value)
            else:
                sanitized[key] = str(value)
        
        return sanitized
    
    async def _store_embeddings(
        self,
        schema: str,
        table: str,
        ids: List[str],
        texts: List[str],
        embeddings: List[List[float]],
        metadata_list: List[Dict[str, Any]]
    ) -> None:
        """
        Store embeddings in database
        """
        # Convert embeddings to PostgreSQL vector format string
        def format_vector(embedding: List[float]) -> str:
            """Convert list of floats to PostgreSQL vector string format"""
            return '[' + ','.join(map(str, embedding)) + ']'
        
        # Prepare batch insert with proper vector formatting
        insert_query = """
            INSERT INTO atabot.embeddings 
            (id, schema_name, table_name, content, embedding, metadata, created_at)
            VALUES ($1, $2, $3, $4, $5::vector, $6, NOW())
            ON CONFLICT (id) DO UPDATE SET
                content = EXCLUDED.content,
                embedding = EXCLUDED.embedding,
                metadata = EXCLUDED.metadata,
                updated_at = NOW()
        """
        
        # Batch insert with formatted vectors
        batch_data = []
        for i in range(len(ids)):
            batch_data.append((
                ids[i],
                schema,
                table,
                texts[i],
                format_vector(embeddings[i]),
                json.dumps(metadata_list[i])
            ))
        
        await db_pool.execute_many(insert_query, batch_data)
        logger.debug(f"Stored {len(batch_data)} embeddings for {schema}.{table}")
    
    async def _clear_table_embeddings(self, schema: str, table: str) -> None:
        """
        Clear all embeddings for a table
        """
        query = """
            DELETE FROM atabot.embeddings
            WHERE schema_name = $1 AND table_name = $2
        """
        await db_pool.execute(query, schema, table)
        logger.info(f"Cleared embeddings for {schema}.{table}")
    
    async def _ensure_sync_tracking(self, schema: str, table: str) -> None:
        """
        Ensure sync tracking record exists
        """
        query = """
            INSERT INTO atabot.sync_status (schema_name, table_name, sync_status)
            VALUES ($1, $2, 'pending')
            ON CONFLICT (schema_name, table_name) DO NOTHING
        """
        await db_pool.execute(query, schema, table)
    
    async def _update_sync_status(
        self,
        schema: str,
        table: str,
        status: str,
        rows_synced: int
    ) -> None:
        """
        Update sync status tracking
        """
        query = """
            UPDATE atabot.sync_status
            SET sync_status = $1,
                last_sync_completed = NOW(),
                rows_synced = $2
            WHERE schema_name = $3 AND table_name = $4
        """
        await db_pool.execute(query, status, rows_synced, schema, table)
    
    async def _get_last_sync_time(
        self,
        schema: str,
        table: str
    ) -> Optional[datetime]:
        """
        Get last successful sync timestamp
        """
        query = """
            SELECT last_sync_completed
            FROM atabot.sync_status
            WHERE schema_name = $1 AND table_name = $2
        """
        result = await db_pool.fetchval(query, schema, table)
        return result
    
    def _get_primary_key(self, table_info: List[Dict[str, Any]]) -> Optional[str]:
        """
        Extract primary key column from table info
        """
        for col in table_info:
            # Look for common primary key patterns
            col_name = col['column_name'].lower()
            if col_name in ['id', 'uuid', 'guid']:
                return col['column_name']
            if col_name.endswith('_id') or col_name.endswith('_uuid'):
                return col['column_name']
            # Check if it's a serial/bigserial
            if 'serial' in col['data_type'].lower():
                return col['column_name']
        
        return None
    
    async def _get_learned_patterns(
        self,
        schema: str,
        table: str
    ) -> Dict[str, Any]:
        """
        Get learned patterns for a table from schema analysis
        """
        query = """
            SELECT metadata, learned_patterns
            FROM atabot.managed_schemas
            WHERE schema_name = $1
        """
        
        result = await db_pool.fetchrow(query, schema)
        
        if not result:
            return {}
        
        patterns = {}
        
        # Extract table-specific patterns
        # JSONB columns are already parsed by asyncpg
        metadata = result['metadata']
        learned_patterns = result['learned_patterns']
        
        if metadata:
            try:
                # Handle both dict and string formats for safety
                if isinstance(metadata, str):
                    metadata = json.loads(metadata)
                
                # Check if metadata contains the table
                if isinstance(metadata, dict) and table in metadata:
                    table_data = metadata[table]
                    
                    # Ensure table_data is a dictionary
                    if isinstance(table_data, dict):
                        patterns = table_data
                    else:
                        logger.warning(f"Table data for {table} is not a dictionary: {type(table_data)}")
                        patterns = {}
            except (json.JSONDecodeError, TypeError) as e:
                logger.warning(f"Failed to parse metadata for {schema}.{table}: {e}")
                patterns = {}
        
        # Add learned patterns
        if learned_patterns:
            try:
                # Handle both dict and string formats for safety
                if isinstance(learned_patterns, str):
                    learned = json.loads(learned_patterns)
                else:
                    learned = learned_patterns
                
                if isinstance(learned, dict):
                    # Safely merge terminology if it exists
                    if 'terminology' in learned and isinstance(learned['terminology'], dict):
                        if 'terminology' not in patterns:
                            patterns['terminology'] = {}
                        patterns['terminology'].update(learned['terminology'])
            except (json.JSONDecodeError, TypeError) as e:
                logger.warning(f"Failed to parse learned patterns for {schema}: {e}")
        
        # Ensure patterns has the expected structure
        if not isinstance(patterns, dict):
            patterns = {}
        
        # Add default values for expected fields if they don't exist
        patterns.setdefault('entity_type', 'record')
        patterns.setdefault('display_fields', [])
        patterns.setdefault('searchable_fields', [])
        patterns.setdefault('terminology', {})
        
        return patterns
    
    async def sync_schema(
        self,
        schema: str,
        tables: Optional[List[str]] = None,
        mode: str = "incremental"
    ) -> Dict[str, Any]:
        """
        Sync entire schema or specific tables (backward compatibility)
        """
        job_id = str(uuid.uuid4())
        return await self.sync_schema_with_job_id(job_id, schema, tables, mode)
    
    async def sync_schema_with_job_id(
        self,
        job_id: str,
        schema: str,
        tables: Optional[List[str]] = None,
        mode: str = "incremental"
    ) -> Dict[str, Any]:
        """
        Sync entire schema with pre-generated job_id
        """
        logger.info(f"Starting schema sync for {schema} with job_id: {job_id}")
        
        # Register main job
        self.active_jobs[job_id] = {
            "status": "running",
            "started_at": datetime.now().isoformat(),
            "schema": schema,
            "mode": mode,
            "type": "schema_sync"
        }
        
        # Ensure schema has been analyzed first
        await self._ensure_schema_analyzed(schema)
        
        # Get tables to sync
        if not tables:
            all_tables = await db_pool.get_tables(schema)
            tables = [t['table_name'] for t in all_tables]
        
        results = {}
        failed = []
        
        for table in tables:
            try:
                # Create sub-job for each table
                sub_job_id = f"{job_id}_{table}"
                result = await self.sync_table_with_job_id(sub_job_id, schema, table, mode)
                results[table] = result
            except Exception as e:
                logger.error(f"Failed to sync {schema}.{table}: {e}")
                failed.append(table)
                results[table] = {"status": "failed", "error": str(e)}
        
        # Update main job
        self.active_jobs[job_id]["status"] = "completed" if not failed else "partial"
        self.active_jobs[job_id]["completed_at"] = datetime.now().isoformat()
        self.active_jobs[job_id]["result"] = {
            "schema": schema,
            "tables_synced": len(tables) - len(failed),
            "tables_failed": len(failed),
            "failed_tables": failed,
            "results": results
        }
        
        return self.active_jobs[job_id]["result"]
    
    async def enable_realtime_sync(
        self,
        schema: str,
        table: str
    ) -> Dict[str, Any]:
        """
        Enable real-time synchronization for a table
        """
        try:
            # Create trigger
            await db_pool.create_trigger(schema, table)
            
            # Update sync status
            query = """
                UPDATE atabot.sync_status
                SET realtime_enabled = true
                WHERE schema_name = $1 AND table_name = $2
            """
            await db_pool.execute(query, schema, table)
            
            logger.info(f"Real-time sync enabled for {schema}.{table}")
            
            return {
                "success": True,
                "message": f"Real-time sync enabled for {schema}.{table}"
            }
            
        except Exception as e:
            logger.error(f"Failed to enable real-time sync: {e}")
            raise
    
    def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Get status of a sync job
        """
        return self.active_jobs.get(job_id)
    
    def get_active_jobs(self) -> List[Dict[str, Any]]:
        """
        Get all active sync jobs
        """
        return [
            {"job_id": job_id, **job_data}
            for job_id, job_data in self.active_jobs.items()
        ]

# Global sync service instance
sync_service = SyncService()