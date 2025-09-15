"""
Data Synchronization Service
Handles bulk and real-time sync of data to vector store
"""
from typing import Dict, List, Any, Optional
from datetime import datetime, date, timezone
import asyncio
import hashlib
import json
from loguru import logger
from decimal import Decimal
import uuid

from app.core.database import db_pool
from app.core.config import settings

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
        self.sync_jobs = {}
        self.batch_size = settings.SYNC_BATCH_SIZE
    
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
        mode: str = "full",
        job_id: Optional[str] = None
    ):
        """
        Sync a single table with proper pattern handling
        """
        if not job_id:
            job_id = f"{datetime.now(timezone.utc).timestamp()}_{schema}_{table}"
        
        logger.info(f"Starting {mode} sync for {schema}.{table} with job_id: {job_id}")
        
        try:
            # Clear existing embeddings if full sync
            if mode == "full":
                await self._clear_table_embeddings(schema, table)
            
            # Get patterns from database (bukan dari analyze_table yang tidak ada!)
            patterns = await self._get_table_patterns(schema, table)
            
            # Get row count for progress tracking
            row_count = await self._get_row_count(schema, table)
            if row_count == 0:
                logger.info(f"No rows to sync in {schema}.{table}")
                return
            
            # Process in batches
            offset = 0
            total_synced = 0
            
            while offset < row_count:
                # Fetch batch of rows
                rows = await self._fetch_batch(schema, table, offset, self.batch_size)
                if not rows:
                    break
                
                # Process this batch
                success_count = await self._process_batch_with_validation(
                    schema, table, rows, patterns
                )
                
                total_synced += success_count
                offset += self.batch_size
                
                # Update progress (sebagai dict, bukan integer!)
                progress = min((offset / row_count) * 100, 100)
                if job_id in self.sync_jobs:
                    self.sync_jobs[job_id]["progress"] = {
                        "percentage": progress,
                        "rows_processed": total_synced,
                        "total_rows": row_count
                    }
                logger.info(f"Sync progress for {schema}.{table}: {progress:.1f}% ({total_synced}/{row_count})")
            
            # Update sync status dengan kolom yang benar
            await self._update_sync_status_correct(schema, table, "completed", total_synced)
            logger.info(f"Successfully synced {total_synced} embeddings for {schema}.{table}")
            
        except Exception as e:
            logger.error(f"Sync failed for {schema}.{table}: {e}")
            await self._update_sync_status_correct(schema, table, "failed", 0, str(e))
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
        start_time = datetime.now(timezone.utc)

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
            self.active_jobs[job_id]["completed_at"] = datetime.now(timezone.utc).isoformat()
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
        start_time = datetime.now(timezone.utc)
        
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
        await self._update_sync_status_correct(schema, table, "completed", rows_processed)
        
        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        
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
        start_time = datetime.now(timezone.utc)
        
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
            logger.info(f"No timestamp column found for {schema}.{table}, attempting to add one...")

            # Try to add updated_at column and trigger (adaptive)
            try:
                await self._ensure_timestamp_column(schema, table)
                update_column = 'updated_at'

                # Re-get table info to include new column
                table_info = await db_pool.get_table_info(schema, table)

            except Exception as e:
                logger.warning(f"Failed to add timestamp column to {schema}.{table}: {e}")
                logger.warning(f"Falling back to full sync for {schema}.{table}")
                return await self._full_sync(schema, table, table_info)
        
        # Query for changed rows
        if last_sync:
            # Ensure last_sync is timezone-aware for proper comparison
            if last_sync.tzinfo is None:
                last_sync = last_sync.replace(tzinfo=timezone.utc)

            query = f"""
                SELECT * FROM {quote_ident(schema)}.{quote_ident(table)}
                WHERE {quote_ident(update_column)} > $1::timestamptz
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
        await self._update_sync_status_correct(schema, table, "completed", total_rows)
        
        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        
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
        
        # Wait for batch processing to complete
        success = await embedding_queue.wait_for_batch(batch_id, timeout=300)

        if not success:
            logger.error(f"Batch {batch_id} failed or timed out")
            return  # Don't store zero vectors

        # Get embeddings from cache
        embeddings = []
        valid_ids = []
        valid_texts = []
        valid_metadata = []

        for i, text in enumerate(texts):
            text_hash = hashlib.md5(text.encode()).hexdigest()
            embedding = embedding_queue.cache.get(text_hash)
            if embedding and len(embedding) > 0:
                # Validate embedding is not all zeros
                non_zero_count = sum(1 for v in embedding if v != 0)
                if non_zero_count > len(embedding) * 0.1:
                    embeddings.append(embedding)
                    valid_ids.append(ids[i])
                    valid_texts.append(texts[i])
                    valid_metadata.append(metadata_list[i])
                else:
                    logger.warning(f"Skipping zero embedding for text: {text[:100]}...")
            else:
                logger.warning(f"No valid embedding found for text: {text[:100]}...")

        if not embeddings:
            logger.error("No valid embeddings to store")
            return
        
        # Store only valid embeddings
        await self._store_embeddings(
            schema, table, valid_ids, valid_texts, embeddings, valid_metadata
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
        Sanitize data for JSON storage with proper validation
        """
        sanitized = {}

        for key, value in data.items():
            try:
                if value is None:
                    sanitized[key] = None
                elif isinstance(value, (str, int, float, bool)):
                    # Validate string length to prevent truncation issues
                    if isinstance(value, str) and len(value) > 1000:
                        sanitized[key] = value[:1000] + "..."
                        logger.warning(f"Truncated long string field '{key}' to 1000 characters")
                    else:
                        sanitized[key] = value
                elif isinstance(value, (datetime, date)):
                    sanitized[key] = value.isoformat()
                elif isinstance(value, Decimal):
                    sanitized[key] = float(value)
                elif isinstance(value, bytes):
                    sanitized[key] = f"<binary:{len(value)}>"
                elif isinstance(value, (list, dict)):
                    # Ensure JSON serialization doesn't fail
                    try:
                        json_str = json.dumps(value, default=str)
                        if len(json_str) > 2000:
                            sanitized[key] = json_str[:2000] + "..."
                            logger.warning(f"Truncated long JSON field '{key}' to 2000 characters")
                        else:
                            sanitized[key] = json_str
                    except (TypeError, ValueError) as e:
                        logger.warning(f"Failed to serialize field '{key}': {e}, converting to string")
                        sanitized[key] = str(value)[:1000]
                else:
                    str_value = str(value)
                    if len(str_value) > 1000:
                        sanitized[key] = str_value[:1000] + "..."
                        logger.warning(f"Truncated long field '{key}' to 1000 characters")
                    else:
                        sanitized[key] = str_value
            except Exception as e:
                logger.error(f"Error sanitizing field '{key}' with value '{value}': {e}")
                sanitized[key] = f"<error:{str(e)}>"

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
        
        # Batch insert with formatted vectors and proper metadata handling
        batch_data = []
        for i in range(len(ids)):
            try:
                # Ensure metadata is properly serialized
                metadata = metadata_list[i]
                if not isinstance(metadata, dict):
                    logger.warning(f"Metadata for ID {ids[i]} is not a dict: {type(metadata)}")
                    metadata = {}

                # Serialize metadata with error handling
                try:
                    metadata_json = json.dumps(metadata, default=str, ensure_ascii=False)
                except (TypeError, ValueError) as e:
                    logger.error(f"Failed to serialize metadata for ID {ids[i]}: {e}")
                    # Create a safe fallback metadata
                    metadata_json = json.dumps({
                        "_error": f"Failed to serialize original metadata: {str(e)}",
                        "_original_type": str(type(metadata)),
                        "id": ids[i] if len(ids) > i else "unknown"
                    })

                batch_data.append((
                    ids[i],
                    schema,
                    table,
                    texts[i],
                    format_vector(embeddings[i]),
                    metadata_json
                ))
            except Exception as e:
                logger.error(f"Failed to prepare batch data for ID {ids[i] if len(ids) > i else 'unknown'}: {e}")
                # Skip this record to prevent the entire batch from failing
                continue
        
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
    
    async def _update_sync_status_correct(
        self,
        schema: str,
        table: str,
        status: str,
        rows_synced: int,
        error: Optional[str] = None
    ):
        """
        Update sync status dengan kolom yang benar dari database
        """
        try:
            if status == "completed":
                await db_pool.execute("""
                    INSERT INTO atabot.sync_status 
                    (schema_name, table_name, sync_status, last_sync_completed, rows_synced)
                    VALUES ($1, $2, $3, NOW(), $4)
                    ON CONFLICT (schema_name, table_name)
                    DO UPDATE SET 
                        sync_status = $3,
                        last_sync_completed = NOW(),
                        rows_synced = $4,
                        last_error = NULL
                """, schema, table, status, rows_synced)
            else:
                await db_pool.execute("""
                    INSERT INTO atabot.sync_status 
                    (schema_name, table_name, sync_status, last_error)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (schema_name, table_name)
                    DO UPDATE SET 
                        sync_status = $3,
                        last_error = $4
                """, schema, table, status, error)
        except Exception as e:
            logger.error(f"Failed to update sync status: {e}")
    
    async def _get_last_sync_time(
        self,
        schema: str,
        table: str
    ) -> Optional[datetime]:
        """
        Get last successful sync timestamp with timezone handling
        """
        query = """
            SELECT last_sync_completed
            FROM atabot.sync_status
            WHERE schema_name = $1 AND table_name = $2
        """
        result = await db_pool.fetchval(query, schema, table)

        # Ensure timezone consistency
        if result:
            if result.tzinfo is None:
                # Make timezone-naive datetime timezone-aware (assume UTC)
                result = result.replace(tzinfo=timezone.utc)
            elif result.tzinfo != timezone.utc:
                # Convert to UTC if it's in a different timezone
                result = result.astimezone(timezone.utc)

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
        mode: str = "incremental",
        job_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Sync entire schema or specific tables (backward compatibility)
        """
        if job_id is None:
            job_id = str(uuid.uuid4())
        return await self.sync_schema_with_job_id(job_id, schema, tables, mode)
    
    async def sync_schema_with_job_id(
        self,
        job_id: str,
        schema: str,
        tables: Optional[List[str]] = None,
        mode: str = "full"
    ):
        """Sync schema with proper job tracking"""
        try:
            # Initialize job dengan format yang benar
            self.sync_jobs[job_id] = {
                "status": "running",
                "schema": schema,
                "tables": tables or [],
                "progress": {  # Progress sebagai dict, bukan integer!
                    "percentage": 0,
                    "tables_completed": 0,
                    "total_tables": 0
                },
                "started_at": datetime.now(timezone.utc).isoformat(),  # Convert ke string!
                "errors": []
            }
            
            # Ensure schema has been analyzed first
            await self._ensure_schema_analyzed(schema)

            # Get all tables if none specified
            if not tables:
                table_list = await db_pool.get_tables(schema)
                tables = [table['table_name'] for table in table_list]

            total_tables = len(tables)
            self.sync_jobs[job_id]["progress"]["total_tables"] = total_tables

            # Sync each table
            for i, table_name in enumerate(tables):
                try:
                    await self.sync_table_with_job_id(
                        f"{job_id}_{table_name}",
                        schema,
                        table_name,
                        mode
                    )

                    # Update progress
                    self.sync_jobs[job_id]["progress"]["tables_completed"] = i + 1
                    self.sync_jobs[job_id]["progress"]["percentage"] = ((i + 1) / total_tables) * 100

                except Exception as e:
                    logger.error(f"Failed to sync table {schema}.{table_name}: {e}")
                    self.sync_jobs[job_id]["errors"].append(f"Table {table_name}: {str(e)}")
            
            self.sync_jobs[job_id]["status"] = "completed"
            self.sync_jobs[job_id]["completed_at"] = datetime.now().isoformat()  # Convert ke string!
            
        except Exception as e:
            logger.error(f"Schema sync failed for {schema}: {e}")
            if job_id in self.sync_jobs:
                self.sync_jobs[job_id]["status"] = "failed"
                self.sync_jobs[job_id]["errors"].append(str(e))
                self.sync_jobs[job_id]["completed_at"] = datetime.now(timezone.utc).isoformat()
    
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
    
    async def _process_embeddings(
        self,
        schema: str,
        table: str,
        ids: List[str],
        texts: List[str],
        metadata_list: List[Dict[str, Any]]
    ) -> None:
        """
        Generate and store embeddings for texts
        """
        if not texts:
            return
        
        logger.info(f"Processing {len(texts)} embeddings for {schema}.{table}")
        
        # Import the embedding queue
        from app.services.embedding_queue import embedding_queue
        
        # Add to queue and get batch ID
        batch_id = await embedding_queue.add_batch(texts, metadata_list)
        logger.info(f"Added batch {batch_id} to embedding queue ({len(texts)} texts)")
        
        # Wait for batch processing with timeout
        logger.info(f"Waiting for batch {batch_id} to complete...")
        success = await embedding_queue.wait_for_batch(batch_id, timeout=300)
        
        if not success:
            logger.error(f"Batch {batch_id} failed or timed out")
            # Don't continue with zero vectors - raise error instead
            raise RuntimeError(f"Failed to generate embeddings for batch {batch_id}")
        
        # Get embeddings from cache
        embeddings = []
        valid_ids = []
        valid_texts = []
        valid_metadata = []
        failed_count = 0

        for i, text in enumerate(texts):
            text_hash = hashlib.md5(text.encode()).hexdigest()
            embedding = embedding_queue.cache.get(text_hash)

            if embedding and len(embedding) > 0:
                # Validate it's not all zeros
                non_zero_count = sum(1 for v in embedding if v != 0)
                if non_zero_count > len(embedding) * 0.1:
                    embeddings.append(embedding)
                    valid_ids.append(ids[i])
                    valid_texts.append(texts[i])
                    valid_metadata.append(metadata_list[i])
                else:
                    logger.warning(f"Skipping zero embedding for text: {text[:100]}...")
                    failed_count += 1
            else:
                logger.warning(f"No valid embedding found for text: {text[:100]}...")
                failed_count += 1
        
        if failed_count > 0:
            logger.error(f"Failed to get valid embeddings for {failed_count}/{len(texts)} texts")
        
        # Only store valid embeddings
        if not embeddings:
            logger.error(f"No valid embeddings to store for {schema}.{table}")
            return

        logger.info(f"Storing {len(embeddings)} valid embeddings for {schema}.{table}")

        # Store only valid embeddings
        await self._store_embeddings(
            schema, table, valid_ids, valid_texts, embeddings, valid_metadata
        )

    # Additional helper method to check embedding validity
    def validate_embedding(embedding: List[float]) -> bool:
        """Check if an embedding is valid (not all zeros)"""
        if not embedding or len(embedding) == 0:
            return False
        
        # Check if at least some values are non-zero
        non_zero_count = sum(1 for v in embedding if v != 0)
        
        # At least 10% should be non-zero for a valid embedding
        return non_zero_count > len(embedding) * 0.1
    
    async def _get_table_patterns(self, schema: str, table: str) -> Dict:
        """
        Get patterns for a table from stored schema analysis
        """
        try:
            query = """
                SELECT metadata, learned_patterns
                FROM atabot.managed_schemas
                WHERE schema_name = $1
            """
            result = await db_pool.fetchrow(query, schema)
            
            if result and result['metadata']:
                metadata = json.loads(result['metadata'])
                if table in metadata:
                    return metadata[table]
            
            # Return default patterns if not found
            return {
                'entity_type': 'record',
                'display_fields': [],
                'searchable_fields': [],
                'terminology': {}
            }
        except Exception as e:
            logger.warning(f"Failed to get patterns for {schema}.{table}: {e}")
            return {
                'entity_type': 'record',
                'display_fields': [],
                'searchable_fields': [],
                'terminology': {}
            }
        
    def _extract_row_id(self, row: Dict[str, Any]) -> Optional[str]:
        """
        Extract row ID from row data
        """
        # Try common primary key column names
        pk_candidates = ['id', 'uuid', 'guid', 'primary_id']
        
        for col in pk_candidates:
            if col in row and row[col] is not None:
                return str(row[col])
        
        # Look for columns ending with _id or _uuid
        for col_name, value in row.items():
            if value is not None and (col_name.endswith('_id') or col_name.endswith('_uuid')):
                return str(value)
        
        # Generate hash-based ID if no primary key found
        row_str = json.dumps(row, sort_keys=True, default=str)
        return hashlib.md5(row_str.encode()).hexdigest()
    
    async def _get_row_count(self, schema: str, table: str) -> int:
        """
        Get total row count for a table
        """
        query = f"SELECT COUNT(*) FROM {quote_ident(schema)}.{quote_ident(table)}"
        try:
            count = await db_pool.fetchval(query)
            return count or 0
        except Exception as e:
            logger.error(f"Failed to get row count for {schema}.{table}: {e}")
            return 0
    
    async def _fetch_batch(self, schema: str, table: str, offset: int, limit: int) -> List[Dict[str, Any]]:
        """
        Fetch batch of rows from table
        """
        query = f"""
            SELECT * FROM {quote_ident(schema)}.{quote_ident(table)}
            LIMIT $1 OFFSET $2
        """
        try:
            rows = await db_pool.fetch(query, limit, offset)
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Failed to fetch batch from {schema}.{table}: {e}")
            return []

    async def process_realtime_change(
        self,
        schema: str,
        table: str,
        change_data: Dict[str, Any]
    ) -> None:
        """
        Process real-time data change (adaptive untuk semua tabel)
        """
        try:
            operation = change_data.get('operation')

            if operation == 'DELETE':
                # Handle DELETE: remove embedding
                old_data = change_data.get('old_data', {})
                row_id = self._extract_row_id(old_data)
                if row_id:
                    await self._delete_embedding(schema, table, row_id)
                    logger.info(f"Deleted embedding for {schema}.{table}.{row_id}")

            elif operation in ['INSERT', 'UPDATE']:
                # Handle INSERT/UPDATE: update embedding
                new_data = change_data.get('new_data', {})
                if new_data:
                    await self._sync_single_row_data(schema, table, new_data)
                    logger.info(f"Updated embedding for {schema}.{table}")

        except Exception as e:
            logger.error(f"Failed to process real-time change for {schema}.{table}: {e}")

    async def _sync_single_row_data(
        self,
        schema: str,
        table: str,
        row_data: Dict[str, Any]
    ) -> None:
        """
        Sync single row data (digunakan untuk real-time updates)
        """
        try:
            # Get patterns
            patterns = await self._get_table_patterns(schema, table)

            # Generate searchable text
            text = self._generate_searchable_text(row_data, table, patterns)
            if not text or len(text.strip()) < 10:
                logger.warning(f"Insufficient text content for row, skipping sync")
                return

            # Prepare metadata
            metadata = self._sanitize_metadata(row_data)
            metadata['_schema'] = schema
            metadata['_table'] = table

            # Generate row ID
            row_id = self._extract_row_id(row_data)
            if not row_id:
                logger.warning(f"No row ID found, generating hash-based ID")
                row_id = hashlib.md5(f"{schema}_{table}_{text}".encode()).hexdigest()

            # Use embedding queue for single item
            from app.services.embedding_queue import embedding_queue

            batch_id = await embedding_queue.add_batch([text], [metadata])
            success = await embedding_queue.wait_for_batch(batch_id, timeout=120)

            if success:
                # Get embedding from cache
                text_hash = hashlib.md5(text.encode()).hexdigest()
                embedding = embedding_queue.cache.get(text_hash)

                if embedding and len(embedding) > 0:
                    # Validate embedding
                    non_zero_count = sum(1 for v in embedding if v != 0)
                    if non_zero_count > len(embedding) * 0.1:
                        # Store valid embedding
                        await self._store_embeddings(
                            schema, table, [row_id], [text], [embedding], [metadata]
                        )
                        logger.info(f"Real-time sync completed for row {row_id}")
                    else:
                        logger.warning(f"Invalid embedding (mostly zeros) for row {row_id}")
                else:
                    logger.error(f"No valid embedding generated for row {row_id}")
            else:
                logger.error(f"Embedding generation failed for real-time sync")

        except Exception as e:
            logger.error(f"Failed to sync single row: {e}")

    async def _delete_embedding(
        self,
        schema: str,
        table: str,
        row_id: str
    ) -> None:
        """
        Delete embedding for a specific row
        """
        try:
            embedding_id = f"{schema}_{table}_{row_id}"
            query = """
                DELETE FROM atabot.embeddings
                WHERE id = $1
            """
            await db_pool.execute(query, embedding_id)
            logger.debug(f"Deleted embedding {embedding_id}")
        except Exception as e:
            logger.error(f"Failed to delete embedding for {row_id}: {e}")

    async def _ensure_timestamp_column(
        self,
        schema: str,
        table: str
    ) -> None:
        """
        Add updated_at column and trigger to table if not exists (adaptive)
        """
        try:
            # Add updated_at column
            alter_query = f"""
                ALTER TABLE {quote_ident(schema)}.{quote_ident(table)}
                ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW()
            """
            await db_pool.execute(alter_query)

            # Create or replace update trigger function
            function_query = f"""
                CREATE OR REPLACE FUNCTION {quote_ident(schema)}.update_{table}_updated_at()
                RETURNS TRIGGER AS $$
                BEGIN
                    NEW.updated_at = NOW();
                    RETURN NEW;
                END;
                $$ language 'plpgsql'
            """
            await db_pool.execute(function_query)

            # Create trigger
            trigger_name = f"update_{table}_updated_at"
            trigger_query = f"""
                DROP TRIGGER IF EXISTS {quote_ident(trigger_name)}
                ON {quote_ident(schema)}.{quote_ident(table)};

                CREATE TRIGGER {quote_ident(trigger_name)}
                    BEFORE UPDATE ON {quote_ident(schema)}.{quote_ident(table)}
                    FOR EACH ROW EXECUTE FUNCTION {quote_ident(schema)}.update_{table}_updated_at()
            """
            await db_pool.execute(trigger_query)

            logger.info(f"Added updated_at column and trigger to {schema}.{table}")

        except Exception as e:
            logger.error(f"Failed to ensure timestamp column for {schema}.{table}: {e}")
            raise

    async def _process_batch_with_validation(
        self,
        schema: str,
        table: str,
        rows: List[Dict],
        patterns: Dict
    ) -> int:
        """
        Process batch with proper validation
        """
        if not rows:
            return 0
        
        # Generate searchable texts
        ids = []
        texts = []
        metadata_list = []
        
        for row in rows:
            # Generate ID
            row_id = self._extract_row_id(row)
            if not row_id:
                continue
            
            # Generate searchable text dengan patterns yang sudah ada
            text = self._generate_searchable_text(row, table, patterns)
            if not text or len(text.strip()) < 10:
                logger.warning(f"Skipping row {row_id} - insufficient text content")
                continue
            
            # Prepare metadata
            metadata = {
                "schema": schema,
                "table": table,
                "row_id": str(row_id),
                "columns": list(row.keys()),
                "synced_at": datetime.now(timezone.utc).isoformat()
            }
            
            ids.append(row_id)
            texts.append(text)
            metadata_list.append(metadata)
        
        if not texts:
            logger.warning(f"No valid texts to process in batch for {schema}.{table}")
            return 0
        
        # Import the embedding queue
        from app.services.embedding_queue import embedding_queue
        
        # Add to queue and get batch ID
        batch_id = await embedding_queue.add_batch(texts, metadata_list)
        logger.info(f"Added batch {batch_id} to embedding queue ({len(texts)} texts)")
        
        # Wait for processing
        logger.info(f"Waiting for batch {batch_id} to complete...")
        success = await embedding_queue.wait_for_batch(batch_id, timeout=300)
        
        if not success:
            logger.error(f"Batch {batch_id} failed or timed out")
            return 0
        
        # Get embeddings from cache
        embeddings = []
        valid_rows = []
        valid_texts = []
        valid_metadata = []
        
        for i, text in enumerate(texts):
            text_hash = hashlib.md5(text.encode()).hexdigest()
            embedding = embedding_queue.cache.get(text_hash)
            
            if embedding and len(embedding) > 0:
                embeddings.append(embedding)
                valid_rows.append(ids[i])
                valid_texts.append(texts[i])
                valid_metadata.append(metadata_list[i])
        
        # Store valid embeddings
        if embeddings:
            await self._store_embeddings(
                schema, table, valid_rows, valid_texts, embeddings, valid_metadata
            )
            return len(embeddings)
        else:
            logger.error(f"No valid embeddings to store for {schema}.{table}")
            return 0

# Global sync service instance
sync_service = SyncService()