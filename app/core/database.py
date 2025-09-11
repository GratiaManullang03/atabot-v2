"""
Async Database Connection Pool using asyncpg
High-performance PostgreSQL operations for ATABOT 2.0
"""
import asyncpg
from asyncpg import Pool, Connection
from typing import Optional, AsyncGenerator, Dict, Any, List
from contextlib import asynccontextmanager
from loguru import logger
import json

from .config import settings

class DatabasePool:
    """Manages async PostgreSQL connection pool"""
    
    def __init__(self):
        self.pool: Optional[Pool] = None
        self.listeners: Dict[str, Any] = {}
    
    async def init_pool(self) -> None:
        """Initialize connection pool"""
        if self.pool:
            return
            
        try:
            # Create connection pool
            self.pool = await asyncpg.create_pool(
                settings.DATABASE_URL,
                min_size=10,
                max_size=settings.DATABASE_POOL_SIZE,
                max_queries=50000,
                max_inactive_connection_lifetime=300,
                command_timeout=float(settings.DATABASE_POOL_TIMEOUT),
                server_settings={
                    'application_name': settings.APP_NAME,
                    'jit': 'off'  # Disable JIT for more predictable performance
                }
            )
            
            # Register custom type handlers
            await self._register_type_handlers()
            
            logger.info(f"Database pool initialized with max {settings.DATABASE_POOL_SIZE} connections")
            
        except Exception as e:
            logger.error(f"Failed to initialize database pool: {e}")
            raise
    
    async def close_pool(self) -> None:
        """Close connection pool"""
        if self.pool:
            await self.pool.close()
            self.pool = None
            logger.info("Database pool closed")
    
    def get_pool_stats(self) -> Dict[str, Any]:
        """Get pool statistics safely"""
        if not self.pool:
            return {"status": "not_initialized"}
        
        stats = {
            "status": "active",
            "configured_min_size": 10,
            "configured_max_size": settings.DATABASE_POOL_SIZE
        }
        
        # Try to get actual stats from pool
        try:
            # asyncpg pool exposes these methods in newer versions
            if hasattr(self.pool, 'get_size'):
                stats["current_size"] = self.pool.get_size()
            if hasattr(self.pool, 'get_idle_size'):
                stats["idle_connections"] = self.pool.get_idle_size()
            if hasattr(self.pool, 'get_max_size'):
                stats["max_size"] = self.pool.get_max_size()
        except Exception as e:
            logger.debug(f"Could not get detailed pool stats: {e}")
        
        return stats
    
    @asynccontextmanager
    async def acquire(self) -> AsyncGenerator[Connection, None]:
        """Acquire a connection from the pool"""
        if not self.pool:
            await self.init_pool()
        
        async with self.pool.acquire() as connection:
            yield connection
    
    async def execute(self, query: str, *args, timeout: Optional[float] = None) -> str:
        """Execute a query without returning results"""
        async with self.acquire() as conn:
            return await conn.execute(query, *args, timeout=timeout)
    
    async def fetch(self, query: str, *args, timeout: Optional[float] = None) -> List[asyncpg.Record]:
        """Fetch multiple rows"""
        async with self.acquire() as conn:
            return await conn.fetch(query, *args, timeout=timeout)
    
    async def fetchrow(self, query: str, *args, timeout: Optional[float] = None) -> Optional[asyncpg.Record]:
        """Fetch a single row"""
        async with self.acquire() as conn:
            return await conn.fetchrow(query, *args, timeout=timeout)
    
    async def fetchval(self, query: str, *args, timeout: Optional[float] = None) -> Any:
        """Fetch a single value"""
        async with self.acquire() as conn:
            return await conn.fetchval(query, *args, timeout=timeout)
    
    async def execute_many(self, query: str, args_list: List[tuple], timeout: Optional[float] = None) -> None:
        """Execute the same query with multiple parameter sets"""
        async with self.acquire() as conn:
            await conn.executemany(query, args_list, timeout=timeout)
    
    async def _register_type_handlers(self) -> None:
        """Register custom type handlers for PostgreSQL types"""
        async with self.acquire() as conn:
            # Register JSON/JSONB codec
            await conn.set_type_codec(
                'jsonb',
                encoder=json.dumps,
                decoder=json.loads,
                schema='pg_catalog',
                format='text'
            )
            
            await conn.set_type_codec(
                'json',
                encoder=json.dumps,
                decoder=json.loads,
                schema='pg_catalog',
                format='text'
            )
    
    async def listen_to_channel(self, channel: str, callback: callable) -> None:
        """Listen to PostgreSQL NOTIFY channel"""
        if not self.pool:
            await self.init_pool()
            
        conn = await self.pool.acquire()
        self.listeners[channel] = conn
        
        await conn.add_listener(channel, callback)
        logger.info(f"Listening to PostgreSQL channel: {channel}")
    
    async def stop_listening(self, channel: str) -> None:
        """Stop listening to a channel"""
        if channel in self.listeners:
            conn = self.listeners[channel]
            await conn.remove_listener(channel)
            await self.pool.release(conn)
            del self.listeners[channel]
            logger.info(f"Stopped listening to channel: {channel}")
    
    async def get_table_info(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """Get column information for a table"""
        query = """
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default,
                character_maximum_length,
                numeric_precision,
                numeric_scale
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
            ORDER BY ordinal_position
        """
        
        rows = await self.fetch(query, schema, table)
        return [dict(row) for row in rows]
    
    async def get_schemas(self) -> List[str]:
        """Get all available schemas (excluding system schemas)"""
        query = """
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
            ORDER BY schema_name
        """
        
        rows = await self.fetch(query)
        return [row['schema_name'] for row in rows]
    
    async def get_tables(self, schema: str) -> List[Dict[str, Any]]:
        """Get all tables in a schema with row counts"""
        # Simplified query yang robust untuk semua PostgreSQL version
        query = """
            WITH table_stats AS (
                SELECT 
                    schemaname,
                    tablename,
                    n_live_tup as row_count
                FROM pg_stat_user_tables
                WHERE schemaname = $1
            ),
            table_sizes AS (
                SELECT 
                    c.relname as table_name,
                    c.reltuples::BIGINT as estimated_rows
                FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = $1 AND c.relkind = 'r'
            )
            SELECT 
                t.table_name,
                t.table_type,
                COALESCE(
                    ts.row_count,
                    tsz.estimated_rows,
                    0
                ) as estimated_row_count,
                pg_catalog.obj_description(c.oid, 'pg_class') as table_comment
            FROM information_schema.tables t
            LEFT JOIN table_stats ts 
                ON ts.tablename = t.table_name
                AND ts.schemaname = t.table_schema
            LEFT JOIN table_sizes tsz
                ON tsz.table_name = t.table_name
            LEFT JOIN pg_catalog.pg_class c 
                ON c.relname = t.table_name
                AND c.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $1)
            WHERE t.table_schema = $1
                AND t.table_type = 'BASE TABLE'
            ORDER BY t.table_name
        """
        
        try:
            rows = await self.fetch(query, schema)
            return [dict(row) for row in rows]
        except Exception as e:
            # Fallback to simpler query if complex one fails
            logger.warning(f"Complex query failed, using simple fallback: {e}")
            fallback_query = """
                SELECT 
                    table_name,
                    'BASE TABLE' as table_type,
                    0 as estimated_row_count,
                    NULL as table_comment
                FROM information_schema.tables
                WHERE table_schema = $1
                    AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """
            rows = await self.fetch(fallback_query, schema)
            
            # Try to get row counts separately
            result = []
            for row in rows:
                row_dict = dict(row)
                table_name = row_dict['table_name']
                
                # Try to get row count
                try:
                    count_query = f'SELECT COUNT(*) FROM "{schema}"."{table_name}"'
                    row_count = await self.fetchval(count_query)
                    row_dict['estimated_row_count'] = row_count
                except:
                    row_dict['estimated_row_count'] = 0
                
                result.append(row_dict)
            
            return result
    
    async def get_foreign_keys(self, schema: str) -> List[Dict[str, Any]]:
        """Get all foreign key relationships in a schema"""
        query = """
            SELECT 
                tc.table_name,
                kcu.column_name,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY' 
            AND tc.table_schema = $1
        """
        
        rows = await self.fetch(query, schema)
        return [dict(row) for row in rows]
    
    async def sample_data(self, schema: str, table: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Get sample data from a table"""
        # Use dynamic SQL safely with identifier quoting
        query = f"""
            SELECT * FROM {asyncpg.introspection.quote_ident(schema)}.{asyncpg.introspection.quote_ident(table)}
            LIMIT $1
        """
        
        rows = await self.fetch(query, limit)
        return [dict(row) for row in rows]
    
    async def check_vector_extension(self) -> bool:
        """Check if pgvector extension is installed"""
        query = """
            SELECT EXISTS (
                SELECT 1 FROM pg_extension WHERE extname = 'vector'
            )
        """
        
        return await self.fetchval(query)
    
    async def create_trigger(self, schema: str, table: str) -> None:
        """Create real-time sync trigger for a table"""
        
        def quote_ident(name: str) -> str:
            """Quote PostgreSQL identifier"""
            return f'"{name}"'
        
        trigger_name = f"atabot_sync_{table}"
        
        # Drop existing trigger if exists
        drop_query = f"""
            DROP TRIGGER IF EXISTS {quote_ident(trigger_name)} 
            ON {quote_ident(schema)}.{quote_ident(table)}
        """
        await self.execute(drop_query)
        
        # Create new trigger
        create_query = f"""
            CREATE TRIGGER {quote_ident(trigger_name)}
            AFTER INSERT OR UPDATE OR DELETE
            ON {quote_ident(schema)}.{quote_ident(table)}
            FOR EACH ROW
            EXECUTE FUNCTION atabot.notify_data_change()
        """
        await self.execute(create_query)
        
        logger.info(f"Created real-time sync trigger for {schema}.{table}")

# Global database pool instance
db_pool = DatabasePool()

async def get_db() -> DatabasePool:
    """Dependency to get database pool"""
    if not db_pool.pool:
        await db_pool.init_pool()
    return db_pool

# Context manager for database lifecycle
@asynccontextmanager
async def database_lifespan():
    """Manage database pool lifecycle"""
    await db_pool.init_pool()
    yield
    await db_pool.close_pool()