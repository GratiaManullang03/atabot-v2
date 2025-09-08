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
from datetime import datetime

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
            
            logger.info(f"Database pool initialized with {settings.DATABASE_POOL_SIZE} connections")
            
        except Exception as e:
            logger.error(f"Failed to initialize database pool: {e}")
            raise
    
    async def close_pool(self) -> None:
        """Close connection pool"""
        if self.pool:
            await self.pool.close()
            self.pool = None
            logger.info("Database pool closed")
    
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
        query = """
            SELECT 
                t.table_name,
                t.table_type,
                obj_description(c.oid) as table_comment,
                COALESCE(
                    (SELECT n_live_tup FROM pg_stat_user_tables 
                     WHERE schemaname = $1 AND tablename = t.table_name),
                    0
                ) as estimated_row_count
            FROM information_schema.tables t
            LEFT JOIN pg_class c ON c.relname = t.table_name
            LEFT JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = t.table_schema
            WHERE t.table_schema = $1
            AND t.table_type = 'BASE TABLE'
            ORDER BY t.table_name
        """
        
        rows = await self.fetch(query, schema)
        return [dict(row) for row in rows]
    
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
        trigger_name = f"atabot_sync_{table}"
        
        # Drop existing trigger if exists
        drop_query = f"""
            DROP TRIGGER IF EXISTS {trigger_name} 
            ON {asyncpg.introspection.quote_ident(schema)}.{asyncpg.introspection.quote_ident(table)}
        """
        await self.execute(drop_query)
        
        # Create new trigger
        create_query = f"""
            CREATE TRIGGER {trigger_name}
            AFTER INSERT OR UPDATE OR DELETE
            ON {asyncpg.introspection.quote_ident(schema)}.{asyncpg.introspection.quote_ident(table)}
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