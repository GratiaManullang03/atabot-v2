"""
Application Initialization Module
Handles database setup and initial configuration
"""
from loguru import logger
import aiofiles
import os

from app.core.database import db_pool
from app.core.config import settings


class AppInitializer:
    """Handles application initialization tasks"""
    
    @staticmethod
    async def initialize_database() -> None:
        """
        Initialize database schema and tables
        """
        try:
            # Check if atabot schema exists
            query = """
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.schemata 
                    WHERE schema_name = 'atabot'
                )
            """
            schema_exists = await db_pool.fetchval(query)
            
            if not schema_exists:
                logger.info("Creating ATABOT schema and tables...")
                
                # Check if init.sql exists
                init_sql_path = os.path.join(os.path.dirname(__file__), '..', 'init.sql')
                
                if os.path.exists(init_sql_path):
                    async with aiofiles.open(init_sql_path, 'r') as f:
                        init_sql = await f.read()
                    
                    # Execute initialization SQL
                    await db_pool.execute(init_sql)
                    logger.info("Database initialization complete")
                else:
                    # Use embedded SQL
                    await AppInitializer._execute_embedded_sql()
            else:
                logger.info("ATABOT schema already exists")
                
        except Exception as e:
            logger.error(f"Database initialization failed: {e}")
            raise
    
    @staticmethod
    async def _execute_embedded_sql() -> None:
        """Execute embedded initialization SQL"""
        # Basic schema creation
        await db_pool.execute("CREATE EXTENSION IF NOT EXISTS vector")
        await db_pool.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"")
        await db_pool.execute("CREATE SCHEMA IF NOT EXISTS atabot")
        
        # Create tables (simplified version)
        await db_pool.execute("""
            CREATE TABLE IF NOT EXISTS atabot.managed_schemas (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                schema_name TEXT UNIQUE NOT NULL,
                display_name TEXT NOT NULL,
                is_active BOOLEAN DEFAULT false,
                metadata JSONB DEFAULT '{}',
                learned_patterns JSONB DEFAULT '{}',
                total_tables INTEGER DEFAULT 0,
                total_rows BIGINT DEFAULT 0,
                business_domain TEXT,
                last_synced_at TIMESTAMP WITH TIME ZONE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        """)
        
        await db_pool.execute("""
            CREATE TABLE IF NOT EXISTS atabot.embeddings (
                id TEXT PRIMARY KEY,
                schema_name TEXT NOT NULL,
                table_name TEXT NOT NULL,
                content TEXT NOT NULL,
                embedding vector(1024) NOT NULL,
                metadata JSONB DEFAULT '{}',
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        """)
        
        await db_pool.execute("""
            CREATE TABLE IF NOT EXISTS atabot.sync_status (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                schema_name TEXT NOT NULL,
                table_name TEXT NOT NULL,
                sync_status TEXT DEFAULT 'pending',
                last_sync_completed TIMESTAMP WITH TIME ZONE,
                rows_synced BIGINT DEFAULT 0,
                realtime_enabled BOOLEAN DEFAULT false,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                UNIQUE(schema_name, table_name)
            )
        """)
        
        await db_pool.execute("""
            CREATE TABLE IF NOT EXISTS atabot.query_logs (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                session_id TEXT,
                query TEXT NOT NULL,
                response_time_ms FLOAT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        """)
        
        # Create indexes
        await db_pool.execute("""
            CREATE INDEX IF NOT EXISTS idx_embeddings_schema_table 
            ON atabot.embeddings(schema_name, table_name)
        """)
        
        await db_pool.execute("""
            CREATE INDEX IF NOT EXISTS idx_embeddings_vector 
            ON atabot.embeddings USING ivfflat (embedding vector_cosine_ops) 
            WITH (lists = 100)
        """)
        
        logger.info("Embedded SQL initialization complete")
    
    @staticmethod
    async def start_realtime_listener() -> None:
        """Start real-time sync listener"""
        if not settings.ENABLE_REALTIME_SYNC:
            return
        
        try:
            async def handle_data_change(connection, pid, channel, payload):
                """Handle data change notifications"""
                logger.debug(f"Data change notification: {payload}")
                # TODO: Process the change
                from app.services.sync_service import sync_service
                # Process in background
            
            await db_pool.listen_to_channel('atabot_data_change', handle_data_change)
            logger.info("Real-time sync listener started")
            
        except Exception as e:
            logger.error(f"Failed to start real-time sync listener: {e}")
    
    @staticmethod
    async def check_dependencies() -> dict:
        """Check all system dependencies"""
        status = {
            "database": False,
            "pgvector": False,
            "voyageai": False
        }
        
        try:
            # Check database
            await db_pool.fetchval("SELECT 1")
            status["database"] = True
            
            # Check pgvector
            has_vector = await db_pool.check_vector_extension()
            status["pgvector"] = has_vector
            
            # Check VoyageAI (test with small embedding)
            from app.core.embeddings import embedding_service
            test_embedding = await embedding_service.generate_embedding("test", "query")
            status["voyageai"] = len(test_embedding) == settings.EMBEDDING_DIMENSIONS
            
        except Exception as e:
            logger.error(f"Dependency check failed: {e}")
        
        return status

# Global initializer instance
app_initializer = AppInitializer()