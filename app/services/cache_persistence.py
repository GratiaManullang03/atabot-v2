"""
Cache Persistence Service - Save embeddings to database for long-term storage
"""
import asyncio
import hashlib
import json
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from loguru import logger

from app.core.config import settings
from app.core.database import db_pool

class CachePersistenceManager:
    """Manages persistent storage of embeddings cache"""
    
    def __init__(self):
        self.initialized = False
        
    async def initialize(self):
        """Create cache table if not exists"""
        try:
            await db_pool.execute("""
                CREATE TABLE IF NOT EXISTS atabot.embedding_cache (
                    text_hash VARCHAR PRIMARY KEY,
                    embedding FLOAT8[],
                    created_at TIMESTAMP DEFAULT NOW(),
                    last_accessed TIMESTAMP DEFAULT NOW(),
                    access_count INT DEFAULT 1,
                    metadata JSONB
                );
                
                CREATE INDEX IF NOT EXISTS idx_cache_created 
                ON atabot.embedding_cache(created_at);
                
                CREATE INDEX IF NOT EXISTS idx_cache_accessed 
                ON atabot.embedding_cache(last_accessed);
            """)
            self.initialized = True
            logger.info("Cache persistence table initialized")
        except Exception as e:
            logger.error(f"Failed to initialize cache table: {e}")
            
    async def save_to_persistent(self, text_hash: str, embedding: List[float], metadata: Optional[Dict] = None):
        """Save embedding to persistent storage"""
        if not self.initialized:
            await self.initialize()
            
        try:
            await db_pool.execute("""
                INSERT INTO atabot.embedding_cache (text_hash, embedding, metadata)
                VALUES ($1, $2, $3)
                ON CONFLICT (text_hash) DO UPDATE
                SET last_accessed = NOW(),
                    access_count = atabot.embedding_cache.access_count + 1
            """, text_hash, embedding, json.dumps(metadata) if metadata else None)
            
        except Exception as e:
            logger.error(f"Failed to persist embedding: {e}")
            
    async def load_from_persistent(self, text_hash: str) -> Optional[List[float]]:
        """Load embedding from persistent storage"""
        if not self.initialized:
            await self.initialize()
            
        try:
            result = await db_pool.fetchrow("""
                UPDATE atabot.embedding_cache 
                SET last_accessed = NOW(),
                    access_count = access_count + 1
                WHERE text_hash = $1
                RETURNING embedding
            """, text_hash)
            
            if result:
                return result['embedding']
                
        except Exception as e:
            logger.error(f"Failed to load from persistent cache: {e}")
            
        return None
    
    async def bulk_save(self, cache_dict: Dict[str, List[float]]):
        """Bulk save cache to persistent storage"""
        if not self.initialized:
            await self.initialize()
            
        if not cache_dict:
            return
            
        try:
            # Prepare data for bulk insert
            values = []
            for text_hash, embedding in cache_dict.items():
                if embedding and len(embedding) > 0:
                    values.append((text_hash, embedding))
            
            if values:
                await db_pool.executemany("""
                    INSERT INTO atabot.embedding_cache (text_hash, embedding)
                    VALUES ($1, $2)
                    ON CONFLICT (text_hash) DO UPDATE
                    SET last_accessed = NOW(),
                        access_count = atabot.embedding_cache.access_count + 1
                """, values)
                
                logger.info(f"Persisted {len(values)} embeddings to database")
                
        except Exception as e:
            logger.error(f"Failed to bulk save cache: {e}")
            
    async def bulk_load(self, limit: int = 1000) -> Dict[str, List[float]]:
        """Load most frequently used embeddings from persistent storage"""
        if not self.initialized:
            await self.initialize()
            
        cache_dict = {}
        
        try:
            # Load most frequently accessed embeddings
            rows = await db_pool.fetch("""
                SELECT text_hash, embedding 
                FROM atabot.embedding_cache
                WHERE last_accessed > NOW() - INTERVAL '7 days'
                ORDER BY access_count DESC, last_accessed DESC
                LIMIT $1
            """, limit)
            
            for row in rows:
                if row['embedding']:
                    cache_dict[row['text_hash']] = row['embedding']
                    
            logger.info(f"Loaded {len(cache_dict)} embeddings from persistent storage")
            
        except Exception as e:
            logger.error(f"Failed to bulk load cache: {e}")
            
        return cache_dict
    
    async def cleanup_old_cache(self, days: int = 30):
        """Remove old unused cache entries"""
        if not self.initialized:
            await self.initialize()
            
        try:
            deleted = await db_pool.execute("""
                DELETE FROM atabot.embedding_cache
                WHERE last_accessed < NOW() - INTERVAL '%s days'
                AND access_count < 5
            """, days)
            
            logger.info(f"Cleaned up {deleted} old cache entries")
            
        except Exception as e:
            logger.error(f"Failed to cleanup cache: {e}")
            
    async def get_cache_stats(self) -> Dict:
        """Get cache statistics from database"""
        if not self.initialized:
            await self.initialize()
            
        try:
            stats = await db_pool.fetchrow("""
                SELECT 
                    COUNT(*) as total_cached,
                    COUNT(CASE WHEN last_accessed > NOW() - INTERVAL '1 day' THEN 1 END) as accessed_today,
                    COUNT(CASE WHEN last_accessed > NOW() - INTERVAL '7 days' THEN 1 END) as accessed_week,
                    AVG(access_count) as avg_access_count,
                    MAX(access_count) as max_access_count,
                    SUM(access_count) as total_accesses
                FROM atabot.embedding_cache
            """)
            
            return dict(stats) if stats else {}
            
        except Exception as e:
            logger.error(f"Failed to get cache stats: {e}")
            return {}

# Global instance
cache_persistence = CachePersistenceManager()

# Background task for periodic persistence
async def periodic_cache_persistence():
    """Periodically save in-memory cache to database"""
    while True:
        try:
            # Wait for 5 minutes
            await asyncio.sleep(300)
            
            # Get current cache from embedding queue
            from app.services.embedding_queue import embedding_queue
            
            if embedding_queue.cache:
                await cache_persistence.bulk_save(embedding_queue.cache)
                logger.info("Periodic cache persistence completed")
                
        except Exception as e:
            logger.error(f"Periodic cache persistence failed: {e}")
            
        # Continue even if error
        await asyncio.sleep(60)