"""
ATABOT 2.0 Core Configuration
Lightweight, adaptive business intelligence system
"""
from typing import Optional, List
from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    """Application settings with environment variable support"""
    
    # Application
    APP_NAME: str
    APP_VERSION: str
    APP_DESCRIPTION: str
    DEBUG: bool
    PORT: int
    LOG_LEVEL: str
    
    # Database - PostgreSQL with pgvector
    DATABASE_URL: str
    DATABASE_POOL_SIZE: int
    DATABASE_MAX_OVERFLOW: int
    DATABASE_POOL_TIMEOUT: int
    
    # AI Services - API only, no local models!
    VOYAGE_API_KEY: str
    VOYAGE_MODEL: str
    VOYAGE_INPUT_TYPE: str
    EMBEDDING_DIMENSIONS: int
    EMBEDDING_BATCH_SIZE: int
    
    # LLM Configuration
    POE_API_KEY: str
    LLM_MODEL: str
    LLM_MAX_TOKENS: int
    LLM_TEMPERATURE: float
    LLM_TIMEOUT: int
    
    # Performance Settings
    SYNC_BATCH_SIZE: int
    SYNC_MAX_WORKERS: int
    VECTOR_SEARCH_LIMIT: int
    MAX_CONCURRENT_REQUESTS: int
    QUERY_TIMEOUT: int

    # Rate Limiting Settings
    VOYAGE_RATE_LIMIT_RPM: int
    VOYAGE_RATE_LIMIT_DELAY: int
    
    # Feature Flags
    ENABLE_STREAMING: bool
    ENABLE_REALTIME_SYNC: bool
    ENABLE_CACHE: bool
    ENABLE_QUERY_DECOMPOSITION: bool
    ENABLE_HYBRID_SEARCH: bool
    
    # Cache Settings
    CACHE_TTL: int
    CACHE_MAX_SIZE: int
    REDIS_URL: Optional[str]
    
    # Security
    SECRET_KEY: str
    ALGORITHM: str
    ACCESS_TOKEN_EXPIRE_MINUTES: int
    
    # CORS
    CORS_ORIGINS: List[str]
    CORS_ALLOW_CREDENTIALS: bool
    CORS_ALLOW_METHODS: List[str]
    CORS_ALLOW_HEADERS: List[str]
    
    # Rate Limiting
    RATE_LIMIT_ENABLED: bool
    RATE_LIMIT_PER_MINUTE: int
    
    # Monitoring
    ENABLE_METRICS: bool
    METRICS_PORT: int
    
    class Config:
        env_file = ".env"
        case_sensitive = True
        
    def get_async_database_url(self) -> str:
        """Convert DATABASE_URL to asyncpg format"""
        url = self.DATABASE_URL
        if url.startswith("postgresql://"):
            url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
        elif url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql+asyncpg://", 1)
        return url
    
    def get_sync_database_url(self) -> str:
        """Get synchronous database URL"""
        url = self.DATABASE_URL
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql://", 1)
        return url


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance"""
    return Settings()


# Global settings instance
settings = get_settings()