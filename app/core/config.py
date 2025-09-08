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
    APP_NAME: str = "ATABOT 2.0"
    APP_VERSION: str = "2.0.0"
    APP_DESCRIPTION: str = "Universal Adaptive Business Intelligence"
    DEBUG: bool = True
    PORT: int = 8000
    LOG_LEVEL: str = "INFO"
    
    # Database - PostgreSQL with pgvector
    DATABASE_URL: str = "postgresql://user:password@localhost:5432/atabot"
    DATABASE_POOL_SIZE: int = 20
    DATABASE_MAX_OVERFLOW: int = 40
    DATABASE_POOL_TIMEOUT: int = 30
    
    # AI Services - API only, no local models!
    VOYAGE_API_KEY: str
    VOYAGE_MODEL: str = "voyage-3.5-lite"
    VOYAGE_INPUT_TYPE: str = "document"  # 'document' or 'query'
    EMBEDDING_DIMENSIONS: int = 1024
    EMBEDDING_BATCH_SIZE: int = 100
    
    # LLM Configuration
    POE_API_KEY: str
    LLM_MODEL: str = "GPT-3.5-Turbo"
    LLM_MAX_TOKENS: int = 2000
    LLM_TEMPERATURE: float = 0.1
    LLM_TIMEOUT: int = 30
    
    # Performance Settings
    SYNC_BATCH_SIZE: int = 1000
    SYNC_MAX_WORKERS: int = 4
    VECTOR_SEARCH_LIMIT: int = 10
    MAX_CONCURRENT_REQUESTS: int = 50
    QUERY_TIMEOUT: int = 30
    
    # Feature Flags
    ENABLE_STREAMING: bool = True
    ENABLE_REALTIME_SYNC: bool = True
    ENABLE_CACHE: bool = True
    ENABLE_QUERY_DECOMPOSITION: bool = True
    ENABLE_HYBRID_SEARCH: bool = True
    
    # Cache Settings
    CACHE_TTL: int = 3600  # 1 hour
    CACHE_MAX_SIZE: int = 1000
    REDIS_URL: Optional[str] = None  # Optional Redis for distributed cache
    
    # Security
    SECRET_KEY: str = "change-this-in-production"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    
    # CORS
    CORS_ORIGINS: List[str] = ["*"]
    CORS_ALLOW_CREDENTIALS: bool = True
    CORS_ALLOW_METHODS: List[str] = ["*"]
    CORS_ALLOW_HEADERS: List[str] = ["*"]
    
    # Rate Limiting
    RATE_LIMIT_ENABLED: bool = True
    RATE_LIMIT_PER_MINUTE: int = 60
    
    # Monitoring
    ENABLE_METRICS: bool = True
    METRICS_PORT: int = 9090
    
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