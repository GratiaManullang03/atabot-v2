"""
API Router Configuration
"""
from fastapi import APIRouter

# Import all endpoint routers
from app.api.v1.endpoints import health, schemas, sync, chat

# Create main API router
api_router = APIRouter()

# Include all endpoint routers with proper prefixes
api_router.include_router(health.router, prefix="", tags=["Health"])
api_router.include_router(schemas.router, prefix="/schemas", tags=["Schemas"]) 
api_router.include_router(sync.router, prefix="/sync", tags=["Synchronization"])
api_router.include_router(chat.router, prefix="/chat", tags=["Chat"])