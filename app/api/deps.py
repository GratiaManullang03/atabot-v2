from typing import Optional
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from app.core.security import verify_token

# Optional: Redis dependency
try:
    import redis
    from app.core.config import settings
    
    def get_redis() -> Optional[redis.Redis]:
        """Get Redis connection (optional)"""
        if settings.REDIS_URL:
            return redis.from_url(settings.REDIS_URL, decode_responses=True)
        return None
except ImportError:
    def get_redis() -> None:
        return None

# JWT Bearer token (skeleton - sesuaikan dengan kebutuhan client)
security = HTTPBearer(auto_error=False)


def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
) -> Optional[dict]:
    """
    Get current user from JWT token
    TODO: Implementasi akan disesuaikan dengan sistem autentikasi client
    Bisa menggunakan:
    - JWT local
    - SSO endpoint validation
    - OAuth2
    - API Key
    """
    if not credentials:
        return None
    
    token = credentials.credentials
    payload = verify_token(token)
    
    if not payload:
        return None
    
    # Return user info dari token
    # Sesuaikan struktur dengan kebutuhan
    return {
        "user_id": payload.get("sub"),
        "username": payload.get("username"),
        # tambahkan field lain sesuai kebutuhan
    }


def require_auth(
    current_user: Optional[dict] = Depends(get_current_user)
) -> dict:
    """Require authenticated user"""
    if not current_user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return current_user