"""
Pydantic models for chat-related API requests and responses
"""
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime


class ChatRequest(BaseModel):
    """Request for chat endpoint"""
    query: str = Field(..., description="User's natural language query")
    schema_name: Optional[str] = Field(None, description="Target schema (uses active if not specified)")
    session_id: Optional[str] = Field(None, description="Session ID for context continuity")
    stream: bool = Field(default=False, description="Enable streaming response")
    top_k: int = Field(default=10, description="Number of relevant documents to retrieve")
    include_sources: bool = Field(default=True, description="Include source documents in response")


class ChatResponse(BaseModel):
    """Response from chat endpoint"""
    success: bool
    session_id: str
    query: str
    answer: str
    sources: List[Dict[str, Any]]
    processing_time: float
    metadata: Dict[str, Any] = {}


class StreamChunk(BaseModel):
    """Single chunk in streaming response"""
    type: str  # 'start', 'content', 'source', 'end', 'error'
    content: Optional[str] = None
    metadata: Dict[str, Any] = {}