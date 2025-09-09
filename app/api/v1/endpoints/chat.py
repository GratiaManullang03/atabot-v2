# app/api/v1/endpoints/chat.py
"""
Chat API Endpoints - Main interface for querying data
Complete implementation with streaming support
"""
from fastapi import APIRouter, HTTPException, Query
from sse_starlette.sse import EventSourceResponse
from typing import Dict, Any, Optional, AsyncGenerator
from datetime import datetime
import json
import uuid
import asyncio
from loguru import logger

from app.core.mcp import mcp_orchestrator
from app.services.query_decomposer import query_decomposer
from app.services.search_service import search_service
from app.services.sql_generator import sql_generator
from app.services.answer_generator import answer_generator
from app.schemas.chat_models import ChatRequest, ChatResponse
from app.core.database import db_pool

router = APIRouter()

class ChatOrchestrator:
    """Orchestrates the complete chat flow"""
    
    async def process_query(
        self,
        request: ChatRequest
    ) -> Dict[str, Any]:
        """
        Main query processing pipeline
        """
        start_time = datetime.now()
        session_id = request.session_id or str(uuid.uuid4())
        
        try:
            # Get or create MCP context
            context = mcp_orchestrator.get_context(session_id)
            if not context:
                context = mcp_orchestrator.create_context(session_id)
            
            # Set active schema if provided
            if request.schema_name:
                context.active_schema = request.schema_name
            elif not context.active_schema:
                # Get first active schema
                schemas = await self._get_active_schemas()
                if schemas:
                    context.active_schema = schemas[0]
                else:
                    raise ValueError("No active schema found. Please sync a schema first.")
            
            # Analyze query intent
            intent = query_decomposer.analyze_query_intent(request.query)
            
            # Decompose complex queries if needed
            if intent.get('requires_decomposition', False):
                sub_queries = await query_decomposer.decompose(
                    request.query,
                    context={'schema': context.active_schema, 'intent': intent}
                )
            else:
                sub_queries = [request.query]
            
            # Process each sub-query
            all_results = []
            all_sources = []
            
            for sub_query in sub_queries:
                # Determine processing strategy
                if intent['type'] in ['aggregation', 'comparison', 'join']:
                    # Use SQL generation
                    result = await self._process_sql_query(
                        sub_query,
                        context.active_schema,
                        intent
                    )
                else:
                    # Use hybrid search
                    result = await self._process_hybrid_search(
                        sub_query,
                        context.active_schema,
                        request.top_k
                    )
                
                all_results.append(result)
                all_sources.extend(result.get('sources', []))
            
            # Generate final answer
            if len(sub_queries) > 1:
                # Recompose multiple answers
                final_answer = await query_decomposer.recompose_answers(
                    sub_queries,
                    [r['answer'] for r in all_results],
                    request.query
                )
            else:
                final_answer = all_results[0]['answer']
            
            # Track in context
            context.add_message("user", request.query)
            context.add_message("assistant", final_answer, {
                "sources": len(all_sources),
                "processing_time": (datetime.now() - start_time).total_seconds()
            })
            
            # Log query
            await self._log_query(
                session_id,
                request.query,
                (datetime.now() - start_time).total_seconds() * 1000
            )
            
            return {
                "success": True,
                "session_id": session_id,
                "query": request.query,
                "answer": final_answer,
                "sources": all_sources[:10] if request.include_sources else [],
                "processing_time": (datetime.now() - start_time).total_seconds(),
                "metadata": {
                    "schema": context.active_schema,
                    "intent": intent['type'],
                    "sub_queries": len(sub_queries)
                }
            }
            
        except Exception as e:
            logger.error(f"Chat processing error: {e}")
            raise
    
    async def _process_sql_query(
        self,
        query: str,
        schema: str,
        intent: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Process query using SQL generation"""
        
        # Generate SQL
        sql_result = await sql_generator.generate_sql(
            query,
            schema,
            context={'intent': intent}
        )
        
        # Execute SQL
        exec_result = await sql_generator.execute_sql(
            sql_result['sql'],
            schema
        )
        
        # Generate answer from results
        answer = await answer_generator.generate_answer(
            query,
            exec_result,
            context={'intent': intent, 'sql': sql_result['sql']}
        )
        
        return {
            "answer": answer,
            "sources": exec_result.get('rows', [])[:5],
            "sql": sql_result['sql']
        }
    
    async def _process_hybrid_search(
        self,
        query: str,
        schema: str,
        top_k: int
    ) -> Dict[str, Any]:
        """Process query using hybrid search"""
        
        # Perform hybrid search
        results = await search_service.hybrid_search(
            query,
            schema,
            top_k=top_k
        )
        
        # Generate answer from results
        answer = await answer_generator.generate_answer(
            query,
            results
        )
        
        return {
            "answer": answer,
            "sources": results
        }
    
    async def _get_active_schemas(self) -> list:
        """Get list of active schemas"""
        query = """
            SELECT schema_name 
            FROM atabot.managed_schemas 
            WHERE is_active = true
        """
        rows = await db_pool.fetch(query)
        return [row['schema_name'] for row in rows]
    
    async def _log_query(
        self,
        session_id: str,
        query: str,
        response_time_ms: float
    ) -> None:
        """Log query for analytics"""
        try:
            await db_pool.execute(
                """
                INSERT INTO atabot.query_logs 
                (session_id, query, response_time_ms, created_at)
                VALUES ($1, $2, $3, NOW())
                """,
                session_id,
                query,
                response_time_ms
            )
        except Exception as e:
            logger.warning(f"Failed to log query: {e}")
    
    async def stream_response(
        self,
        request: ChatRequest
    ) -> AsyncGenerator[str, None]:
        """
        Generate streaming response
        """
        session_id = request.session_id or str(uuid.uuid4())
        
        # Send initial message
        yield json.dumps({
            "type": "start",
            "session_id": session_id,
            "timestamp": datetime.now().isoformat()
        })
        yield "\n"
        
        try:
            # Process query
            result = await self.process_query(request)
            
            # Stream answer in chunks
            answer = result['answer']
            chunk_size = 50  # characters per chunk
            
            for i in range(0, len(answer), chunk_size):
                chunk = answer[i:i+chunk_size]
                yield json.dumps({
                    "type": "content",
                    "content": chunk
                })
                yield "\n"
                await asyncio.sleep(0.05)  # Small delay for streaming effect
            
            # Send sources if requested
            if request.include_sources and result.get('sources'):
                yield json.dumps({
                    "type": "sources",
                    "sources": result['sources']
                })
                yield "\n"
            
            # Send completion
            yield json.dumps({
                "type": "complete",
                "processing_time": result['processing_time']
            })
            yield "\n"
            
        except Exception as e:
            yield json.dumps({
                "type": "error",
                "error": str(e)
            })
            yield "\n"

# Initialize orchestrator
chat_orchestrator = ChatOrchestrator()

@router.post("/", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """
    Main chat endpoint for natural language queries
    """
    try:
        result = await chat_orchestrator.process_query(request)
        return ChatResponse(**result)
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Chat error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.post("/stream")
async def chat_stream(request: ChatRequest):
    """
    Streaming chat endpoint for real-time responses
    """
    try:
        return EventSourceResponse(
            chat_orchestrator.stream_response(request)
        )
    except Exception as e:
        logger.error(f"Stream error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/history/{session_id}")
async def get_chat_history(
    session_id: str,
    limit: int = Query(default=10, le=100)
):
    """
    Get chat history for a session
    """
    context = mcp_orchestrator.get_context(session_id)
    
    if not context:
        raise HTTPException(status_code=404, detail="Session not found")
    
    history = context.get_recent_context(limit)
    
    return {
        "session_id": session_id,
        "history": history,
        "active_schema": context.active_schema,
        "total_messages": len(context.conversation_history)
    }

@router.delete("/history/{session_id}")
async def clear_chat_history(session_id: str):
    """
    Clear chat history for a session
    """
    if session_id in mcp_orchestrator.contexts:
        del mcp_orchestrator.contexts[session_id]
        return {"success": True, "message": "Session cleared"}
    
    raise HTTPException(status_code=404, detail="Session not found")

@router.post("/feedback")
async def submit_feedback(
    session_id: str,
    rating: int = Query(..., ge=1, le=5),
    comment: Optional[str] = None
):
    """
    Submit feedback for a chat session
    """
    # TODO: Store feedback in database
    logger.info(f"Feedback for session {session_id}: rating={rating}, comment={comment}")
    
    return {
        "success": True,
        "message": "Feedback received"
    }