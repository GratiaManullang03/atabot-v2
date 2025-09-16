"""
Chat API Endpoints - Main interface for querying data
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
from app.services.security_guard import security_guard
from app.services.join_analyzer import join_analyzer
from app.schemas.chat_models import ChatRequest, ChatResponse
from app.core.database import db_pool
from app.services.smart_router import smart_router

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
            # SECURITY VALIDATION FIRST
            is_valid, violation_reason = security_guard.validate_query(request.query)
            if not is_valid:
                logger.warning(f"Query rejected by security guard: {violation_reason}")
                return {
                    "success": False,
                    "session_id": session_id,
                    "query": request.query,
                    "answer": security_guard.generate_safe_response_template(request.query),
                    "sources": [],
                    "processing_time": (datetime.now() - start_time).total_seconds(),
                    "metadata": {
                        "security_violation": violation_reason,
                        "rejected": True
                    }
                }

            # Sanitize query
            sanitized_query = security_guard.sanitize_query(request.query)
            if sanitized_query != request.query:
                logger.info(f"Query sanitized: '{request.query}' -> '{sanitized_query}'")
                request.query = sanitized_query

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
                # Check for multi-table join opportunities first
                join_opportunity = await join_analyzer.detect_join_opportunity(
                    sub_query,
                    context.active_schema
                )

                if join_opportunity:
                    # Use join-based processing
                    result = await self._process_join_query(
                        sub_query,
                        context.active_schema,
                        join_opportunity
                    )
                elif intent['type'] in ['aggregation', 'comparison', 'join']:
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

            # VALIDATE RESPONSE CONTENT
            if not security_guard.validate_response_content(final_answer):
                logger.error("Response failed security validation, using safe fallback")
                final_answer = "Maaf, tidak dapat memproses permintaan Anda saat ini. Silakan coba dengan pertanyaan yang lebih spesifik tentang data inventory atau bisnis."

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

    async def _process_join_query(
        self,
        query: str,
        schema: str,
        join_opportunity: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Process query that requires joining multiple tables"""

        try:
            logger.info(f"Processing join query: {query}")
            logger.info(f"Join strategy: {join_opportunity}")

            # Execute join query
            results = await join_analyzer.execute_join_query(
                join_opportunity,
                schema,
                where_conditions=[],
                limit=50
            )

            if not results:
                return {
                    'answer': f"Tidak ditemukan data yang sesuai dengan query join: {query}",
                    'sources': []
                }

            # Convert to sources format
            sources = []
            for i, result in enumerate(results[:10]):  # Limit to top 10
                source = {
                    'id': f"join_result_{i}",
                    'schema_name': schema,
                    'table_name': 'multiple_tables',
                    'content': f"Join result from tables: {', '.join(join_opportunity['tables'])}",
                    'metadata': result,
                    'score': 0.9,
                    'source': {
                        'schema': schema,
                        'table': 'join',
                        'id': f"join_{i}"
                    }
                }
                sources.append(source)

            # Generate answer using the join results
            answer = await answer_generator.generate_answer(
                query,
                sources,
                context={'intent': 'join'},
                language="auto"
            )

            return {
                'answer': answer,
                'sources': sources
            }

        except Exception as e:
            logger.error(f"Join query processing failed: {e}")
            return {
                'answer': f"Gagal memproses query join: {str(e)}",
                'sources': []
            }
    
    async def _process_sql_query(
        self,
        query: str,
        schema: str,
        intent: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Process query using SQL generation"""
        
        # TRY SMART ROUTER FIRST!
        sql = smart_router.generate_sql_without_llm(
            query, 
            schema,
            intent.get('target_table')
        )
        
        if sql:
            # No LLM needed!
            logger.info(f"Used smart router for SQL: {sql}")
            exec_result = await sql_generator.execute_sql(sql, schema)
        else:
            # Fallback to LLM
            sql_result = await sql_generator.generate_sql(
                query,
                schema,
                context={'intent': intent}
            )
            sql = sql_result['sql']
            exec_result = await sql_generator.execute_sql(sql, schema)
        
        # Generate answer
        answer = await answer_generator.generate_answer(
            query,
            exec_result,
            context={'intent': intent, 'sql': sql}
        )
        
        return {
            "answer": answer,
            "sources": exec_result.get('rows', [])[:5],
            "sql": sql
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