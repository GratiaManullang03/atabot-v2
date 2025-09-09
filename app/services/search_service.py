"""
Search Service - Hybrid SQL + Vector Search
Combines traditional SQL filtering with semantic vector similarity
"""
from typing import Dict, List, Any, Optional
from loguru import logger
import json
import asyncpg

from app.core.database import db_pool
from app.core.embeddings import embedding_service
from app.core.config import settings

class SearchService:
    """
    Service for hybrid search operations combining:
    - Vector similarity search (semantic)
    - SQL filtering (exact matches, ranges, etc.)
    - Full-text search (PostgreSQL)
    """
    
    def __init__(self):
        self.search_cache = {}
        self.cache_ttl = settings.CACHE_TTL
        
    async def hybrid_search(
        self,
        query: str,
        schema: str,
        table: Optional[str] = None,
        top_k: int = 10,
        filters: Dict[str, Any] = {},
        similarity_threshold: float = 0.5
    ) -> List[Dict[str, Any]]:
        """
        Perform hybrid search combining SQL filters and vector similarity
        
        Args:
            query: Natural language search query
            schema: Schema to search in
            table: Specific table to search (optional)
            top_k: Number of results to return
            filters: SQL WHERE conditions as dict
            similarity_threshold: Minimum similarity score (0-1)
            
        Returns:
            List of search results with scores and metadata
        """
        try:
            logger.info(f"Hybrid search: query='{query}', schema='{schema}', table='{table}'")
            
            # Generate query embedding
            query_embedding = await embedding_service.generate_embedding(
                query,
                input_type="query"  # Important: use 'query' for search
            )
            
            # Build the hybrid search query
            search_query = self._build_hybrid_search_query(
                schema, table, filters, top_k * 2  # Get more for re-ranking
            )
            
            # Execute vector similarity search with filters
            results = await db_pool.fetch(
                search_query,
                query_embedding,  # Pass as parameter $1
                similarity_threshold
            )
            
            # Process and rank results
            processed_results = []
            for row in results:
                result = dict(row)
                
                # Parse metadata
                if result.get('metadata'):
                    result['metadata'] = json.loads(result['metadata'])
                
                # Calculate combined score (vector similarity + relevance boosting)
                vector_score = result['similarity']
                
                # Boost score based on query terms appearing in content
                content_boost = self._calculate_content_boost(query, result['content'])
                
                # Combined score
                result['score'] = (vector_score * 0.7) + (content_boost * 0.3)
                
                processed_results.append(result)
            
            # Sort by combined score and limit
            processed_results.sort(key=lambda x: x['score'], reverse=True)
            final_results = processed_results[:top_k]
            
            # Add source information
            for result in final_results:
                result['source'] = {
                    'schema': schema,
                    'table': result.get('table_name', table),
                    'id': result.get('id')
                }
            
            logger.info(f"Found {len(final_results)} results for query: {query}")
            return final_results
            
        except Exception as e:
            logger.error(f"Hybrid search failed: {e}")
            raise
    
    def _build_hybrid_search_query(
        self,
        schema: str,
        table: Optional[str],
        filters: Dict[str, Any],
        limit: int
    ) -> str:
        """
        Build PostgreSQL query for hybrid search
        """
        # Base query with vector similarity
        base_query = """
            SELECT 
                e.id,
                e.schema_name,
                e.table_name,
                e.content,
                e.metadata,
                e.created_at,
                1 - (e.embedding <=> $1::vector) as similarity
            FROM atabot.embeddings e
            WHERE e.schema_name = '{schema}'
        """
        
        # Add table filter if specified
        if table:
            base_query += f" AND e.table_name = '{table}'"
        
        # Add similarity threshold
        base_query += " AND 1 - (e.embedding <=> $1::vector) >= $2"
        
        # Add custom filters on metadata
        if filters:
            for key, value in filters.items():
                if isinstance(value, str):
                    base_query += f" AND e.metadata->'{key}' = '\"{value}\"'"
                elif isinstance(value, (int, float)):
                    base_query += f" AND (e.metadata->'{key}')::numeric = {value}"
                elif isinstance(value, dict):
                    # Handle range queries
                    if 'gte' in value:
                        base_query += f" AND (e.metadata->'{key}')::numeric >= {value['gte']}"
                    if 'lte' in value:
                        base_query += f" AND (e.metadata->'{key}')::numeric <= {value['lte']}"
                    if 'contains' in value:
                        base_query += f" AND e.metadata->'{key}' ILIKE '%{value['contains']}%'"
        
        # Order by similarity and limit
        base_query += f"""
            ORDER BY similarity DESC
            LIMIT {limit}
        """
        
        return base_query.format(schema=schema)
    
    def _calculate_content_boost(self, query: str, content: str) -> float:
        """
        Calculate relevance boost based on query terms in content
        """
        if not content:
            return 0.0
        
        query_lower = query.lower()
        content_lower = content.lower()
        
        # Split query into terms
        query_terms = query_lower.split()
        
        # Count matching terms
        matches = 0
        for term in query_terms:
            if len(term) > 2 and term in content_lower:  # Skip short words
                matches += 1
        
        # Calculate boost score (0-1)
        if len(query_terms) > 0:
            return matches / len(query_terms)
        
        return 0.0
    
    async def semantic_search(
        self,
        query: str,
        schemas: List[str],
        top_k: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Pure semantic search across multiple schemas
        """
        all_results = []
        
        for schema in schemas:
            results = await self.hybrid_search(
                query=query,
                schema=schema,
                top_k=top_k // len(schemas) if len(schemas) > 1 else top_k
            )
            all_results.extend(results)
        
        # Sort by score and limit
        all_results.sort(key=lambda x: x['score'], reverse=True)
        return all_results[:top_k]
    
    async def sql_search(
        self,
        schema: str,
        table: str,
        conditions: Dict[str, Any],
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Traditional SQL search with exact conditions
        """
        # Build WHERE clause
        where_parts = []
        params = []
        param_count = 1
        
        for column, value in conditions.items():
            if isinstance(value, dict):
                # Handle operators
                if 'eq' in value:
                    where_parts.append(f"{column} = ${param_count}")
                    params.append(value['eq'])
                    param_count += 1
                elif 'like' in value:
                    where_parts.append(f"{column} ILIKE ${param_count}")
                    params.append(f"%{value['like']}%")
                    param_count += 1
                elif 'in' in value:
                    placeholders = ', '.join([f"${i}" for i in range(param_count, param_count + len(value['in']))])
                    where_parts.append(f"{column} IN ({placeholders})")
                    params.extend(value['in'])
                    param_count += len(value['in'])
            else:
                # Simple equality
                where_parts.append(f"{column} = ${param_count}")
                params.append(value)
                param_count += 1
        
        where_clause = " AND ".join(where_parts) if where_parts else "1=1"
        
        # Execute query
        query = f"""
            SELECT * FROM {asyncpg.introspection.quote_ident(schema)}.{asyncpg.introspection.quote_ident(table)}
            WHERE {where_clause}
            LIMIT ${param_count}
        """
        params.append(limit)
        
        rows = await db_pool.fetch(query, *params)
        return [dict(row) for row in rows]
    
    async def find_similar(
        self,
        reference_id: str,
        schema: str,
        top_k: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Find similar items based on a reference item
        """
        # Get reference embedding
        query = """
            SELECT embedding, content, metadata
            FROM atabot.embeddings
            WHERE id = $1 AND schema_name = $2
        """
        
        reference = await db_pool.fetchrow(query, reference_id, schema)
        
        if not reference:
            raise ValueError(f"Reference item {reference_id} not found")
        
        # Find similar items
        similar_query = """
            SELECT 
                id,
                content,
                metadata,
                1 - (embedding <=> $1::vector) as similarity
            FROM atabot.embeddings
            WHERE schema_name = $2
                AND id != $3
            ORDER BY embedding <=> $1::vector
            LIMIT $4
        """
        
        results = await db_pool.fetch(
            similar_query,
            reference['embedding'],
            schema,
            reference_id,
            top_k
        )
        
        return [
            {
                **dict(row),
                'metadata': json.loads(row['metadata']) if row['metadata'] else {}
            }
            for row in results
        ]
    
    async def aggregate_search(
        self,
        query: str,
        schema: str,
        aggregation: str,
        group_by: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Perform aggregated search (COUNT, SUM, AVG, etc.)
        """
        # First, find relevant records using vector search
        relevant_items = await self.hybrid_search(
            query=query,
            schema=schema,
            top_k=1000  # Get more items for aggregation
        )
        
        if not relevant_items:
            return {
                "query": query,
                "result": 0,
                "message": "No relevant items found"
            }
        
        # Extract IDs of relevant items
        item_ids = [item['id'] for item in relevant_items]
        
        # Perform aggregation on metadata
        if aggregation.upper() == 'COUNT':
            return {
                "query": query,
                "aggregation": "COUNT",
                "result": len(item_ids),
                "group_by": group_by
            }
        
        # For other aggregations, we need to look at specific fields
        # This would need to be implemented based on your specific needs
        return {
            "query": query,
            "aggregation": aggregation,
            "message": "Complex aggregations require specific implementation"
        }
    
    async def multi_table_search(
        self,
        query: str,
        schema: str,
        tables: List[str],
        top_k_per_table: int = 5
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Search across multiple tables and return grouped results
        """
        results_by_table = {}
        
        for table in tables:
            results = await self.hybrid_search(
                query=query,
                schema=schema,
                table=table,
                top_k=top_k_per_table
            )
            
            if results:
                results_by_table[table] = results
        
        return results_by_table
    
    async def search_with_context(
        self,
        query: str,
        schema: str,
        context: Dict[str, Any],
        top_k: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Search with additional context for better relevance
        """
        # Enhance query with context
        enhanced_query = query
        
        if context.get('entity_type'):
            enhanced_query = f"{context['entity_type']}: {query}"
        
        if context.get('time_range'):
            # Add time filter
            filters = {
                'created_at': {
                    'gte': context['time_range'].get('start'),
                    'lte': context['time_range'].get('end')
                }
            }
        else:
            filters = {}
        
        # Add any additional filters from context
        if context.get('filters'):
            filters.update(context['filters'])
        
        return await self.hybrid_search(
            query=enhanced_query,
            schema=schema,
            top_k=top_k,
            filters=filters
        )
    
    async def get_search_suggestions(
        self,
        partial_query: str,
        schema: str,
        limit: int = 5
    ) -> List[str]:
        """
        Get search suggestions based on partial query
        """
        # Get frequently searched terms from embeddings
        query = """
            SELECT DISTINCT 
                substring(content from 1 for 100) as snippet
            FROM atabot.embeddings
            WHERE schema_name = $1
                AND content ILIKE $2
            LIMIT $3
        """
        
        results = await db_pool.fetch(
            query,
            schema,
            f"%{partial_query}%",
            limit
        )
        
        suggestions = []
        for row in results:
            # Extract meaningful phrases from snippets
            snippet = row['snippet']
            # Simple extraction - could be improved with NLP
            if ':' in snippet:
                suggestions.append(snippet.split(':')[1].strip()[:50])
        
        return suggestions[:limit]

# Global search service instance
search_service = SearchService()