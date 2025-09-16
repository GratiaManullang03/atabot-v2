"""
Search Service - Hybrid SQL + Vector Search
Combines traditional SQL filtering with semantic vector similarity
"""
from typing import Dict, List, Any, Optional
from loguru import logger
import json

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

            # Check if this is an aggregation/superlative query
            aggregation_result = await self._handle_aggregation_query(query, schema, table, top_k)
            if aggregation_result:
                logger.info(f"Aggregation query handled, returning {len(aggregation_result)} results")
                return aggregation_result

            # Enhanced strategy: detect potential product names in query
            query_terms = [term for term in query.split() if len(term) > 1]
            product_keywords = ['stok', 'stock', 'produk', 'product', 'item', 'barang']

            # Check for short product terms (like "ALO") that might need fallback search
            has_short_product_terms = any(len(term) >= 2 and len(term) <= 6 for term in query_terms)
            has_product_context = any(keyword in query.lower() for keyword in product_keywords)

            # Use fallback for queries with short product terms OR traditional short queries
            is_short_query = len(query_terms) <= 2 and any(len(term) <= 5 for term in query_terms)
            needs_fallback = is_short_query or (has_short_product_terms and has_product_context)

            if needs_fallback:
                logger.info(f"Fallback search triggered: query='{query}', short_query={is_short_query}, has_short_terms={has_short_product_terms}, has_context={has_product_context}")
                # Try PostgreSQL text search first for queries needing fallback
                fallback_results = await self._fallback_text_search(query, schema, table, top_k)
                if fallback_results:
                    logger.info(f"Fallback text search returned {len(fallback_results)} results")
                    return fallback_results
                else:
                    logger.info("Fallback search returned no results, continuing with vector search")

            # Generate query embedding
            query_embedding = await embedding_service.generate_embedding(
                query,
                input_type="query"  # Important: use 'query' for search
            )

            if not query_embedding:
                logger.error("Failed to generate embedding for query")
                return []

            # Format embedding for PostgreSQL vector type
            embedding_str = '[' + ','.join(map(str, query_embedding)) + ']'

            # Build the hybrid search query
            search_query = self._build_hybrid_search_query(
                schema, table, filters, top_k * 2  # Get more for re-ranking
            )

            # Execute vector similarity search with filters
            logger.info(f"Executing search query with embedding_str length: {len(embedding_str) if embedding_str else 0}")
            logger.info(f"Search query: {search_query[:200]}...")

            results = await db_pool.fetch(
                search_query,
                embedding_str,  # Pass formatted embedding string
                similarity_threshold
            )

            logger.info(f"Raw search results count: {len(results)}")

            # Process and rank results
            processed_results = []
            for i, row in enumerate(results):
                try:
                    result = dict(row)
                    logger.info(f"Processing result {i+1}: id={result.get('id')}, metadata type: {type(result.get('metadata'))}")

                    # Parse metadata - handle both string and dict formats
                    if result.get('metadata'):
                        metadata = result['metadata']
                        if isinstance(metadata, str):
                            logger.debug(f"Parsing metadata string: {metadata[:100]}...")
                            result['metadata'] = json.loads(metadata)
                        elif isinstance(metadata, dict):
                            logger.debug("Metadata is already a dict, keeping as-is")
                            # Already parsed (JSONB column)
                            pass
                        else:
                            logger.warning(f"Unexpected metadata type: {type(metadata)}")
                            result['metadata'] = {}
                    else:
                        logger.debug("No metadata found in result")
                        result['metadata'] = {}

                    # Calculate combined score (vector similarity + relevance boosting)
                    vector_score = result.get('similarity', 0.0)
                    logger.debug(f"Vector score for result {i+1}: {vector_score}")

                    # Boost score based on query terms appearing in content
                    content = result.get('content', '')
                    content_boost = self._calculate_content_boost(query, content)
                    logger.debug(f"Content boost for result {i+1}: {content_boost}")

                    # Combined score
                    result['score'] = (vector_score * 0.7) + (content_boost * 0.3)
                    logger.info(f"Final score for result {i+1}: {result['score']} (vector: {vector_score}, boost: {content_boost})")

                    processed_results.append(result)

                except Exception as e:
                    logger.error(f"Error processing search result {i+1}: {e}")
                    logger.error(f"Result data: {dict(row) if row else 'None'}")
                    continue
            
            # Sort by combined score and limit
            logger.info(f"Sorting {len(processed_results)} processed results by score")
            processed_results.sort(key=lambda x: x['score'], reverse=True)
            final_results = processed_results[:top_k]

            logger.info(f"Top {len(final_results)} results selected")

            # Add source information
            for i, result in enumerate(final_results):
                logger.debug(f"Adding source info to result {i+1}: table={result.get('table_name', table)}")
                result['source'] = {
                    'schema': schema,
                    'table': result.get('table_name', table),
                    'id': result.get('id')
                }

            logger.info(f"Search completed successfully: Found {len(final_results)} results for query: '{query}'")

            # Log top results for debugging
            for i, result in enumerate(final_results[:3]):  # Show top 3
                logger.info(f"Top vector result {i+1}: score={result['score']:.4f}, similarity={result.get('similarity', 0):.4f}, content='{result.get('content', '')[:100]}...'")

            # Log if specific terms are found in any results
            alo_found = any('alo' in result.get('content', '').lower() for result in final_results)
            logger.info(f"ALO term found in any results: {alo_found}")

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
        Enhanced relevance boost with partial matching support
        """
        if not content:
            return 0.0

        query_lower = query.lower()
        content_lower = content.lower()

        # Split query into terms
        query_terms = [term for term in query_lower.split() if len(term) > 2]

        # Enhanced matching strategies
        exact_matches = 0
        partial_matches = 0
        sequence_bonus = 0

        for term in query_terms:
            # 1. Exact term match (highest weight)
            if term in content_lower:
                exact_matches += 1
                logger.debug(f"Exact match found for term '{term}' in content")

                # 2. Check for sequence bonus (terms appearing together)
                if len(query_terms) > 1:
                    query_sequence = ' '.join(query_terms)
                    if query_sequence in content_lower:
                        sequence_bonus = 0.3
                        logger.debug(f"Sequence bonus applied for '{query_sequence}'")

            # 3. Partial word match (for partial searches like "ALO" in "ALO LEGGING")
            else:
                # Split content into words and check partial matches
                content_words = content_lower.split()
                for word in content_words:
                    if term in word and len(term) >= 2:  # Lowered from 3 to 2 for better "ALO" matching
                        partial_matches += 0.7  # Lower weight than exact match
                        logger.debug(f"Partial match found: term '{term}' in word '{word}'")
                        break

        # Calculate base score
        total_terms = len(query_terms)
        if total_terms == 0:
            return 0.0

        # Enhanced scoring formula
        exact_score = exact_matches / total_terms
        partial_score = partial_matches / total_terms

        # Combine scores with weights
        final_score = (exact_score * 1.0) + (partial_score * 0.6) + sequence_bonus

        logger.debug(f"Content boost calculation: exact={exact_matches}/{total_terms}={exact_score:.2f}, partial={partial_matches}/{total_terms}={partial_score:.2f}, sequence={sequence_bonus:.2f}, final={final_score:.2f}")

        # Cap at 1.0
        return min(final_score, 1.0)
    
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
        # Quote identifier function
        def quote_ident(name: str) -> str:
            return f'"{name}"'
        
        # Build WHERE clause
        where_parts = []
        params = []
        param_count = 1
        
        for column, value in conditions.items():
            if isinstance(value, dict):
                # Handle operators
                if 'eq' in value:
                    where_parts.append(f"{quote_ident(column)} = ${param_count}")
                    params.append(value['eq'])
                    param_count += 1
                elif 'like' in value:
                    where_parts.append(f"{quote_ident(column)} ILIKE ${param_count}")
                    params.append(f"%{value['like']}%")
                    param_count += 1
                elif 'in' in value:
                    placeholders = ', '.join([f"${i}" for i in range(param_count, param_count + len(value['in']))])
                    where_parts.append(f"{quote_ident(column)} IN ({placeholders})")
                    params.extend(value['in'])
                    param_count += len(value['in'])
            else:
                # Simple equality
                where_parts.append(f"{quote_ident(column)} = ${param_count}")
                params.append(value)
                param_count += 1
        
        where_clause = " AND ".join(where_parts) if where_parts else "1=1"
        
        # Execute query
        query = f"""
            SELECT * FROM {quote_ident(schema)}.{quote_ident(table)}
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
        
        # Format reference embedding for PostgreSQL
        ref_embedding_str = '[' + ','.join(map(str, reference['embedding'])) + ']'

        results = await db_pool.fetch(
            similar_query,
            ref_embedding_str,
            schema,
            reference_id,
            top_k
        )
        
        processed_similar = []
        for row in results:
            result = dict(row)

            # Handle metadata parsing safely
            if result.get('metadata'):
                metadata = result['metadata']
                if isinstance(metadata, str):
                    result['metadata'] = json.loads(metadata)
                elif isinstance(metadata, dict):
                    # Already parsed (JSONB column)
                    pass
                else:
                    result['metadata'] = {}
            else:
                result['metadata'] = {}

            processed_similar.append(result)

        logger.info(f"Found {len(processed_similar)} similar items")
        return processed_similar
    
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

    async def _handle_aggregation_query(
        self,
        query: str,
        schema: str,
        table: Optional[str] = None,
        top_k: int = 10
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Handle aggregation queries like "stok paling banyak", "harga tertinggi", etc.
        """
        query_lower = query.lower()

        # Detect superlative patterns
        superlative_patterns = {
            # Stock-related superlatives
            'highest_stock': ['stok paling banyak', 'stok terbanyak', 'stok tertinggi', 'paling banyak stok'],
            'lowest_stock': ['stok paling sedikit', 'stok terendah', 'stok paling rendah'],

            # Price-related superlatives
            'highest_price': ['harga tertinggi', 'harga paling mahal', 'paling mahal'],
            'lowest_price': ['harga terendah', 'harga paling murah', 'paling murah'],
        }

        # Check which pattern matches
        detected_pattern = None
        for pattern_type, patterns in superlative_patterns.items():
            if any(pattern in query_lower for pattern in patterns):
                detected_pattern = pattern_type
                break

        if not detected_pattern:
            return None  # Not an aggregation query

        logger.info(f"Aggregation query detected: {detected_pattern}")

        # Build appropriate aggregation query
        if detected_pattern == 'highest_stock':
            return await self._get_highest_stock_items(schema, table, top_k)
        elif detected_pattern == 'lowest_stock':
            return await self._get_lowest_stock_items(schema, table, top_k)
        elif detected_pattern == 'highest_price':
            return await self._get_highest_price_items(schema, table, top_k)
        elif detected_pattern == 'lowest_price':
            return await self._get_lowest_price_items(schema, table, top_k)

        return None

    async def _get_highest_stock_items(
        self,
        schema: str,
        table: Optional[str] = None,
        top_k: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get items with highest stock levels
        """
        try:
            # Query embeddings table but order by stock in metadata
            query = """
                SELECT
                    e.id,
                    e.schema_name,
                    e.table_name,
                    e.content,
                    e.metadata,
                    e.created_at,
                    0.95 as similarity,
                    (e.metadata->>'im_stock')::numeric as stock_value
                FROM atabot.embeddings e
                WHERE e.schema_name = $1
                    AND e.metadata->>'im_stock' IS NOT NULL
                    AND e.metadata->>'im_stock' ~ '^[0-9.]+$'  -- Only numeric values
                    AND (e.metadata->>'im_stock')::numeric > 0  -- Positive stock only
            """

            params = [schema]
            param_count = 2

            if table:
                query += f" AND e.table_name = ${param_count}"
                params.append(table)
                param_count += 1

            query += f" ORDER BY (e.metadata->>'im_stock')::numeric DESC LIMIT ${param_count}"
            params.append(top_k)

            logger.info(f"Executing highest stock query: {query[:200]}...")
            results = await db_pool.fetch(query, *params)

            # Process results
            processed_results = []
            for i, row in enumerate(results):
                try:
                    result = dict(row)

                    # Parse metadata
                    if result.get('metadata'):
                        metadata = result['metadata']
                        if isinstance(metadata, str):
                            result['metadata'] = json.loads(metadata)
                        elif not isinstance(metadata, dict):
                            result['metadata'] = {}
                    else:
                        result['metadata'] = {}

                    # Set score based on rank (highest stock = highest score)
                    result['score'] = 1.0 - (i * 0.05)  # 1.0, 0.95, 0.90, etc.

                    # Add source information
                    result['source'] = {
                        'schema': schema,
                        'table': result.get('table_name', table),
                        'id': result.get('id'),
                        'query_type': 'highest_stock_aggregation'
                    }

                    processed_results.append(result)

                    # Log for debugging
                    stock_val = result['metadata'].get('im_stock', 'N/A')
                    item_desc = result['metadata'].get('im_desc', 'N/A')
                    logger.info(f"Highest stock result {i+1}: {item_desc} - {stock_val} units")

                except Exception as e:
                    logger.error(f"Error processing highest stock result {i+1}: {e}")
                    continue

            logger.info(f"Found {len(processed_results)} highest stock items")
            return processed_results

        except Exception as e:
            logger.error(f"Failed to get highest stock items: {e}")
            return []

    async def _get_lowest_stock_items(self, schema: str, table: Optional[str] = None, top_k: int = 10) -> List[Dict[str, Any]]:
        """Get items with lowest stock levels (placeholder)"""
        # Similar to _get_highest_stock_items but with ASC order
        return []

    async def _get_highest_price_items(self, schema: str, table: Optional[str] = None, top_k: int = 10) -> List[Dict[str, Any]]:
        """Get items with highest prices (placeholder)"""
        return []

    async def _get_lowest_price_items(self, schema: str, table: Optional[str] = None, top_k: int = 10) -> List[Dict[str, Any]]:
        """Get items with lowest prices (placeholder)"""
        return []

    async def _fallback_text_search(
        self,
        query: str,
        schema: str,
        table: Optional[str] = None,
        top_k: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Enhanced PostgreSQL text search with product name focus
        """
        try:
            # Extract and prioritize search terms intelligently
            all_terms = query.split()
            product_terms = []  # High priority terms (likely product names)
            context_terms = []  # Lower priority context terms

            # Common context words that shouldn't be prioritized
            common_words = {'berapa', 'stok', 'stock', 'ada', 'dan', 'di', 'program', 'apa', 'yang', 'ini', 'itu'}

            for term in all_terms:
                cleaned_term = term.strip('.,?!:').upper()
                if len(cleaned_term) >= 2:
                    if cleaned_term.lower() in common_words:
                        context_terms.append(cleaned_term)
                    else:
                        product_terms.append(cleaned_term)  # Likely product name

            # Add some context terms for broader matching but with lower priority
            important_context = ['STOK', 'STOCK', 'PROGRAM']
            for term in context_terms:
                if term in important_context:
                    product_terms.append(term)

            if not product_terms:
                return []  # No meaningful product terms found

            logger.info(f"Product terms (high priority): {product_terms}")
            logger.info(f"Context terms (low priority): {context_terms}")

            # Build prioritized search conditions
            search_conditions = []
            params = []
            param_count = 1

            # Add exact phrase matching first (highest priority)
            if len(product_terms) >= 2:
                phrase = ' '.join(product_terms[:2])  # First two product terms
                search_conditions.append(f"e.content ILIKE ${param_count}")
                params.append(f"%{phrase}%")
                param_count += 1

            # Add individual product term matching
            for term in product_terms:
                search_conditions.append(f"e.content ILIKE ${param_count}")
                params.append(f"%{term}%")
                param_count += 1

            where_clause = " OR ".join(search_conditions)

            # Build full query with intelligent scoring based on match type
            scoring_case = "CASE"

            # Exact phrase match gets highest score
            if len(product_terms) >= 2:
                phrase = ' '.join(product_terms[:2])
                scoring_case += f" WHEN e.content ILIKE '%{phrase}%' THEN 1.0"

            # Individual product terms get high scores
            for i, term in enumerate(product_terms[:3]):  # Top 3 product terms
                score = 0.9 - (i * 0.1)  # Decreasing scores: 0.9, 0.8, 0.7
                scoring_case += f" WHEN e.content ILIKE '%{term}%' THEN {score}"

            scoring_case += " ELSE 0.5 END"

            text_search_query = f"""
                SELECT
                    e.id,
                    e.schema_name,
                    e.table_name,
                    e.content,
                    e.metadata,
                    e.created_at,
                    {scoring_case} as similarity
                FROM atabot.embeddings e
                WHERE e.schema_name = ${param_count}
                  AND ({where_clause})
            """
            params.append(schema)
            param_count += 1

            # Add table filter if specified
            if table:
                text_search_query += f" AND e.table_name = ${param_count}"
                params.append(table)

            # Order by similarity score and content length (shorter content often more relevant for product searches)
            text_search_query += f" ORDER BY similarity DESC, length(e.content) ASC LIMIT ${len(params) + 1}"
            params.append(top_k)

            logger.info(f"Fallback text search query: {text_search_query[:200]}...")
            logger.info(f"Fallback search parameters: {params}")
            results = await db_pool.fetch(text_search_query, *params)
            logger.info(f"Fallback text search raw results: {len(results)} found")

            # Log some raw results for debugging
            for i, row in enumerate(results[:3]):
                logger.info(f"Raw fallback result {i+1}: content='{dict(row).get('content', '')[:100]}...'")

            # Process results similar to hybrid search
            processed_results = []
            for i, row in enumerate(results):
                try:
                    result = dict(row)

                    # Parse metadata
                    if result.get('metadata'):
                        metadata = result['metadata']
                        if isinstance(metadata, str):
                            result['metadata'] = json.loads(metadata)
                        elif not isinstance(metadata, dict):
                            result['metadata'] = {}
                    else:
                        result['metadata'] = {}

                    # Calculate text relevance score
                    content = result.get('content', '')
                    text_score = self._calculate_content_boost(query, content)
                    result['score'] = text_score * 0.9  # High score for text matches

                    logger.debug(f"Fallback result {i+1}: content='{content[:50]}...', text_score={text_score:.4f}, final_score={result['score']:.4f}")

                    # Add source information
                    result['source'] = {
                        'schema': schema,
                        'table': result.get('table_name', table),
                        'id': result.get('id')
                    }

                    processed_results.append(result)

                except Exception as e:
                    logger.error(f"Error processing fallback result {i+1}: {e}")
                    continue

            # Sort by score
            processed_results.sort(key=lambda x: x['score'], reverse=True)

            logger.info(f"Fallback text search found {len(processed_results)} results")

            # Log top fallback results for debugging
            for i, result in enumerate(processed_results[:3]):
                logger.info(f"Top fallback result {i+1}: score={result['score']:.4f}, content='{result.get('content', '')[:100]}...'")

            return processed_results

        except Exception as e:
            logger.error(f"Fallback text search failed: {e}")
            return []

# Global search service instance
search_service = SearchService()