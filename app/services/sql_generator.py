"""
SQL Generator Service - Converts natural language to SQL
Uses schema understanding to generate accurate queries
"""
from typing import Dict, List, Any, Optional
from loguru import logger
import re

from app.core.llm import llm_client
from app.core.database import db_pool

class SQLGenerator:
    """
    Service for generating SQL queries from natural language
    """
    
    def __init__(self):
        # Query cache removed - using real-time generation only
        self.learned_patterns = {}
        
    async def generate_sql(
        self,
        natural_query: str,
        schema: str,
        context: Dict[str, Any] = {},
        allow_complex: bool = True
    ) -> Dict[str, Any]:
        """
        Generate SQL with smart routing
        """
        try:
            logger.info(f"Generating SQL for: {natural_query}")
            
            # TRY SMART ROUTER FIRST!
            from app.services.smart_router import smart_router
            
            # Get schema info for table hint
            schema_info = await self._get_schema_info(schema)
            table_hint = self._identify_target_table(natural_query, schema_info)
            
            # Try generate without LLM
            sql = smart_router.generate_sql_without_llm(
                natural_query, 
                schema, 
                table_hint
            )
            
            if sql:
                logger.info("SQL generated WITHOUT LLM!")
                return {
                    "sql": sql,
                    "intent": {"type": "simple"},
                    "schema": schema,
                    "confidence": 0.9,
                    "explanation": "Query processed without LLM",
                    "llm_used": False  # Track this!
                }
            
            # If smart router can't handle, continue with existing LLM logic
            logger.info("Complex query, using LLM...")
            logger.info(f"Generating SQL for: {natural_query}")
            
            # Get schema information
            schema_info = await self._get_schema_info(schema)
            
            # Detect query intent
            intent = self._analyze_query_intent(natural_query)
            
            # Generate SQL based on intent
            if intent['type'] == 'aggregation':
                sql = await self._generate_aggregation_sql(
                    natural_query, schema, schema_info, intent
                )
            elif intent['type'] == 'search':
                sql = await self._generate_search_sql(
                    natural_query, schema, schema_info, intent
                )
            elif intent['type'] == 'comparison':
                sql = await self._generate_comparison_sql(
                    natural_query, schema, schema_info, intent
                )
            elif intent['type'] == 'join' and allow_complex:
                sql = await self._generate_join_sql(
                    natural_query, schema, schema_info, intent
                )
            else:
                # Default to simple SELECT
                sql = await self._generate_simple_sql(
                    natural_query, schema, schema_info, intent
                )
            
            # Validate SQL
            validated_sql = await self._validate_sql(sql, schema)
            
            return {
                "sql": validated_sql,
                "intent": intent,
                "schema": schema,
                "confidence": self._calculate_confidence(validated_sql, intent),
                "explanation": self._explain_query(validated_sql, intent)
            }
            
        except Exception as e:
            logger.error(f"SQL generation failed: {e}")
            raise
    
    def _analyze_query_intent(self, query: str) -> Dict[str, Any]:
        """
        Analyze the intent and requirements of the query
        """
        query_lower = query.lower()
        
        intent = {
            'type': 'unknown',
            'aggregations': [],
            'filters': [],
            'grouping': [],
            'ordering': [],
            'limit': None,
            'entities': [],
            'requires_join': False
        }
        
        # Detect aggregations
        agg_patterns = {
            'count': r'\b(count|jumlah|berapa banyak|how many)\b',
            'sum': r'\b(sum|total|jumlah total)\b',
            'avg': r'\b(average|rata-rata|mean)\b',
            'max': r'\b(maximum|highest|tertinggi|terbesar)\b',
            'min': r'\b(minimum|lowest|terendah|terkecil)\b'
        }
        
        for agg_type, pattern in agg_patterns.items():
            if re.search(pattern, query_lower):
                intent['aggregations'].append(agg_type)
        
        if intent['aggregations']:
            intent['type'] = 'aggregation'
        
        # Detect search/filter patterns
        if re.search(r'\b(find|search|show|list|cari|tampilkan|where|yang)\b', query_lower):
            if not intent['type'] == 'aggregation':
                intent['type'] = 'search'
        
        # Detect comparison
        if re.search(r'\b(compare|versus|vs|bandingkan|dibanding|difference)\b', query_lower):
            intent['type'] = 'comparison'
        
        # Detect grouping
        if re.search(r'\b(by|per|group by|grouped|for each)\b', query_lower):
            # Extract grouping field
            intent['grouping'] = self._extract_grouping_fields(query)
        
        # Detect ordering
        if re.search(r'\b(top|bottom|highest|lowest|order by|sort)\b', query_lower):
            intent['ordering'] = self._extract_ordering(query)
        
        # Detect limit
        limit_match = re.search(r'\b(top|first|last)\s+(\d+)\b', query_lower)
        if limit_match:
            intent['limit'] = int(limit_match.group(2))
        
        # Detect multiple tables (potential JOIN)
        entities = self._extract_entities(query)
        intent['entities'] = entities
        if len(entities) > 1:
            intent['requires_join'] = True
            if intent['type'] == 'unknown':
                intent['type'] = 'join'
        
        return intent
    
    async def _get_schema_info(self, schema: str) -> Dict[str, Any]:
        """
        Get comprehensive schema information
        """
        # Get managed schema info
        query = """
            SELECT metadata, learned_patterns
            FROM atabot.managed_schemas
            WHERE schema_name = $1
        """
        
        result = await db_pool.fetchrow(query, schema)
        
        if not result:
            # Fallback to basic table info
            tables = await db_pool.get_tables(schema)
            schema_info = {
                'tables': {t['table_name']: {'row_count': t['estimated_row_count']} 
                          for t in tables}
            }
        else:
            # JSONB columns are already parsed by asyncpg
            metadata = result['metadata']
            learned_patterns = result['learned_patterns']
            
            # Handle both dict and string formats for safety
            if isinstance(metadata, str):
                import json
                try:
                    metadata = json.loads(metadata)
                except json.JSONDecodeError:
                    metadata = {}
            
            schema_info = metadata if metadata else {}
            
            # Add learned patterns if available
            if learned_patterns:
                if isinstance(learned_patterns, str):
                    import json
                    try:
                        learned_patterns = json.loads(learned_patterns)
                    except json.JSONDecodeError:
                        learned_patterns = {}
                
                if isinstance(learned_patterns, dict):
                    schema_info['patterns'] = learned_patterns
        
        # Get foreign keys for relationships
        foreign_keys = await db_pool.get_foreign_keys(schema)
        schema_info['relationships'] = foreign_keys
        
        return schema_info
    
    async def _generate_aggregation_sql(
        self,
        query: str,
        schema: str,
        schema_info: Dict[str, Any],
        intent: Dict[str, Any]
    ) -> str:
        """
        Generate SQL for aggregation queries
        """
        # Use LLM with schema context
        tables_context = self._format_schema_context(schema_info, limit=5)

        # DEBUG: Log the schema context being sent to LLM
        logger.info(f"=== SCHEMA CONTEXT FOR LLM ===")
        logger.info(f"Schema: {schema}")
        logger.info(f"Tables context: {tables_context}")
        logger.info(f"Schema info keys: {list(schema_info.keys())}")

        prompt = f"""Generate PostgreSQL query for this aggregation request.

            Schema: {schema}
            Query: "{query}"

            IMPORTANT - Use these EXACT table and column names:
            Table: {schema}.item_metadata
            Columns:
            - im_id (integer) - Item ID
            - im_desc (text) - Item description/name
            - im_program (text) - Program name
            - im_warehouse (text) - Warehouse location
            - im_stock (numeric) - Stock quantity
            - updated_at (timestamp) - Last update time

            For queries about:
            - "stok" or "stock" → use im_stock column
            - "barang" or "item" or product name → use im_desc column (with ILIKE for partial matches)
            - "program" → use im_program column

            Requirements:
            - Use appropriate aggregation functions: {', '.join(intent['aggregations'])}
            {"- Group by: " + ', '.join(intent['grouping']) if intent['grouping'] else ""}
            {"- Order by: " + ', '.join(intent['ordering']) if intent['ordering'] else ""}
            {"- Limit: " + str(intent['limit']) if intent['limit'] else ""}
            - Use ILIKE '%search_term%' for text searches
            - Always qualify table name as {schema}.item_metadata

            Return ONLY the SQL query without explanation.
        """

        logger.info(f"=== FULL PROMPT FOR LLM ===")
        logger.info(prompt)
                    
        sql = await llm_client.generate(
            prompt=prompt,
            temperature=0.1,
            max_tokens=500
        )
        
        return self._clean_sql(sql)
    
    async def _generate_search_sql(
        self,
        query: str,
        schema: str,
        schema_info: Dict[str, Any],
        intent: Dict[str, Any]
    ) -> str:
        """
        Generate SQL for search/filter queries
        """
        tables_context = self._format_schema_context(schema_info, limit=5)
        
        prompt = f"""Generate PostgreSQL SELECT query for this search request.

            Schema: {schema}
            Query: "{query}"

            Available tables and columns:
            {tables_context}

            Requirements:
            - Include relevant WHERE conditions
            - Use ILIKE for text searches
            - Return all relevant columns
            {"- Limit: " + str(intent['limit']) if intent['limit'] else "- Limit: 100"}

            Return ONLY the SQL query without explanation.
        """
        
        sql = await llm_client.generate(
            prompt=prompt,
            temperature=0.1,
            max_tokens=500
        )
        
        return self._clean_sql(sql)
    
    async def _generate_comparison_sql(
        self,
        query: str,
        schema: str,
        schema_info: Dict[str, Any],
        intent: Dict[str, Any]
    ) -> str:
        """
        Generate SQL for comparison queries
        """
        tables_context = self._format_schema_context(schema_info, limit=5)
        
        prompt = f"""Generate PostgreSQL query to compare data as requested.

            Schema: {schema}
            Query: "{query}"

            Available tables and columns:
            {tables_context}

            Requirements:
            - Use CASE statements or UNION for comparisons
            - Include clear labels for compared items
            - Calculate differences if applicable

            Return ONLY the SQL query without explanation.
        """
        
        sql = await llm_client.generate(
            prompt=prompt,
            temperature=0.1,
            max_tokens=500
        )
        
        return self._clean_sql(sql)
    
    async def _generate_join_sql(
        self,
        query: str,
        schema: str,
        schema_info: Dict[str, Any],
        intent: Dict[str, Any]
    ) -> str:
        """
        Generate SQL with JOIN operations
        """
        tables_context = self._format_schema_context(schema_info, limit=10)
        relationships = self._format_relationships(schema_info.get('relationships', []))
        
        prompt = f"""Generate PostgreSQL query with appropriate JOINs.

            Schema: {schema}
            Query: "{query}"

            Available tables and columns:
            {tables_context}

            Known relationships:
            {relationships}

            Requirements:
            - Use appropriate JOIN types (INNER, LEFT, etc.)
            - Join on correct foreign key relationships
            - Select relevant columns from all joined tables
            - Avoid Cartesian products

            Return ONLY the SQL query without explanation.
        """
        
        sql = await llm_client.generate(
            prompt=prompt,
            temperature=0.1,
            max_tokens=500
        )
        
        return self._clean_sql(sql)
    
    async def _generate_simple_sql(
        self,
        query: str,
        schema: str,
        schema_info: Dict[str, Any],
        intent: Dict[str, Any]
    ) -> str:
        """
        Generate simple SELECT SQL
        """
        # Determine most likely table
        table = self._identify_target_table(query, schema_info)
        
        if not table:
            # Fallback to LLM
            tables_context = self._format_schema_context(schema_info, limit=5)
            prompt = f"""Generate PostgreSQL SELECT query.

            Schema: {schema}
            Query: "{query}"

            Available tables:
            {tables_context}

            Return ONLY the SQL query without explanation.
        """
            
            sql = await llm_client.generate(
                prompt=prompt,
                temperature=0.1,
                max_tokens=300
            )
            return self._clean_sql(sql)
        
        # Generate simple SELECT
        return f"SELECT * FROM {schema}.{table} LIMIT 100"
    
    def _format_schema_context(
        self,
        schema_info: Dict[str, Any],
        limit: int = 10
    ) -> str:
        """
        Format schema information for LLM context
        """
        context_parts = []
        
        tables = list(schema_info.get('tables', {}).items())[:limit]
        
        for table_name, table_info in tables:
            if isinstance(table_info, dict):
                # Include column information if available
                if 'columns' in table_info:
                    columns = []
                    for col_name, col_info in table_info['columns'].items():
                        if isinstance(col_info, dict):
                            col_type = col_info.get('type', 'unknown')
                            columns.append(f"  - {col_name} ({col_type})")
                        elif isinstance(col_info, list):
                            # Handle column lists
                            for col in col_info[:5]:  # Limit columns
                                if isinstance(col, str):
                                    columns.append(f"  - {col}")
                    
                    if columns:
                        context_parts.append(f"Table: {table_name}")
                        context_parts.extend(columns[:10])  # Limit to 10 columns
                else:
                    context_parts.append(f"Table: {table_name} ({table_info.get('row_count', 0)} rows)")
        
        return "\n".join(context_parts)
    
    def _format_relationships(self, relationships: List[Dict[str, Any]]) -> str:
        """
        Format foreign key relationships for context
        """
        if not relationships:
            return "No explicit foreign keys found"
        
        rel_strings = []
        for rel in relationships[:10]:  # Limit to 10 relationships
            rel_strings.append(
                f"- {rel['table_name']}.{rel['column_name']} -> "
                f"{rel['foreign_table_name']}.{rel['foreign_column_name']}"
            )
        
        return "\n".join(rel_strings)
    
    def _identify_target_table(
        self,
        query: str,
        schema_info: Dict[str, Any]
    ) -> Optional[str]:
        """
        Identify the most likely target table from query
        """
        query_lower = query.lower()
        tables = schema_info.get('tables', {})
        
        # Check for exact table name matches
        for table_name in tables.keys():
            if table_name.lower() in query_lower:
                return table_name
        
        # Check for entity type matches
        for table_name, table_info in tables.items():
            if isinstance(table_info, dict):
                entity_type = table_info.get('entity_type', '')
                if entity_type and entity_type in query_lower:
                    return table_name
        
        # If no specific table found, return the first table
        if tables:
            return list(tables.keys())[0]
        
        return None
    
    async def _validate_sql(self, sql: str, schema: str) -> str:
        """
        Validate and potentially fix SQL query
        """
        try:
            if not sql.strip():
                raise ValueError("Empty SQL query")
            
            # Check for dangerous operations
            dangerous_keywords = ['DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'CREATE', 'INSERT', 'UPDATE']
            sql_upper = sql.upper()
            for keyword in dangerous_keywords:
                if keyword in sql_upper:
                    raise ValueError(f"Dangerous operation {keyword} not allowed")
            
            # Fix schema references - prevent double schema naming
            # Remove any existing schema references first
            sql = re.sub(rf'\b{re.escape(schema)}\.{re.escape(schema)}\b', schema, sql)
            
            # Ensure proper schema qualification
            if f'"{schema}".' not in sql and f'{schema}.' not in sql:
                # Add schema to unqualified table references
                sql = re.sub(
                    r'\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*)\b',
                    rf'FROM "{schema}".\1',
                    sql,
                    flags=re.IGNORECASE
                )
                sql = re.sub(
                    r'\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)\b',
                    rf'JOIN "{schema}".\1',
                    sql,
                    flags=re.IGNORECASE
                )
            
            return sql
            
        except Exception as e:
            logger.warning(f"SQL validation warning: {e}")
            return sql
    
    async def _generate_simple_sql(
        self,
        query: str,
        schema: str,
        schema_info: Dict[str, Any],
        intent: Dict[str, Any]
    ) -> str:
        """
        Generate simple SELECT SQL
        """
        # Determine most likely table
        table = self._identify_target_table(query, schema_info)
        
        if not table:
            # Fallback to LLM if no table identified
            tables_context = self._format_schema_context(schema_info, limit=5)
            prompt = f"""Generate PostgreSQL SELECT query.

                Schema: {schema}
                Query: "{query}"

                Available tables:
                {tables_context}

                Return ONLY the SQL query without explanation."""
            
            sql = await llm_client.generate(
                prompt=prompt,
                temperature=0.1,
                max_tokens=300
            )
            return self._clean_sql(sql)
        
        # Generate simple SELECT with proper schema qualification
        return f'SELECT * FROM "{schema}"."{table}" LIMIT 100'
    
    def _extract_entities(self, query: str) -> List[str]:
        """
        Extract potential entity references from query
        """
        # Simple extraction of capitalized words and quoted strings
        entities = []
        
        # Capitalized words
        entities.extend(re.findall(r'\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b', query))
        
        # Quoted strings
        entities.extend(re.findall(r'"([^"]+)"', query))
        entities.extend(re.findall(r"'([^']+)'", query))
        
        return list(set(entities))
    
    def _extract_grouping_fields(self, query: str) -> List[str]:
        """
        Extract GROUP BY fields from natural language
        """
        grouping = []
        
        # Look for "by [field]" patterns
        by_pattern = re.findall(r'\bby\s+(\w+)', query.lower())
        grouping.extend(by_pattern)
        
        # Look for "per [field]" patterns
        per_pattern = re.findall(r'\bper\s+(\w+)', query.lower())
        grouping.extend(per_pattern)
        
        return list(set(grouping))
    
    def _extract_ordering(self, query: str) -> List[str]:
        """
        Extract ORDER BY fields from natural language
        """
        ordering = []
        query_lower = query.lower()
        
        # Detect ordering direction
        if 'highest' in query_lower or 'top' in query_lower or 'largest' in query_lower:
            ordering.append('DESC')
        elif 'lowest' in query_lower or 'bottom' in query_lower or 'smallest' in query_lower:
            ordering.append('ASC')
        
        return ordering
    
    def _clean_sql(self, sql: str) -> str:
        """
        Clean and format SQL query
        """
        # Remove markdown code blocks if present
        sql = re.sub(r'```sql\s*', '', sql)
        sql = re.sub(r'```\s*', '', sql)
        
        # Remove comments
        sql = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
        
        # Clean whitespace
        sql = ' '.join(sql.split())
        
        # Ensure semicolon at end
        sql = sql.rstrip(';') + ';'
        
        return sql
    
    async def _validate_sql(self, sql: str, schema: str) -> str:
        """
        Validate and potentially fix SQL query
        """
        try:
            # Basic syntax validation using EXPLAIN
            explain_query = f"EXPLAIN (FORMAT JSON) {sql}"
            
            # Try to explain the query (will fail if syntax is invalid)
            # Note: This is a lightweight check, not actual execution
            # In production, you might want to use a SQL parser library
            
            # For now, just do basic validation
            if not sql.strip():
                raise ValueError("Empty SQL query")
            
            # Check for dangerous operations
            dangerous_keywords = ['DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'CREATE', 'INSERT', 'UPDATE']
            sql_upper = sql.upper()
            for keyword in dangerous_keywords:
                if keyword in sql_upper:
                    raise ValueError(f"Dangerous operation {keyword} not allowed")
            
            # Ensure schema is specified if not already
            if f"{schema}." not in sql and "FROM" in sql.upper():
                # Add schema to table references
                sql = re.sub(
                    r'\bFROM\s+(\w+)',
                    f'FROM {schema}.\\1',
                    sql,
                    flags=re.IGNORECASE
                )
                sql = re.sub(
                    r'\bJOIN\s+(\w+)',
                    f'JOIN {schema}.\\1',
                    sql,
                    flags=re.IGNORECASE
                )
            
            return sql
            
        except Exception as e:
            logger.warning(f"SQL validation warning: {e}")
            # Return original SQL if validation fails
            # In production, you might want to handle this differently
            return sql
    
    def _calculate_confidence(self, sql: str, intent: Dict[str, Any]) -> float:
        """
        Calculate confidence score for generated SQL
        """
        confidence = 0.5  # Base confidence
        
        # Increase confidence based on intent clarity
        if intent['type'] != 'unknown':
            confidence += 0.2
        
        # Increase confidence if aggregations match
        if intent['aggregations']:
            sql_upper = sql.upper()
            for agg in intent['aggregations']:
                if agg.upper() in sql_upper:
                    confidence += 0.1
        
        # Increase confidence if query is simple
        if 'JOIN' not in sql.upper():
            confidence += 0.1
        
        # Decrease confidence for complex queries
        if sql.count('JOIN') > 2:
            confidence -= 0.2
        
        return min(1.0, max(0.0, confidence))
    
    def _explain_query(self, sql: str, intent: Dict[str, Any]) -> str:
        """
        Generate human-readable explanation of the SQL query
        """
        explanation_parts = []
        
        sql_upper = sql.upper()
        
        # Explain operation type
        if 'SELECT' in sql_upper:
            if intent['aggregations']:
                explanation_parts.append(f"This query calculates {', '.join(intent['aggregations'])}")
            else:
                explanation_parts.append("This query retrieves data")
        
        # Explain tables involved
        tables = re.findall(r'FROM\s+(\S+)', sql, re.IGNORECASE)
        if tables:
            explanation_parts.append(f"from {', '.join(tables)}")
        
        # Explain filters
        if 'WHERE' in sql_upper:
            explanation_parts.append("with specific conditions")
        
        # Explain grouping
        if 'GROUP BY' in sql_upper:
            explanation_parts.append("grouped by certain fields")
        
        # Explain ordering
        if 'ORDER BY' in sql_upper:
            if 'DESC' in sql_upper:
                explanation_parts.append("sorted in descending order")
            else:
                explanation_parts.append("sorted in ascending order")
        
        # Explain limit
        limit_match = re.search(r'LIMIT\s+(\d+)', sql, re.IGNORECASE)
        if limit_match:
            explanation_parts.append(f"limited to {limit_match.group(1)} results")
        
        return ' '.join(explanation_parts) + '.'
    
    async def execute_sql(
        self,
        sql: str,
        schema: str,
        limit: int = 1000
    ) -> Dict[str, Any]:
        """
        Execute SQL query and return results
        """
        try:
            # Add safety limit if not present
            if 'LIMIT' not in sql.upper():
                sql = sql.rstrip(';') + f' LIMIT {limit};'
            
            # Execute query
            rows = await db_pool.fetch(sql)
            
            # Convert to list of dicts
            results = [dict(row) for row in rows]
            
            return {
                "success": True,
                "rows": results,
                "row_count": len(results),
                "columns": list(results[0].keys()) if results else []
            }
            
        except Exception as e:
            logger.error(f"SQL execution failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "rows": [],
                "row_count": 0
            }

# Global SQL generator instance
sql_generator = SQLGenerator()