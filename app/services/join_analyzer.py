"""
Join Analyzer Service - Automatically detects and handles multi-table relationships
Enables Atabot to work with related data across multiple tables
"""
from typing import Dict, List, Any, Optional, Tuple
from loguru import logger
import re
from app.core.database import db_pool

class JoinAnalyzer:
    """
    Service for analyzing and executing multi-table joins
    """

    def __init__(self):
        self.relationship_cache = {}
        self.foreign_key_cache = {}

    async def analyze_schema_relationships(self, schema: str) -> Dict[str, Any]:
        """
        Analyze all relationships in a schema
        """
        if schema in self.relationship_cache:
            return self.relationship_cache[schema]

        try:
            # Get all foreign key relationships
            fk_query = """
                SELECT
                    tc.table_name as source_table,
                    kcu.column_name as source_column,
                    ccu.table_name as target_table,
                    ccu.column_name as target_column,
                    tc.constraint_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage ccu
                    ON ccu.constraint_name = tc.constraint_name
                    AND ccu.table_schema = tc.table_schema
                WHERE tc.constraint_type = 'FOREIGN KEY'
                    AND tc.table_schema = $1
                ORDER BY tc.table_name, kcu.ordinal_position
            """

            relationships = await db_pool.fetch(fk_query, schema)

            # Organize relationships
            schema_relationships = {
                'tables': {},
                'foreign_keys': [],
                'join_paths': {}
            }

            for rel in relationships:
                source_table = rel['source_table']
                target_table = rel['target_table']

                # Add to foreign keys list
                schema_relationships['foreign_keys'].append({
                    'source_table': source_table,
                    'source_column': rel['source_column'],
                    'target_table': target_table,
                    'target_column': rel['target_column'],
                    'constraint_name': rel['constraint_name']
                })

                # Build table relationships
                if source_table not in schema_relationships['tables']:
                    schema_relationships['tables'][source_table] = {
                        'references': [],
                        'referenced_by': []
                    }
                if target_table not in schema_relationships['tables']:
                    schema_relationships['tables'][target_table] = {
                        'references': [],
                        'referenced_by': []
                    }

                # Add references
                schema_relationships['tables'][source_table]['references'].append({
                    'table': target_table,
                    'local_column': rel['source_column'],
                    'foreign_column': rel['target_column']
                })

                schema_relationships['tables'][target_table]['referenced_by'].append({
                    'table': source_table,
                    'local_column': rel['target_column'],
                    'foreign_column': rel['source_column']
                })

            # Build join paths for common patterns
            schema_relationships['join_paths'] = self._build_join_paths(schema_relationships)

            # Cache results
            self.relationship_cache[schema] = schema_relationships

            logger.info(f"Analyzed {len(relationships)} relationships in schema '{schema}'")
            return schema_relationships

        except Exception as e:
            logger.error(f"Failed to analyze schema relationships: {e}")
            return {'tables': {}, 'foreign_keys': [], 'join_paths': {}}

    def _build_join_paths(self, relationships: Dict[str, Any]) -> Dict[str, List[Dict]]:
        """
        Build common join paths between tables
        """
        join_paths = {}
        tables = relationships['tables']

        for table1 in tables.keys():
            for table2 in tables.keys():
                if table1 != table2:
                    path = self._find_join_path(table1, table2, tables)
                    if path:
                        key = f"{table1}_to_{table2}"
                        join_paths[key] = path

        return join_paths

    def _find_join_path(self, source: str, target: str, tables: Dict) -> Optional[List[Dict]]:
        """
        Find the shortest join path between two tables
        """
        # Direct relationship check
        if source in tables:
            # Check if source directly references target
            for ref in tables[source]['references']:
                if ref['table'] == target:
                    return [{
                        'from_table': source,
                        'to_table': target,
                        'join_condition': f"{source}.{ref['local_column']} = {target}.{ref['foreign_column']}"
                    }]

            # Check if target references source
            for ref in tables[source]['referenced_by']:
                if ref['table'] == target:
                    return [{
                        'from_table': source,
                        'to_table': target,
                        'join_condition': f"{source}.{ref['local_column']} = {target}.{ref['foreign_column']}"
                    }]

        # For now, only handle direct relationships
        # Could be expanded for multi-hop joins
        return None

    async def detect_join_opportunity(self, query: str, schema: str) -> Optional[Dict[str, Any]]:
        """
        Detect if query requires joining multiple tables
        """
        try:
            # Get schema relationships
            relationships = await self.analyze_schema_relationships(schema)

            # Analyze query for table/entity mentions
            mentioned_entities = self._extract_entities_from_query(query)

            if len(mentioned_entities) < 2:
                return None

            # Find potential table matches for entities
            table_matches = await self._match_entities_to_tables(mentioned_entities, schema, relationships)

            if len(table_matches) < 2:
                return None

            # Check if we can join these tables
            join_strategy = self._plan_join_strategy(table_matches, relationships)

            if join_strategy:
                logger.info(f"Join opportunity detected: {join_strategy}")
                return join_strategy

            return None

        except Exception as e:
            logger.error(f"Failed to detect join opportunity: {e}")
            return None

    def _extract_entities_from_query(self, query: str) -> List[str]:
        """
        Extract potential entity/table names from query
        """
        # Common patterns that might indicate different entities
        patterns = [
            r'\b(status|state|condition)\b',
            r'\b(category|type|kind)\b',
            r'\b(customer|client|user)\b',
            r'\b(product|item|goods)\b',
            r'\b(order|transaction|sale)\b',
            r'\b(supplier|vendor|provider)\b',
            r'\b(warehouse|location|branch)\b',
            r'\b(program|campaign|promotion)\b'
        ]

        entities = []
        query_lower = query.lower()

        for pattern in patterns:
            matches = re.findall(pattern, query_lower)
            entities.extend(matches)

        # Also extract capitalized words (potential proper nouns)
        proper_nouns = re.findall(r'\b[A-Z][A-Z0-9_]*\b', query)
        entities.extend([noun.lower() for noun in proper_nouns])

        return list(set(entities))  # Remove duplicates

    async def _match_entities_to_tables(
        self,
        entities: List[str],
        schema: str,
        relationships: Dict[str, Any]
    ) -> List[str]:
        """
        Match extracted entities to actual table names
        """
        # Get all tables in schema
        tables_query = """
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = $1
            ORDER BY table_name
        """

        schema_info = await db_pool.fetch(tables_query, schema)

        table_names = set()
        table_columns = {}

        for row in schema_info:
            table_name = row['table_name']
            column_name = row['column_name']

            table_names.add(table_name)

            if table_name not in table_columns:
                table_columns[table_name] = []
            table_columns[table_name].append(column_name)

        matched_tables = []

        for entity in entities:
            # Direct table name match
            for table in table_names:
                if entity in table.lower() or table.lower() in entity:
                    matched_tables.append(table)
                    continue

            # Column name match (might indicate related data)
            for table, columns in table_columns.items():
                for column in columns:
                    if entity in column.lower():
                        matched_tables.append(table)
                        break

        return list(set(matched_tables))  # Remove duplicates

    def _plan_join_strategy(
        self,
        tables: List[str],
        relationships: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Plan how to join the identified tables
        """
        if len(tables) < 2:
            return None

        join_strategy = {
            'tables': tables,
            'joins': [],
            'primary_table': tables[0]  # Start with first table
        }

        # Try to find join paths between tables
        for i in range(len(tables) - 1):
            table1 = tables[i]
            table2 = tables[i + 1]

            # Look for direct relationship
            join_path_key = f"{table1}_to_{table2}"
            reverse_key = f"{table2}_to_{table1}"

            join_path = relationships['join_paths'].get(join_path_key) or \
                       relationships['join_paths'].get(reverse_key)

            if join_path:
                join_strategy['joins'].extend(join_path)
            else:
                # No direct relationship found
                logger.warning(f"No join path found between {table1} and {table2}")
                return None

        return join_strategy if join_strategy['joins'] else None

    async def execute_join_query(
        self,
        join_strategy: Dict[str, Any],
        schema: str,
        where_conditions: List[str] = [],
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Execute a join query based on the strategy
        """
        try:
            # Build SELECT clause
            select_columns = []
            for table in join_strategy['tables']:
                # Add a few key columns from each table
                select_columns.append(f"{table}.*")

            # Build FROM clause
            primary_table = join_strategy['primary_table']
            from_clause = f'"{schema}"."{primary_table}"'

            # Build JOIN clauses
            join_clauses = []
            for join in join_strategy['joins']:
                from_table = join['from_table']
                to_table = join['to_table']
                condition = join['join_condition']

                if from_table != primary_table:
                    join_clauses.append(
                        f'LEFT JOIN "{schema}"."{to_table}" ON {condition}'
                    )

            # Build WHERE clause
            where_clause = ""
            if where_conditions:
                where_clause = "WHERE " + " AND ".join(where_conditions)

            # Construct full query
            query = f"""
                SELECT {', '.join(select_columns)}
                FROM {from_clause}
                {' '.join(join_clauses)}
                {where_clause}
                LIMIT {limit}
            """

            logger.info(f"Executing join query: {query[:200]}...")

            results = await db_pool.fetch(query)
            return [dict(row) for row in results]

        except Exception as e:
            logger.error(f"Failed to execute join query: {e}")
            raise

# Global join analyzer instance
join_analyzer = JoinAnalyzer()