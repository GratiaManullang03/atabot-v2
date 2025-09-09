"""
Universal Schema Analyzer
Learns from ANY database structure without hardcoded assumptions
"""
from typing import Dict, List, Any
from collections import defaultdict, Counter
import re
from datetime import datetime
from loguru import logger
import json

from app.core.database import db_pool

class SchemaAnalyzer:
    """
    Analyzes database schemas to understand:
    - Entity types (person, product, transaction, etc)
    - Relationships between tables
    - Business terminology
    - Data patterns and types
    """
    
    def __init__(self):
        self.learned_patterns = {}
        self.entity_patterns = self._init_entity_patterns()
        self.terminology_map = defaultdict(set)
    
    def _init_entity_patterns(self) -> Dict[str, List[str]]:
        """
        Initialize minimal pattern recognition
        These are NOT hardcoded business terms, just linguistic patterns
        """
        return {
            'person': ['user', 'person', 'customer', 'client', 'patient', 'student', 
                      'employee', 'staff', 'member', 'contact', 'individual'],
            'transaction': ['order', 'transaction', 'sale', 'purchase', 'payment', 
                           'invoice', 'receipt', 'billing', 'transfer'],
            'item': ['product', 'item', 'article', 'goods', 'material', 'asset', 
                    'resource', 'inventory', 'stock'],
            'location': ['location', 'address', 'place', 'site', 'branch', 'store', 
                        'warehouse', 'facility', 'region', 'area'],
            'time_event': ['event', 'appointment', 'schedule', 'booking', 'reservation',
                          'session', 'meeting', 'activity'],
            'document': ['document', 'file', 'report', 'record', 'note', 'attachment',
                        'message', 'email', 'letter'],
            'category': ['category', 'type', 'class', 'group', 'tag', 'label',
                        'classification', 'segment'],
            'measurement': ['metric', 'measure', 'statistic', 'analytics', 'performance',
                           'score', 'rating', 'evaluation']
        }
    
    async def analyze_schema(self, schema_name: str) -> Dict[str, Any]:
        """
        Comprehensive schema analysis with learning
        """
        logger.info(f"Analyzing schema: {schema_name}")
        
        # Get all tables in schema
        tables = await db_pool.get_tables(schema_name)
        
        # Get foreign key relationships
        foreign_keys = await db_pool.get_foreign_keys(schema_name)
        
        # Analyze each table
        table_analyses = {}
        for table in tables:
            table_name = table['table_name']
            table_analyses[table_name] = await self._analyze_table(
                schema_name, 
                table_name,
                table['estimated_row_count']
            )
        
        # Detect business domain
        business_domain = self._detect_business_domain(table_analyses)
        
        # Build relationship graph
        relationships = self._build_relationship_graph(foreign_keys, table_analyses)
        
        # Learn terminology from actual data
        terminology = await self._learn_terminology(schema_name, tables[:5])  # Sample first 5 tables
        
        # Generate schema summary
        analysis = {
            'schema_name': schema_name,
            'business_domain': business_domain,
            'total_tables': len(tables),
            'total_rows': sum(t['estimated_row_count'] for t in tables),
            'table_analyses': table_analyses,
            'relationships': relationships,
            'terminology': terminology,
            'entity_graph': self._create_entity_graph(table_analyses, relationships),
            'discovered_at': datetime.now().isoformat()
        }
        
        # Store learned patterns
        await self._store_learned_patterns(schema_name, analysis)
        
        return analysis
    
    async def _analyze_table(
        self, 
        schema_name: str, 
        table_name: str,
        row_count: int
    ) -> Dict[str, Any]:
        """
        Analyze individual table structure and content
        """
        # Get column information
        columns = await db_pool.get_table_info(schema_name, table_name)
        
        # Detect entity type from table name
        entity_type = self._detect_entity_type(table_name)
        
        # Analyze columns
        column_analysis = self._analyze_columns(columns)
        
        # Sample data for pattern learning (only if table has data)
        patterns = {}
        if row_count > 0:
            sample_data = await db_pool.sample_data(schema_name, table_name, limit=10)
            patterns = self._learn_data_patterns(sample_data)
        
        return {
            'table_name': table_name,
            'entity_type': entity_type,
            'row_count': row_count,
            'columns': column_analysis,
            'primary_key': column_analysis.get('primary_key'),
            'foreign_keys': column_analysis.get('foreign_keys', []),
            'data_patterns': patterns,
            'searchable_fields': self._identify_searchable_fields(column_analysis),
            'display_fields': self._identify_display_fields(column_analysis)
        }
    
    def _detect_entity_type(self, table_name: str) -> str:
        """
        Detect entity type from table name using patterns
        NOT hardcoded business logic!
        """
        table_lower = table_name.lower()
        
        # Remove common prefixes/suffixes
        cleaned_name = re.sub(r'^(tbl_|t_|tb_)', '', table_lower)
        cleaned_name = re.sub(r'(_table|_tbl|_tb|s)$', '', cleaned_name)
        
        # Check against entity patterns
        for entity_type, patterns in self.entity_patterns.items():
            for pattern in patterns:
                if pattern in cleaned_name or cleaned_name in pattern:
                    return entity_type
        
        # Check for common patterns
        if re.search(r'(log|audit|history)', cleaned_name):
            return 'audit'
        elif re.search(r'(config|setting|parameter)', cleaned_name):
            return 'configuration'
        elif re.search(r'(map|mapping|relation|link)', cleaned_name):
            return 'relationship'
        
        return 'unknown'
    
    def _analyze_columns(self, columns: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analyze column structures to understand data model
        """
        analysis = {
            'total_columns': len(columns),
            'column_types': defaultdict(list),
            'nullable_columns': [],
            'primary_key': None,
            'foreign_keys': [],
            'timestamp_columns': [],
            'numeric_columns': [],
            'text_columns': [],
            'boolean_columns': []
        }
        
        for col in columns:
            col_name = col['column_name']
            col_type = col['data_type'].lower()
            
            # Categorize columns by type
            if 'int' in col_type or 'serial' in col_type:
                analysis['column_types']['integer'].append(col_name)
                analysis['numeric_columns'].append(col_name)
                
                # Detect primary key patterns
                if any(pk in col_name.lower() for pk in ['id', 'code', 'key']):
                    if not analysis['primary_key']:
                        analysis['primary_key'] = col_name
                        
            elif 'numeric' in col_type or 'decimal' in col_type or 'float' in col_type:
                analysis['column_types']['decimal'].append(col_name)
                analysis['numeric_columns'].append(col_name)
                
            elif 'char' in col_type or 'text' in col_type:
                analysis['column_types']['text'].append(col_name)
                analysis['text_columns'].append(col_name)
                
            elif 'timestamp' in col_type or 'date' in col_type or 'time' in col_type:
                analysis['column_types']['temporal'].append(col_name)
                analysis['timestamp_columns'].append(col_name)
                
            elif 'bool' in col_type:
                analysis['column_types']['boolean'].append(col_name)
                analysis['boolean_columns'].append(col_name)
                
            elif 'json' in col_type:
                analysis['column_types']['json'].append(col_name)
            
            # Track nullable columns
            if col['is_nullable']:
                analysis['nullable_columns'].append(col_name)
            
            # Detect foreign key patterns
            if col_name.endswith('_id') or col_name.endswith('_code'):
                potential_ref = col_name[:-3] if col_name.endswith('_id') else col_name[:-5]
                analysis['foreign_keys'].append({
                    'column': col_name,
                    'potential_reference': potential_ref
                })
        
        return analysis
    
    def _learn_data_patterns(self, sample_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Learn patterns from actual data samples
        """
        if not sample_data:
            return {}
        
        patterns = {
            'value_patterns': defaultdict(dict),
            'data_formats': defaultdict(str),
            'field_semantics': {}
        }
        
        for column in sample_data[0].keys():
            values = [row.get(column) for row in sample_data if row.get(column) is not None]
            
            if not values:
                continue
            
            # Detect data patterns
            if all(isinstance(v, (int, float)) for v in values):
                # Numeric pattern analysis
                min_val = min(values)
                max_val = max(values)
                avg_val = sum(values) / len(values)
                
                patterns['value_patterns'][column] = {
                    'type': 'numeric',
                    'range': [min_val, max_val],
                    'average': avg_val
                }
                
                # Detect if it's likely currency (large numbers, decimals)
                if avg_val > 100 and any(isinstance(v, float) for v in values):
                    patterns['field_semantics'][column] = 'currency'
                elif all(0 <= v <= 100 for v in values):
                    patterns['field_semantics'][column] = 'percentage'
                elif all(v >= 0 and isinstance(v, int) for v in values):
                    patterns['field_semantics'][column] = 'quantity'
                    
            elif all(isinstance(v, str) for v in values):
                # Text pattern analysis
                avg_length = sum(len(v) for v in values) / len(values)
                
                # Check for common patterns
                if all(re.match(r'^[A-Z]{2,5}\d+$', v) for v in values[:3]):
                    patterns['data_formats'][column] = 'code'
                elif all('@' in v for v in values[:3]):
                    patterns['data_formats'][column] = 'email'
                elif all(re.match(r'^\+?\d[\d\s\-\(\)]+$', v) for v in values[:3]):
                    patterns['data_formats'][column] = 'phone'
                elif avg_length > 100:
                    patterns['data_formats'][column] = 'description'
                else:
                    patterns['data_formats'][column] = 'text'
                
                patterns['value_patterns'][column] = {
                    'type': 'text',
                    'avg_length': avg_length,
                    'sample_values': list(set(values[:5]))
                }
        
        return patterns
    
    def _detect_business_domain(self, table_analyses: Dict[str, Any]) -> str:
        """
        Intelligently detect the business domain from table structures
        """
        entity_counts = Counter()
        
        for analysis in table_analyses.values():
            entity_type = analysis['entity_type']
            if entity_type != 'unknown':
                entity_counts[entity_type] += 1
        
        # Look for domain-specific patterns
        table_names = [a['table_name'].lower() for a in table_analyses.values()]
        
        # Healthcare indicators
        if any(term in ' '.join(table_names) for term in ['patient', 'diagnosis', 'medical', 'health', 'treatment']):
            return 'healthcare'
        
        # Education indicators
        elif any(term in ' '.join(table_names) for term in ['student', 'course', 'grade', 'enrollment', 'teacher']):
            return 'education'
        
        # E-commerce/Retail indicators
        elif any(term in ' '.join(table_names) for term in ['product', 'order', 'cart', 'customer', 'payment']):
            return 'retail'
        
        # Finance indicators
        elif any(term in ' '.join(table_names) for term in ['account', 'transaction', 'balance', 'ledger', 'invoice']):
            return 'finance'
        
        # Manufacturing indicators
        elif any(term in ' '.join(table_names) for term in ['production', 'inventory', 'warehouse', 'supplier', 'material']):
            return 'manufacturing'
        
        # HR indicators
        elif any(term in ' '.join(table_names) for term in ['employee', 'payroll', 'attendance', 'leave', 'department']):
            return 'human_resources'
        
        # Default to most common entity type
        if entity_counts:
            most_common = entity_counts.most_common(1)[0][0]
            return f"{most_common}_management"
        
        return 'general'
    
    def _build_relationship_graph(
        self, 
        foreign_keys: List[Dict[str, Any]], 
        table_analyses: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Build a graph of relationships between tables
        """
        relationships = []
        
        # Process explicit foreign keys
        for fk in foreign_keys:
            relationships.append({
                'from_table': fk['table_name'],
                'from_column': fk['column_name'],
                'to_table': fk['foreign_table_name'],
                'to_column': fk['foreign_column_name'],
                'type': 'foreign_key'
            })
        
        # Detect implicit relationships (same column names)
        tables = list(table_analyses.keys())
        for i, table1 in enumerate(tables):
            for table2 in tables[i+1:]:
                cols1 = set(table_analyses[table1]['columns']['text_columns'] + 
                          table_analyses[table1]['columns']['numeric_columns'])
                cols2 = set(table_analyses[table2]['columns']['text_columns'] + 
                          table_analyses[table2]['columns']['numeric_columns'])
                
                common_cols = cols1.intersection(cols2)
                for col in common_cols:
                    if col.endswith('_id') or col.endswith('_code'):
                        relationships.append({
                            'from_table': table1,
                            'from_column': col,
                            'to_table': table2,
                            'to_column': col,
                            'type': 'implicit'
                        })
        
        return relationships
    
    async def _learn_terminology(
        self, 
        schema_name: str, 
        tables: List[Dict[str, Any]]
    ) -> Dict[str, List[str]]:
        """
        Learn business terminology from actual data
        """
        terminology = defaultdict(set)
        
        for table in tables[:5]:  # Sample first 5 tables
            table_name = table['table_name']
            
            # Get column names
            columns = await db_pool.get_table_info(schema_name, table_name)
            
            for col in columns:
                col_name = col['column_name'].lower()
                
                # Extract meaningful parts
                parts = re.split(r'[_\s]+', col_name)
                
                for part in parts:
                    if len(part) > 2 and not part.isdigit():
                        # Group similar terms
                        if 'price' in part or 'cost' in part or 'amount' in part:
                            terminology['monetary'].add(part)
                        elif 'date' in part or 'time' in part:
                            terminology['temporal'].add(part)
                        elif 'name' in part or 'title' in part:
                            terminology['identifier'].add(part)
                        elif 'desc' in part or 'note' in part:
                            terminology['description'].add(part)
                        else:
                            terminology['general'].add(part)
        
        # Convert sets to lists for JSON serialization
        return {k: list(v) for k, v in terminology.items()}
    
    def _identify_searchable_fields(self, column_analysis: Dict[str, Any]) -> List[str]:
        """
        Identify which fields are good for searching
        """
        searchable = []
        
        # Text fields are generally searchable
        searchable.extend(column_analysis['text_columns'])
        
        # Add specific numeric fields that might be searched
        for col in column_analysis['numeric_columns']:
            if any(term in col.lower() for term in ['code', 'id', 'number', 'no']):
                searchable.append(col)
        
        return searchable
    
    def _identify_display_fields(self, column_analysis: Dict[str, Any]) -> List[str]:
        """
        Identify which fields are good for display
        """
        display = []
        
        # Prioritize name/title fields
        for col in column_analysis['text_columns']:
            if any(term in col.lower() for term in ['name', 'title', 'description']):
                display.append(col)
        
        # Add primary key
        if column_analysis['primary_key']:
            display.append(column_analysis['primary_key'])
        
        # Add important numeric fields
        for col in column_analysis['numeric_columns']:
            if any(term in col.lower() for term in ['total', 'amount', 'price', 'quantity']):
                display.append(col)
        
        return display[:5]  # Limit to 5 most important fields
    
    def _create_entity_graph(
        self, 
        table_analyses: Dict[str, Any], 
        relationships: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Create a graph representation of entities and their relationships
        """
        nodes = []
        edges = []
        
        # Create nodes for each table
        for table_name, analysis in table_analyses.items():
            nodes.append({
                'id': table_name,
                'label': table_name,
                'type': analysis['entity_type'],
                'size': min(100, max(10, analysis['row_count'] // 1000))  # Size based on row count
            })
        
        # Create edges for relationships
        for rel in relationships:
            edges.append({
                'from': rel['from_table'],
                'to': rel['to_table'],
                'label': rel['from_column'],
                'type': rel['type']
            })
        
        return {
            'nodes': nodes,
            'edges': edges
        }
    
    async def _store_learned_patterns(self, schema_name: str, analysis: Dict[str, Any]) -> None:
        """
        Store learned patterns in database for future use
        """
        try:
            # Store in managed_schemas table
            query = """
                INSERT INTO atabot.managed_schemas 
                (schema_name, display_name, metadata, total_tables, total_rows, 
                 learned_patterns, business_domain, last_synced_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
                ON CONFLICT (schema_name) 
                DO UPDATE SET
                    metadata = EXCLUDED.metadata,
                    total_tables = EXCLUDED.total_tables,
                    total_rows = EXCLUDED.total_rows,
                    learned_patterns = EXCLUDED.learned_patterns,
                    business_domain = EXCLUDED.business_domain,
                    last_synced_at = NOW()
            """
            
            await db_pool.execute(
                query,
                schema_name,
                schema_name.replace('_', ' ').title(),
                json.dumps(analysis['table_analyses']),
                analysis['total_tables'],
                analysis['total_rows'],
                json.dumps({
                    'terminology': analysis['terminology'],
                    'relationships': analysis['relationships'],
                    'entity_graph': analysis['entity_graph']
                }),
                analysis['business_domain']
            )
            
            logger.info(f"Stored learned patterns for schema: {schema_name}")
            
        except Exception as e:
            logger.error(f"Failed to store learned patterns: {e}")

# Global schema analyzer instance
schema_analyzer = SchemaAnalyzer()