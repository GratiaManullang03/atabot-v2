"""
Smart Query Router - Minimize LLM usage
"""
from typing import Dict, Any, Optional
import re

class SmartRouter:
    """
    Route queries to appropriate handlers without LLM when possible
    """
    
    def __init__(self):
        # Enhanced patterns for common business queries
        self.patterns = {
            'stock_query': re.compile(r'\b(stok|stock|berapa.*stok)\s+([A-Z\s]+)', re.I),
            'program_query': re.compile(r'\b(program|ada di program)\s+(apa|mana)', re.I),
            'product_search': re.compile(r'\b(cari|search|find)\s+([A-Z\s]+)', re.I),
            'count': re.compile(r'\b(count|jumlah|berapa banyak|how many)\b', re.I),
            'sum': re.compile(r'\b(total|sum|jumlah total)\b', re.I),
            'list': re.compile(r'\b(list|show|tampilkan|tunjukkan)\s+all\b', re.I),
            'simple_where': re.compile(r'\b(where|dengan|yang)\s+(\w+)\s*(=|is)\s*(["\']?)([^"\']+)\4', re.I),
            'status_check': re.compile(r'\b(status|kondisi|keadaan)\s+(.+)', re.I),
            'availability': re.compile(r'\b(tersedia|available|ada)\s+(.+)', re.I)
        }
        
        # Cache for SQL templates
        self.sql_cache = {}
    
    def analyze_query(self, query: str) -> Dict[str, Any]:
        """
        Enhanced query analysis without LLM
        """
        query_lower = query.lower()

        # Business-specific patterns first
        if self.patterns['stock_query'].search(query):
            match = self.patterns['stock_query'].search(query)
            if match:
                product_name = match.group(2).strip()
                return {
                    'type': 'stock_query',
                    'product': product_name,
                    'needs_llm': False,
                    'use_search': True,  # Use search service instead of direct SQL
                    'search_terms': [product_name, 'stok', 'stock']
                }

        if self.patterns['product_search'].search(query):
            match = self.patterns['product_search'].search(query)
            if match:
                product_name = match.group(2).strip()
                return {
                    'type': 'product_search',
                    'product': product_name,
                    'needs_llm': False,
                    'use_search': True,
                    'search_terms': [product_name]
                }

        # Generic patterns
        if self.patterns['count'].search(query):
            return {
                'type': 'count',
                'needs_llm': False,
                'sql_template': 'SELECT COUNT(*) FROM {table} {where}'
            }

        if self.patterns['sum'].search(query):
            # Extract field to sum
            field_match = re.search(r'(total|sum)\s+(\w+)', query_lower)
            if field_match:
                return {
                    'type': 'sum',
                    'field': field_match.group(2),
                    'needs_llm': False,
                    'sql_template': f'SELECT SUM({field_match.group(2)}) FROM {{table}} {{where}}'
                }

        if self.patterns['list'].search(query):
            return {
                'type': 'list',
                'needs_llm': False,
                'sql_template': 'SELECT * FROM {table} LIMIT 100'
            }

        # Complex query - needs LLM
        return {
            'type': 'complex',
            'needs_llm': True
        }
    
    def generate_sql_without_llm(
        self, 
        query: str, 
        schema: str,
        table_hint: Optional[str] = None
    ) -> Optional[str]:
        """
        Generate SQL without LLM for simple queries
        """
        analysis = self.analyze_query(query)
        
        if not analysis['needs_llm'] and table_hint:
            template = analysis.get('sql_template')
            if template:
                # Build WHERE clause if needed
                where_match = self.patterns['simple_where'].search(query)
                where_clause = ""
                if where_match:
                    field = where_match.group(2)
                    value = where_match.group(5)
                    where_clause = f"WHERE {field} = '{value}'"
                
                sql = template.format(
                    table=f'"{schema}"."{table_hint}"',
                    where=where_clause
                )
                
                return sql
        
        return None

# Global router instance
smart_router = SmartRouter()