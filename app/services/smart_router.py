"""
Smart Query Router - Minimize LLM usage
"""
from typing import Dict, Any, Optional
import re
from loguru import logger

class SmartRouter:
    """
    Route queries to appropriate handlers without LLM when possible
    """
    
    def __init__(self):
        # Pre-compiled patterns for common queries
        self.patterns = {
            'count': re.compile(r'\b(count|jumlah|berapa banyak|how many)\b', re.I),
            'sum': re.compile(r'\b(total|sum|jumlah total)\b', re.I),
            'list': re.compile(r'\b(list|show|tampilkan|tunjukkan)\s+all\b', re.I),
            'simple_where': re.compile(r'\b(where|dengan|yang)\s+(\w+)\s*(=|is)\s*(["\']?)([^"\']+)\4', re.I),
        }
        
        # Cache for SQL templates
        self.sql_cache = {}
    
    def analyze_query(self, query: str) -> Dict[str, Any]:
        """
        Analyze query without LLM
        """
        query_lower = query.lower()
        
        # Check for simple patterns first
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