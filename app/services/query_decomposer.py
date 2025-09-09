"""
Query Decomposer Service - Breaks complex queries into simpler sub-queries
Uses AI to intelligently decompose multi-part questions
"""
from typing import Dict, List, Any, Optional, Tuple
from loguru import logger
import re
import json

from app.core.llm import llm_client
from app.core.config import settings


class QueryDecomposer:
    """
    Service for decomposing complex queries into atomic sub-queries
    """
    
    def __init__(self):
        self.decomposition_cache = {}
        self.pattern_library = self._init_pattern_library()
    
    def _init_pattern_library(self) -> Dict[str, Any]:
        """
        Initialize patterns for query decomposition
        These are linguistic patterns, not business-specific
        """
        return {
            'conjunctions': [
                'and', 'atau', 'dan', 'or', 'serta', 'also', 'juga',
                'as well as', 'along with', 'beserta'
            ],
            'comparisons': [
                'vs', 'versus', 'compared to', 'dibanding', 'berbanding',
                'difference between', 'perbedaan antara'
            ],
            'aggregations': [
                'total', 'sum', 'average', 'count', 'maximum', 'minimum',
                'jumlah', 'rata-rata', 'tertinggi', 'terendah'
            ],
            'time_indicators': [
                'yesterday', 'today', 'tomorrow', 'last week', 'this month',
                'kemarin', 'hari ini', 'besok', 'minggu lalu', 'bulan ini'
            ]
        }
    
    async def decompose(
        self,
        query: str,
        context: Dict[str, Any] = {},
        use_ai: bool = True
    ) -> List[str]:
        """
        Decompose a complex query into simpler sub-queries
        
        Args:
            query: The complex query to decompose
            context: Additional context about the query
            use_ai: Whether to use AI for decomposition
            
        Returns:
            List of simpler sub-queries
        """
        logger.info(f"Decomposing query: {query}")
        
        # Check cache first
        cache_key = f"{query}:{json.dumps(context, sort_keys=True)}"
        if cache_key in self.decomposition_cache:
            return self.decomposition_cache[cache_key]
        
        # Analyze query complexity
        complexity = self._analyze_complexity(query)
        
        if complexity['score'] < 2:
            # Simple query, no decomposition needed
            result = [query]
        elif use_ai and settings.ENABLE_QUERY_DECOMPOSITION:
            # Use AI for intelligent decomposition
            result = await self._ai_decompose(query, context, complexity)
        else:
            # Fallback to rule-based decomposition
            result = self._rule_based_decompose(query, complexity)
        
        # Cache result
        self.decomposition_cache[cache_key] = result
        
        return result
    
    def _analyze_complexity(self, query: str) -> Dict[str, Any]:
        """
        Analyze query complexity to determine decomposition strategy
        """
        query_lower = query.lower()
        
        complexity = {
            'score': 0,
            'has_conjunction': False,
            'has_comparison': False,
            'has_aggregation': False,
            'has_multiple_entities': False,
            'entity_count': 0,
            'clause_count': 1
        }
        
        # Check for conjunctions
        for conj in self.pattern_library['conjunctions']:
            if f' {conj} ' in f' {query_lower} ':
                complexity['has_conjunction'] = True
                complexity['score'] += 1
                break
        
        # Check for comparisons
        for comp in self.pattern_library['comparisons']:
            if comp in query_lower:
                complexity['has_comparison'] = True
                complexity['score'] += 2
                break
        
        # Check for aggregations
        for agg in self.pattern_library['aggregations']:
            if agg in query_lower:
                complexity['has_aggregation'] = True
                complexity['score'] += 1
        
        # Count potential entities (capitalized words, quoted strings)
        entities = re.findall(r'\b[A-Z][a-z]+\b|"[^"]+"|\'[^\']+\'', query)
        complexity['entity_count'] = len(entities)
        if len(entities) > 2:
            complexity['has_multiple_entities'] = True
            complexity['score'] += 1
        
        # Count clauses (based on punctuation and conjunctions)
        clause_markers = len(re.findall(r'[,;]|\s(and|dan|or|atau)\s', query_lower))
        complexity['clause_count'] = clause_markers + 1
        complexity['score'] += clause_markers
        
        return complexity
    
    async def _ai_decompose(
        self,
        query: str,
        context: Dict[str, Any],
        complexity: Dict[str, Any]
    ) -> List[str]:
        """
        Use AI to intelligently decompose the query
        """
        try:
            # Build context-aware prompt
            system_prompt = """You are an expert at breaking down complex questions into simple, atomic sub-questions.
Each sub-question should:
1. Ask for exactly ONE piece of information
2. Be answerable independently
3. Preserve the original language and terminology
4. Together, fully answer the original question"""
            
            # Add context hints
            context_hints = []
            if context.get('schema'):
                context_hints.append(f"Database schema: {context['schema']}")
            if context.get('entity_type'):
                context_hints.append(f"Entity type: {context['entity_type']}")
            if complexity['has_comparison']:
                context_hints.append("This appears to be a comparison query")
            if complexity['has_aggregation']:
                context_hints.append("This query involves aggregation")
            
            prompt = f"""Decompose this complex query into simple sub-queries:

Query: "{query}"

{chr(10).join(context_hints) if context_hints else ''}

Rules:
1. Each sub-query must be atomic (single question)
2. Preserve exact names, terms, and language
3. If comparing items, create separate queries for each
4. If aggregating, separate the retrieval from aggregation

Output format: Return ONLY a JSON array of strings
Example: ["question 1", "question 2", "question 3"]"""
            
            # Get AI response
            response = await llm_client.generate(
                prompt=prompt,
                system_prompt=system_prompt,
                temperature=0.3,
                max_tokens=500
            )
            
            # Parse response
            sub_queries = self._parse_ai_response(response, query)
            
            if sub_queries and len(sub_queries) > 1:
                logger.info(f"AI decomposed query into {len(sub_queries)} sub-queries")
                return sub_queries
            
        except Exception as e:
            logger.warning(f"AI decomposition failed: {e}, falling back to rules")
        
        # Fallback to rule-based
        return self._rule_based_decompose(query, complexity)
    
    def _parse_ai_response(self, response: str, original_query: str) -> List[str]:
        """
        Parse AI response to extract sub-queries
        """
        try:
            # Look for JSON array in response
            json_match = re.search(r'\[.*?\]', response, re.DOTALL)
            if json_match:
                sub_queries = json.loads(json_match.group(0))
                
                # Validate sub-queries
                if isinstance(sub_queries, list) and all(isinstance(q, str) for q in sub_queries):
                    # Filter out empty or invalid queries
                    valid_queries = [q.strip() for q in sub_queries if q and len(q.strip()) > 5]
                    
                    if valid_queries:
                        return valid_queries
        except:
            pass
        
        # If parsing fails, return original
        return [original_query]
    
    def _rule_based_decompose(self, query: str, complexity: Dict[str, Any]) -> List[str]:
        """
        Rule-based query decomposition for fallback
        """
        sub_queries = []
        query_lower = query.lower()
        
        # Handle comparisons
        if complexity['has_comparison']:
            # Split on comparison words
            for comp in self.pattern_library['comparisons']:
                if comp in query_lower:
                    parts = query.split(comp)
                    if len(parts) == 2:
                        # Create separate queries for each part
                        base_question = self._extract_base_question(query)
                        for part in parts:
                            sub_queries.append(f"{base_question} {part.strip()}?")
                    break
        
        # Handle conjunctions
        elif complexity['has_conjunction']:
            # Split on major conjunctions
            split_pattern = r'\s+(and|dan|or|atau|serta)\s+'
            parts = re.split(split_pattern, query, flags=re.IGNORECASE)
            
            # Group parts intelligently
            current_query = ""
            for i, part in enumerate(parts):
                if part.lower() not in self.pattern_library['conjunctions']:
                    if current_query:
                        current_query += f" {part}"
                    else:
                        current_query = part
                    
                    # Check if this completes a question
                    if '?' in part or i == len(parts) - 1:
                        sub_queries.append(current_query.strip())
                        current_query = ""
        
        # Handle multiple entities
        elif complexity['has_multiple_entities'] and complexity['entity_count'] > 2:
            # Extract entities and create individual queries
            entities = re.findall(r'\b[A-Z][a-z]+\b|"[^"]+"|\'[^\']+\'', query)
            base_pattern = re.sub(r'\b[A-Z][a-z]+\b|"[^"]+"|\'[^\']+\'', '{}', query)
            
            for entity in entities:
                sub_queries.append(base_pattern.format(entity))
        
        # Default: return as-is
        if not sub_queries:
            sub_queries = [query]
        
        # Clean up sub-queries
        cleaned = []
        for sq in sub_queries:
            sq = sq.strip()
            # Ensure question mark
            if sq and not sq.endswith('?'):
                sq += '?'
            if len(sq) > 5:  # Minimum meaningful length
                cleaned.append(sq)
        
        return cleaned if cleaned else [query]
    
    def _extract_base_question(self, query: str) -> str:
        """
        Extract the base question pattern from a query
        """
        # Remove specific entities to find pattern
        pattern = re.sub(r'\b[A-Z][a-z]+\b|"[^"]+"|\'[^\']+\'|\d+', '', query)
        
        # Extract question words
        question_words = ['what', 'which', 'how', 'when', 'where', 'who', 
                         'apa', 'mana', 'bagaimana', 'kapan', 'dimana', 'siapa']
        
        for word in question_words:
            if word in pattern.lower():
                # Found question word, extract that part
                start = pattern.lower().index(word)
                return pattern[start:].split()[0].capitalize()
        
        return "What is"
    
    async def recompose_answers(
        self,
        sub_queries: List[str],
        sub_answers: List[str],
        original_query: str
    ) -> str:
        """
        Recompose sub-answers into a coherent response to the original query
        """
        if len(sub_answers) == 1:
            return sub_answers[0]
        
        # Use AI to combine answers intelligently
        try:
            prompt = f"""Combine these answers into a coherent response to the original question.

Original question: "{original_query}"

Sub-questions and answers:
{chr(10).join([f"Q: {q}{chr(10)}A: {a}" for q, a in zip(sub_queries, sub_answers)])}

Create a natural, unified response that fully answers the original question.
If the question was a comparison, clearly present both sides.
If the question asked for multiple items, list them clearly."""
            
            combined = await llm_client.generate(
                prompt=prompt,
                temperature=0.3,
                max_tokens=1000
            )
            
            return combined
            
        except Exception as e:
            logger.error(f"Failed to recompose answers: {e}")
            
            # Fallback: simple concatenation
            return "\n\n".join([
                f"Regarding '{q}':\n{a}"
                for q, a in zip(sub_queries, sub_answers)
            ])
    
    def analyze_query_intent(self, query: str) -> Dict[str, Any]:
        """
        Analyze the intent and type of the query
        """
        query_lower = query.lower()
        
        intent = {
            'type': 'unknown',
            'requires_aggregation': False,
            'requires_comparison': False,
            'requires_filtering': False,
            'requires_joining': False,
            'time_sensitive': False
        }
        
        # Detect query type
        if any(word in query_lower for word in ['total', 'sum', 'count', 'average', 'jumlah', 'rata-rata']):
            intent['type'] = 'aggregation'
            intent['requires_aggregation'] = True
        
        elif any(word in query_lower for word in ['compare', 'versus', 'vs', 'bandingkan', 'perbedaan']):
            intent['type'] = 'comparison'
            intent['requires_comparison'] = True
        
        elif any(word in query_lower for word in ['list', 'show', 'display', 'tampilkan', 'daftar']):
            intent['type'] = 'listing'
        
        elif any(word in query_lower for word in ['find', 'search', 'locate', 'cari', 'temukan']):
            intent['type'] = 'search'
        
        elif '?' in query:
            intent['type'] = 'question'
        
        # Check for filtering needs
        if any(word in query_lower for word in ['where', 'with', 'having', 'yang', 'dengan']):
            intent['requires_filtering'] = True
        
        # Check for time sensitivity
        if any(word in query_lower for word in self.pattern_library['time_indicators']):
            intent['time_sensitive'] = True
        
        # Check for potential joins (multiple entity references)
        entities = re.findall(r'\b[A-Z][a-z]+\b', query)
        if len(set(entities)) > 1:
            intent['requires_joining'] = True
        
        return intent


# Global query decomposer instance
query_decomposer = QueryDecomposer()