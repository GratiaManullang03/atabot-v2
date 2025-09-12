"""
LLM Service using Poe API
Lightweight API-based language model integration
"""
import httpx
from typing import Dict, Any, Optional, List
from loguru import logger
import json
from tenacity import retry, stop_after_attempt, wait_exponential

from app.core.config import settings

class PoeClient:
    """
    Client for Poe API (ChatGPT, Claude, etc.)
    """
    
    def __init__(self):
        self.api_key = settings.POE_API_KEY
        self.model = settings.LLM_MODEL
        self.base_url = "https://api.poe.com/v1"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        self.client = httpx.AsyncClient(timeout=settings.LLM_TIMEOUT)
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    async def generate(
        self,
        prompt: str,
        context: Optional[str] = None,
        max_tokens: int = None,
        temperature: float = None,
        system_prompt: Optional[str] = None
    ) -> str:
        """
        Generate text using LLM
        
        Args:
            prompt: User prompt
            context: Additional context for the prompt
            max_tokens: Maximum tokens to generate
            temperature: Sampling temperature
            system_prompt: System instruction
        
        Returns:
            Generated text
        """
        if not prompt:
            raise ValueError("Prompt cannot be empty")
        
        # Build messages
        messages = []
        
        if system_prompt:
            messages.append({
                "role": "system",
                "content": system_prompt
            })
        
        # Build full prompt with context
        full_prompt = prompt
        if context:
            full_prompt = f"""Context:\n{context}\n\nQuery:\n{prompt}"""
        
        messages.append({
            "role": "user",
            "content": full_prompt
        })
        
        # Prepare request
        request_data = {
            "model": self.model,
            "messages": messages,
            "max_tokens": max_tokens or settings.LLM_MAX_TOKENS,
            "temperature": temperature or settings.LLM_TEMPERATURE
        }

        # Track metrics
        from app.core.metrics import usage_tracker
        usage_tracker.log_api_call('voyage')
        
        try:
            response = await self.client.post(
                f"{self.base_url}/chat/completions",
                headers=self.headers,
                json=request_data
            )
            
            response.raise_for_status()
            
            result = response.json()
            return result["choices"][0]["message"]["content"]
            
        except httpx.HTTPStatusError as e:
            logger.error(f"LLM API error: {e.response.status_code} - {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"LLM generation failed: {e}")
            raise
    
    async def generate_sql(
        self,
        query: str,
        schema_info: Dict[str, Any],
        examples: Optional[List[Dict[str, str]]] = None
    ) -> str:
        """
        Generate SQL query from natural language
        
        Args:
            query: Natural language query
            schema_info: Database schema information
            examples: Optional few-shot examples
        
        Returns:
            SQL query string
        """
        system_prompt = """You are an expert SQL query generator.
Generate PostgreSQL queries based on natural language requests.
Return ONLY the SQL query without any explanation or markdown formatting."""
        
        # Build context with schema info
        context_parts = ["Database Schema:"]
        for table_name, table_info in schema_info.items():
            context_parts.append(f"\nTable: {table_name}")
            if "columns" in table_info:
                for col in table_info["columns"][:10]:  # Limit columns
                    context_parts.append(f"  - {col['name']} ({col['type']})")
        
        # Add examples if provided
        if examples:
            context_parts.append("\nExamples:")
            for example in examples[:3]:  # Limit examples
                context_parts.append(f"Q: {example['question']}")
                context_parts.append(f"SQL: {example['sql']}")
        
        context = "\n".join(context_parts)
        
        sql = await self.generate(
            prompt=f"Generate SQL for: {query}",
            context=context,
            system_prompt=system_prompt,
            temperature=0.1  # Low temperature for deterministic SQL
        )
        
        # Clean up the SQL
        sql = sql.strip()
        if sql.startswith("```"):
            # Remove markdown code blocks if present
            sql = sql.split("```")[1]
            if sql.startswith("sql"):
                sql = sql[3:]
        
        return sql.strip()
    
    async def decompose_query(
        self,
        query: str,
        context: Optional[Dict[str, Any]] = None
    ) -> List[str]:
        """
        Decompose complex query into sub-queries
        
        Args:
            query: Complex natural language query
            context: Optional context about the data
        
        Returns:
            List of simpler sub-queries
        """
        system_prompt = """You are an expert at query decomposition.
Break down complex queries into simple, atomic sub-queries.
Each sub-query should ask for ONE piece of information.
Return the result as a JSON array of strings."""
        
        prompt = f"""Decompose this complex query into simple sub-queries:
        
Query: "{query}"

Rules:
1. Each sub-query must be answerable independently
2. Preserve the original language and terminology
3. Keep the same level of detail
4. Output format: ["query1", "query2", ...]"""
        
        response = await self.generate(
            prompt=prompt,
            system_prompt=system_prompt,
            temperature=0.3
        )
        
        try:
            # Parse JSON array from response
            import re
            json_match = re.search(r'\[.*\]', response, re.DOTALL)
            if json_match:
                sub_queries = json.loads(json_match.group(0))
                return sub_queries if isinstance(sub_queries, list) else [query]
        except:
            logger.warning(f"Failed to parse decomposed queries, returning original")
        
        return [query]
    
    async def generate_answer(
        self,
        query: str,
        context: str,
        sources: Optional[List[Dict[str, Any]]] = None,
        language: str = "auto"
    ) -> str:
        """
        Generate natural language answer from data
        
        Args:
            query: User's question
            context: Relevant data/documents
            sources: Source information for citations
            language: Target language for response
        
        Returns:
            Natural language answer
        """
        system_prompt = f"""You are ATABOT, an intelligent business assistant.
Answer questions based ONLY on the provided context.
Be accurate, concise, and helpful.
If the context doesn't contain the answer, say so clearly.
{"Respond in the same language as the query." if language == "auto" else f"Respond in {language}."}"""
        
        # Build prompt with sources
        prompt_parts = [f"Question: {query}", "", "Available Data:"]
        prompt_parts.append(context)
        
        if sources:
            prompt_parts.append("\nSources:")
            for i, source in enumerate(sources[:5], 1):
                prompt_parts.append(f"{i}. {source.get('table', 'Unknown')}: {source.get('summary', '')}")
        
        prompt_parts.append("\nProvide a clear, accurate answer based on the data above:")
        
        answer = await self.generate(
            prompt="\n".join(prompt_parts),
            system_prompt=system_prompt,
            temperature=0.3
        )
        
        return answer
    
    async def extract_entities(
        self,
        text: str,
        entity_types: Optional[List[str]] = None
    ) -> Dict[str, List[str]]:
        """
        Extract entities from text
        
        Args:
            text: Text to analyze
            entity_types: Types of entities to extract
        
        Returns:
            Dictionary of entity type -> list of entities
        """
        system_prompt = """You are an entity extraction expert.
Extract entities from text and categorize them.
Return the result as JSON object with entity types as keys."""
        
        types_str = ""
        if entity_types:
            types_str = f"\nFocus on these entity types: {', '.join(entity_types)}"
        
        prompt = f"""Extract entities from this text:

Text: "{text}"
{types_str}

Output format:
{{
  "persons": ["name1", "name2"],
  "locations": ["place1", "place2"],
  "dates": ["date1", "date2"],
  "numbers": ["123", "456"],
  ...
}}"""
        
        response = await self.generate(
            prompt=prompt,
            system_prompt=system_prompt,
            temperature=0.1
        )
        
        try:
            # Parse JSON from response
            import re
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            if json_match:
                entities = json.loads(json_match.group(0))
                return entities if isinstance(entities, dict) else {}
        except:
            logger.warning("Failed to parse extracted entities")
        
        return {}
    
    async def close(self):
        """
        Close the HTTP client
        """
        await self.client.aclose()

# Global LLM client instance
llm_client = PoeClient()

async def get_llm_client() -> PoeClient:
    """
    Dependency to get LLM client
    """
    return llm_client