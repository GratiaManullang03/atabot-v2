"""
VoyageAI Embedding Service
Lightweight API-based embedding generation - NO local models!
"""
import voyageai
from typing import List, Optional, Dict, Any
from loguru import logger
import asyncio
from functools import lru_cache
from tenacity import retry, stop_after_attempt, wait_exponential
import hashlib

from .config import settings


class EmbeddingService:
    """VoyageAI API client for generating embeddings"""
    
    def __init__(self):
        """Initialize VoyageAI client"""
        self.client = voyageai.Client(api_key=settings.VOYAGE_API_KEY)
        self.model = settings.VOYAGE_MODEL
        self.dimensions = settings.EMBEDDING_DIMENSIONS
        self.batch_size = settings.EMBEDDING_BATCH_SIZE
        
        # Simple in-memory cache for embeddings
        self._cache: Dict[str, List[float]] = {}
        self._cache_size = 0
        self._max_cache_size = 1000
        
    def _get_cache_key(self, text: str, input_type: str) -> str:
        """Generate cache key for text"""
        return hashlib.md5(f"{text}:{input_type}".encode()).hexdigest()
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    async def generate_embedding(
        self, 
        text: str, 
        input_type: str = "document"
    ) -> List[float]:
        """
        Generate embedding for a single text
        
        Args:
            text: Text to embed
            input_type: 'document' for indexing, 'query' for searching
        
        Returns:
            List of floats representing the embedding vector
        """
        if not text or not text.strip():
            raise ValueError("Text cannot be empty")
        
        # Check cache
        cache_key = self._get_cache_key(text, input_type)
        if settings.ENABLE_CACHE and cache_key in self._cache:
            logger.debug(f"Embedding cache hit for text: {text[:50]}...")
            return self._cache[cache_key]
        
        # Truncate very long texts to avoid API limits
        max_length = 8000  # VoyageAI has character limits
        if len(text) > max_length:
            logger.warning(f"Text truncated from {len(text)} to {max_length} characters")
            text = text[:max_length]
        
        try:
            # VoyageAI's client is synchronous, so we run it in executor
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self.client.embed(
                    texts=[text],
                    model=self.model,
                    input_type=input_type
                )
            )
            
            embedding = response.embeddings[0]
            
            # Cache the result
            if settings.ENABLE_CACHE:
                self._add_to_cache(cache_key, embedding)
            
            return embedding
            
        except Exception as e:
            logger.error(f"Failed to generate embedding: {e}")
            raise
    
    async def generate_batch_embeddings(
        self,
        texts: List[str],
        input_type: str = "document",
        show_progress: bool = True
    ) -> List[List[float]]:
        """
        Generate embeddings for multiple texts in batches
        
        Args:
            texts: List of texts to embed
            input_type: 'document' for indexing, 'query' for searching
            show_progress: Whether to log progress
        
        Returns:
            List of embedding vectors
        """
        if not texts:
            return []
        
        # Filter out empty texts
        valid_texts = [(i, t) for i, t in enumerate(texts) if t and t.strip()]
        if len(valid_texts) != len(texts):
            logger.warning(f"Filtered out {len(texts) - len(valid_texts)} empty texts")
        
        all_embeddings = [None] * len(texts)
        
        # Process in batches
        for batch_start in range(0, len(valid_texts), self.batch_size):
            batch_end = min(batch_start + self.batch_size, len(valid_texts))
            batch_indices_texts = valid_texts[batch_start:batch_end]
            batch_texts = [text for _, text in batch_indices_texts]
            
            try:
                # Check cache first
                cached_embeddings = []
                uncached_indices = []
                uncached_texts = []
                
                for idx, (orig_idx, text) in enumerate(batch_indices_texts):
                    cache_key = self._get_cache_key(text, input_type)
                    if settings.ENABLE_CACHE and cache_key in self._cache:
                        cached_embeddings.append((orig_idx, self._cache[cache_key]))
                    else:
                        uncached_indices.append((idx, orig_idx))
                        uncached_texts.append(text[:8000])  # Truncate if needed
                
                # Generate embeddings for uncached texts
                if uncached_texts:
                    loop = asyncio.get_event_loop()
                    response = await loop.run_in_executor(
                        None,
                        lambda: self.client.embed(
                            texts=uncached_texts,
                            model=self.model,
                            input_type=input_type
                        )
                    )
                    
                    # Cache and store results
                    for (batch_idx, orig_idx), embedding in zip(uncached_indices, response.embeddings):
                        all_embeddings[orig_idx] = embedding
                        if settings.ENABLE_CACHE:
                            cache_key = self._get_cache_key(batch_indices_texts[batch_idx][1], input_type)
                            self._add_to_cache(cache_key, embedding)
                
                # Add cached embeddings
                for orig_idx, embedding in cached_embeddings:
                    all_embeddings[orig_idx] = embedding
                
                if show_progress:
                    progress = min(batch_end, len(valid_texts))
                    logger.info(f"Processed {progress}/{len(valid_texts)} embeddings")
                    
            except Exception as e:
                logger.error(f"Failed to generate batch embeddings: {e}")
                raise
        
        # Fill in empty embeddings with zeros (for empty texts)
        for i in range(len(all_embeddings)):
            if all_embeddings[i] is None:
                all_embeddings[i] = [0.0] * self.dimensions
        
        return all_embeddings
    
    def _add_to_cache(self, key: str, embedding: List[float]) -> None:
        """Add embedding to cache with size limit"""
        if self._cache_size >= self._max_cache_size:
            # Simple FIFO eviction
            first_key = next(iter(self._cache))
            del self._cache[first_key]
            self._cache_size -= 1
        
        self._cache[key] = embedding
        self._cache_size += 1
    
    def clear_cache(self) -> None:
        """Clear the embedding cache"""
        self._cache.clear()
        self._cache_size = 0
        logger.info("Embedding cache cleared")
    
    async def generate_hybrid_embeddings(
        self,
        data: Dict[str, Any],
        schema_context: Dict[str, Any]
    ) -> str:
        """
        Generate intelligent text representation from structured data
        This is the ADAPTIVE part - no hardcoded templates!
        
        Args:
            data: Row data from database
            schema_context: Learned context about the schema/table
        
        Returns:
            Natural language representation of the data
        """
        # Extract entity type from context
        entity_type = schema_context.get('entity_type', 'record')
        table_name = schema_context.get('table_name', 'data')
        learned_terms = schema_context.get('terminology', {})
        
        # Build text representation based on learned patterns
        text_parts = []
        
        # Add entity context
        if entity_type != 'unknown':
            text_parts.append(f"This is a {entity_type} record from {table_name}:")
        
        # Process each field intelligently
        for key, value in data.items():
            if value is None or key.startswith('_'):
                continue
            
            # Get learned term for this field
            field_label = learned_terms.get(key, key.replace('_', ' '))
            
            # Format value based on detected type
            if isinstance(value, (int, float)):
                if 'price' in key.lower() or 'cost' in key.lower() or 'amount' in key.lower():
                    # Detected as currency
                    formatted_value = f"{value:,.2f}"
                elif 'percent' in key.lower() or 'rate' in key.lower():
                    # Detected as percentage
                    formatted_value = f"{value}%"
                else:
                    formatted_value = str(value)
            elif isinstance(value, bool):
                formatted_value = "yes" if value else "no"
            else:
                formatted_value = str(value)
            
            text_parts.append(f"{field_label}: {formatted_value}")
        
        return ". ".join(text_parts)
    
    def calculate_similarity(self, embedding1: List[float], embedding2: List[float]) -> float:
        """
        Calculate cosine similarity between two embeddings
        
        Args:
            embedding1: First embedding vector
            embedding2: Second embedding vector
        
        Returns:
            Similarity score between 0 and 1
        """
        # Simple cosine similarity without numpy
        dot_product = sum(a * b for a, b in zip(embedding1, embedding2))
        norm1 = sum(a * a for a in embedding1) ** 0.5
        norm2 = sum(b * b for b in embedding2) ** 0.5
        
        if norm1 == 0 or norm2 == 0:
            return 0.0
        
        return dot_product / (norm1 * norm2)


# Global embedding service instance
embedding_service = EmbeddingService()


async def get_embedding_service() -> EmbeddingService:
    """Dependency to get embedding service"""
    return embedding_service