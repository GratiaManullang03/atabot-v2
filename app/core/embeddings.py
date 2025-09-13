"""
VoyageAI Embedding Service with Better Error Handling
Handles rate limits and validation properly
"""
import voyageai
from typing import List, Dict, Any, Optional
from loguru import logger
import asyncio
from tenacity import retry, stop_after_attempt, wait_exponential
import hashlib
from datetime import datetime

from app.core.config import settings

class RateLimiter:
    """Simple rate limiter for API calls"""
    
    def __init__(self, max_requests: int = 3, window_seconds: int = 60):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.requests = []
    
    async def wait_if_needed(self):
        """Wait if rate limit would be exceeded"""
        now = datetime.now()
        
        # Clean old requests
        self.requests = [
            req_time for req_time in self.requests 
            if (now - req_time).total_seconds() < self.window_seconds
        ]
        
        # Check if we need to wait
        if len(self.requests) >= self.max_requests:
            # Calculate wait time
            oldest_request = min(self.requests)
            wait_time = self.window_seconds - (now - oldest_request).total_seconds()
            
            if wait_time > 0:
                logger.warning(f"Rate limit reached, waiting {wait_time:.1f} seconds...")
                await asyncio.sleep(wait_time + 1)  # Add 1 second buffer
                
                # Clean again after waiting
                now = datetime.now()
                self.requests = [
                    req_time for req_time in self.requests 
                    if (now - req_time).total_seconds() < self.window_seconds
                ]
        
        # Record this request
        self.requests.append(now)

class EmbeddingService:
    """VoyageAI API client for generating embeddings with rate limiting"""
    
    def __init__(self):
        """Initialize VoyageAI client"""
        self.client = voyageai.Client(api_key=settings.VOYAGE_API_KEY)
        self.model = settings.VOYAGE_MODEL
        self.dimensions = settings.EMBEDDING_DIMENSIONS
        self.batch_size = min(settings.EMBEDDING_BATCH_SIZE, 8)  # Reduce batch size for testing
                
        # Rate limiter: Conservative for free tier
        self.rate_limiter = RateLimiter(max_requests=2, window_seconds=60)
        
        # Simple in-memory cache for embeddings
        self._cache: Dict[str, List[float]] = {}
        self._cache_size = 0
        self._max_cache_size = 1000
        
    def _get_cache_key(self, text: str, input_type: str) -> str:
        """Generate cache key for text"""
        return hashlib.md5(f"{text}:{input_type}".encode()).hexdigest()
    
    def _validate_embedding(self, embedding: List[float]) -> bool:
        """Check if an embedding is valid (not all zeros)"""
        if not embedding or len(embedding) == 0:
            return False
        
        # Check if at least some values are non-zero
        non_zero_count = sum(1 for v in embedding if v != 0)
        
        # At least 10% should be non-zero for a valid embedding
        return non_zero_count > len(embedding) * 0.1
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=10, max=60)
    )
    async def generate_embedding(
        self, 
        text: str, 
        input_type: str = "document"
    ) -> Optional[List[float]]:
        """
        Generate embedding for a single text with rate limiting and validation
        
        Args:
            text: Text to embed
            input_type: 'document' for indexing, 'query' for searching
        
        Returns:
            List of floats representing the embedding vector, or None if failed
        """
        if not text or not text.strip():
            logger.error("Empty text provided for embedding")
            return None
        
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
            # Apply rate limiting
            await self.rate_limiter.wait_if_needed()
            
            logger.info(f"Generating embedding for text: {text[:100]}...")
            
            # VoyageAI's client is synchronous, so we run it in executor
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self.client.embed(
                    texts=[text],
                    model=self.model,
                    input_type=input_type,
                    truncation=True  # Enable truncation to avoid errors
                )
            )
            
            if not response or not response.embeddings or len(response.embeddings) == 0:
                logger.error("VoyageAI returned empty response")
                return None
            
            embedding = response.embeddings[0]
            
            # Validate embedding
            if not self._validate_embedding(embedding):
                logger.error(f"VoyageAI returned invalid embedding (all zeros or empty)")
                return None
            
            # Cache the result
            if settings.ENABLE_CACHE:
                self._add_to_cache(cache_key, embedding)
            
            logger.info(f"Successfully generated embedding with {len(embedding)} dimensions")
            return embedding
            
        except Exception as e:
            error_msg = str(e).lower()
            
            if "rate limit" in error_msg:
                logger.error(f"Rate limit hit: {e}")
                
                # Check if it's about payment method
                if "payment method" in error_msg:
                    logger.critical("VoyageAI requires payment method for higher rate limits!")
                    logger.info("Please add payment method at: https://dashboard.voyageai.com/")
                
                # Wait longer before retry
                await asyncio.sleep(30)
                raise  # Let retry decorator handle it
            
            elif "api key" in error_msg or "unauthorized" in error_msg:
                logger.critical(f"VoyageAI API key issue: {e}")
                logger.info("Please check your VOYAGE_API_KEY in .env file")
                return None
            
            else:
                logger.error(f"Failed to generate embedding: {e}")
                return None
    
    async def generate_batch_embeddings(
        self,
        texts: List[str],
        input_type: str = "document",
        show_progress: bool = True
    ) -> List[Optional[List[float]]]:
        """
        Generate embeddings for multiple texts with aggressive rate limiting and validation
        
        Args:
            texts: List of texts to embed
            input_type: 'document' for indexing, 'query' for searching
            show_progress: Whether to log progress
        
        Returns:
            List of embedding vectors (None for failed embeddings)
        """
        if not texts:
            return []
        
        # Filter out empty texts
        valid_indices_texts = [(i, t) for i, t in enumerate(texts) if t and t.strip()]
        
        if len(valid_indices_texts) != len(texts):
            logger.warning(f"Filtered out {len(texts) - len(valid_indices_texts)} empty texts")
        
        # Initialize results with None
        all_embeddings = [None] * len(texts)
        
        # Process in small batches due to rate limiting
        batch_size = min(self.batch_size, 8)  # Even smaller batches for free tier
        total_batches = (len(valid_indices_texts) + batch_size - 1) // batch_size
        
        logger.info(f"Processing {len(valid_indices_texts)} texts in {total_batches} batches (batch size: {batch_size})")
        
        successful = 0
        failed = 0
        
        for batch_num in range(total_batches):
            batch_start = batch_num * batch_size
            batch_end = min(batch_start + batch_size, len(valid_indices_texts))
            batch_indices_texts = valid_indices_texts[batch_start:batch_end]
            
            # Check cache first
            cached_count = 0
            for orig_idx, text in batch_indices_texts:
                cache_key = self._get_cache_key(text, input_type)
                if settings.ENABLE_CACHE and cache_key in self._cache:
                    all_embeddings[orig_idx] = self._cache[cache_key]
                    cached_count += 1
                    successful += 1
            
            # Get uncached texts
            uncached = [(idx, text) for idx, text in batch_indices_texts 
                       if all_embeddings[idx] is None]
            
            if uncached:
                try:
                    # Apply rate limiting
                    await self.rate_limiter.wait_if_needed()
                    
                    uncached_texts = [text[:8000] for _, text in uncached]  # Truncate
                    
                    logger.info(f"Batch {batch_num + 1}/{total_batches}: Generating {len(uncached_texts)} embeddings...")
                    
                    loop = asyncio.get_event_loop()
                    response = await loop.run_in_executor(
                        None,
                        lambda: self.client.embed(
                            texts=uncached_texts,
                            model=self.model,
                            input_type=input_type,
                            truncation=True
                        )
                    )
                    
                    if response and response.embeddings:
                        for (orig_idx, text), embedding in zip(uncached, response.embeddings):
                            if self._validate_embedding(embedding):
                                all_embeddings[orig_idx] = embedding
                                
                                # Cache valid embedding
                                if settings.ENABLE_CACHE:
                                    cache_key = self._get_cache_key(text, input_type)
                                    self._add_to_cache(cache_key, embedding)
                                
                                successful += 1
                            else:
                                logger.error(f"Invalid embedding for text {orig_idx}")
                                failed += 1
                    else:
                        logger.error("Empty response from VoyageAI")
                        failed += len(uncached)
                    
                except Exception as e:
                    logger.error(f"Batch {batch_num + 1} failed: {e}")
                    failed += len(uncached)
                    
                    if "rate limit" in str(e).lower():
                        logger.info("Waiting 60 seconds due to rate limit...")
                        await asyncio.sleep(60)
            
            if show_progress:
                progress = min(batch_end, len(valid_indices_texts))
                percent = (progress / len(valid_indices_texts)) * 100
                logger.info(f"Progress: {progress}/{len(valid_indices_texts)} ({percent:.1f}%) - Success: {successful}, Failed: {failed}, Cached: {cached_count}")
            
            # Wait between batches to respect rate limit
            if batch_num < total_batches - 1:
                wait_time = 21  # 60 seconds / 3 RPM = 20 seconds, +1 for safety
                logger.info(f"Waiting {wait_time} seconds before next batch (rate limit)...")
                await asyncio.sleep(wait_time)
        
        # Log final statistics
        logger.info(f"Embedding generation complete: {successful} successful, {failed} failed")
        
        # IMPORTANT: Don't return zero vectors for failed embeddings
        # Return None instead so caller can handle appropriately
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
        if not embedding1 or not embedding2:
            return 0.0
            
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