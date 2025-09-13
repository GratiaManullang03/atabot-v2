"""
Embedding Queue Service
"""
import asyncio
from typing import List, Dict, Optional
from collections import deque
from datetime import datetime
from loguru import logger
import hashlib

from app.core.embeddings import embedding_service

class EmbeddingQueue:
    """
    Queue system for efficient embedding generation with proper batch tracking
    """
    
    def __init__(self):
        self.queue = deque()
        self.processing = False
        self.cache = {}  # Text hash -> embedding
        self.batch_status = {}  # Batch ID -> status ('pending', 'processing', 'completed', 'failed')
        self.batch_results = {}  # Batch ID -> results
        
    async def add_batch(self, texts: List[str], metadata: List[Dict]) -> str:
        """Add texts to queue and return batch ID"""
        batch_id = hashlib.md5(f"{datetime.now().isoformat()}_{len(texts)}".encode()).hexdigest()
        
        # Prepare batch data
        text_hashes = []
        unique_texts = []
        unique_metadata = []
        unique_hashes = []
        
        for text, meta in zip(texts, metadata):
            text_hash = hashlib.md5(text.encode()).hexdigest()
            text_hashes.append(text_hash)
            
            # Check if already in cache
            if text_hash not in self.cache:
                unique_texts.append(text)
                unique_metadata.append(meta)
                unique_hashes.append(text_hash)
        
        # Track batch status
        self.batch_status[batch_id] = 'pending'
        self.batch_results[batch_id] = {
            'text_hashes': text_hashes,
            'total': len(texts),
            'to_process': len(unique_texts),
            'cached': len(texts) - len(unique_texts)
        }
        
        if unique_texts:
            # Add to queue
            self.queue.append({
                'batch_id': batch_id,
                'texts': unique_texts,
                'metadata': unique_metadata,
                'text_hashes': unique_hashes,
                'added_at': datetime.now()
            })
            
            logger.info(f"Batch {batch_id}: {len(unique_texts)} new texts to process, {len(texts) - len(unique_texts)} already cached")
            
            # Start processing if not already running
            if not self.processing:
                asyncio.create_task(self._process_queue())
        else:
            # All texts already cached
            self.batch_status[batch_id] = 'completed'
            logger.info(f"Batch {batch_id}: All {len(texts)} texts already cached")
        
        return batch_id
    
    async def wait_for_batch(self, batch_id: str, timeout: int = 300) -> bool:
        """Wait for a specific batch to complete"""
        start_time = datetime.now()
        
        while (datetime.now() - start_time).seconds < timeout:
            status = self.batch_status.get(batch_id, 'unknown')
            
            if status == 'completed':
                logger.info(f"Batch {batch_id} completed successfully")
                return True
            elif status == 'failed':
                logger.error(f"Batch {batch_id} failed")
                return False
            elif status == 'unknown':
                logger.warning(f"Unknown batch ID: {batch_id}")
                return False
            
            # Still pending or processing
            await asyncio.sleep(1)
        
        logger.error(f"Batch {batch_id} timed out after {timeout} seconds")
        return False
    
    def get_embeddings_for_batch(self, batch_id: str, texts: List[str]) -> List[Optional[List[float]]]:
        """Get embeddings for a batch of texts"""
        embeddings = []
        
        for text in texts:
            text_hash = hashlib.md5(text.encode()).hexdigest()
            embedding = self.cache.get(text_hash)
            embeddings.append(embedding)  # Can be None if not found
        
        return embeddings
    
    async def _process_queue(self):
        """Process queue with rate limiting and proper error handling"""
        if self.processing:
            logger.warning("Queue processing already in progress")
            return
            
        self.processing = True
        logger.info("Starting queue processing")
        
        try:
            while self.queue:
                # Collect up to 120 texts (max per VoyageAI request)
                batch_texts = []
                batch_metadata = []
                batch_ids = []
                batch_hashes = []
                
                # Collect texts from multiple batches if needed
                while self.queue and len(batch_texts) < 120:
                    batch = self.queue[0]
                    
                    # Calculate how many texts we can take from this batch
                    can_take = min(120 - len(batch_texts), len(batch['texts']))
                    
                    if can_take == len(batch['texts']):
                        # Take entire batch
                        batch = self.queue.popleft()
                        batch_texts.extend(batch['texts'])
                        batch_metadata.extend(batch['metadata'])
                        batch_hashes.extend(batch['text_hashes'])
                        batch_ids.append(batch['batch_id'])
                        
                        # Update status to processing
                        self.batch_status[batch['batch_id']] = 'processing'
                    else:
                        # Take partial batch
                        batch_texts.extend(batch['texts'][:can_take])
                        batch_metadata.extend(batch['metadata'][:can_take])
                        batch_hashes.extend(batch['text_hashes'][:can_take])
                        
                        # Update queue with remaining
                        batch['texts'] = batch['texts'][can_take:]
                        batch['metadata'] = batch['metadata'][can_take:]
                        batch['text_hashes'] = batch['text_hashes'][can_take:]
                        
                        # Mark batch as partially processing
                        if batch['batch_id'] not in batch_ids:
                            self.batch_status[batch['batch_id']] = 'processing'
                            batch_ids.append(batch['batch_id'])
                        break
                
                if batch_texts:
                    logger.info(f"Processing {len(batch_texts)} texts in batch")
                    
                    try:
                        # Generate embeddings with retry logic
                        embeddings = await self._generate_embeddings_with_retry(batch_texts)
                        
                        # Store embeddings in cache
                        successful = 0
                        failed = 0
                        
                        for text_hash, embedding in zip(batch_hashes, embeddings):
                            if embedding and len(embedding) > 0:
                                # Validate embedding
                                non_zero_count = sum(1 for v in embedding if v != 0)
                                if non_zero_count > len(embedding) * 0.1:
                                    self.cache[text_hash] = embedding
                                    successful += 1
                                else:
                                    logger.warning(f"Invalid embedding (mostly zeros) for hash {text_hash}")
                                    failed += 1
                            else:
                                logger.warning(f"No embedding generated for hash {text_hash}")
                                failed += 1
                        
                        logger.info(f"Processed batch: {successful} successful, {failed} failed")
                        
                        # Update batch statuses
                        for batch_id in batch_ids:
                            # Check if all texts for this batch are processed
                            if self._is_batch_complete(batch_id):
                                self.batch_status[batch_id] = 'completed'
                                logger.info(f"Batch {batch_id} completed")
                        
                    except Exception as e:
                        logger.error(f"Embedding generation failed: {e}")
                        
                        # Mark batches as failed
                        for batch_id in batch_ids:
                            self.batch_status[batch_id] = 'failed'
                        
                        # If rate limit error, wait before continuing
                        if "rate limit" in str(e).lower():
                            logger.warning("Rate limit hit, waiting 60 seconds...")
                            await asyncio.sleep(60)
                        else:
                            # For other errors, wait shorter
                            await asyncio.sleep(5)
                    
                    # Always wait between batches for rate limiting (free tier: 3 RPM)
                    # Wait 21 seconds to ensure we don't exceed 3 requests per minute
                    if self.queue:  # Only wait if there are more items to process
                        logger.info("Waiting 21 seconds for rate limit...")
                        await asyncio.sleep(21)
                    
        finally:
            self.processing = False
            logger.info("Queue processing completed")
    
    async def _generate_embeddings_with_retry(
        self, 
        texts: List[str], 
        max_retries: int = 3
    ) -> List[Optional[List[float]]]:
        """Generate embeddings with retry logic"""
        for attempt in range(max_retries):
            try:
                # Use the embedding service to generate embeddings
                embeddings = await embedding_service.generate_batch_embeddings(
                    texts,
                    input_type="document",
                    show_progress=True
                )
                
                # Check if we got valid embeddings
                valid_count = sum(1 for e in embeddings if e is not None)
                logger.info(f"Generated {valid_count}/{len(texts)} valid embeddings")
                
                return embeddings
                
            except Exception as e:
                logger.error(f"Attempt {attempt + 1}/{max_retries} failed: {e}")
                
                if attempt < max_retries - 1:
                    wait_time = min(30 * (2 ** attempt), 300)  # Exponential backoff
                    logger.info(f"Retrying in {wait_time} seconds...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error("All retry attempts failed")
                    # Return None for all texts
                    return [None] * len(texts)
        
        return [None] * len(texts)
    
    def _is_batch_complete(self, batch_id: str) -> bool:
        """Check if all texts for a batch have been processed"""
        # Check if batch is still in queue
        for batch in self.queue:
            if batch['batch_id'] == batch_id:
                return False  # Still has pending texts
        
        # Check if all text hashes have embeddings
        batch_info = self.batch_results.get(batch_id, {})
        text_hashes = batch_info.get('text_hashes', [])
        
        for text_hash in text_hashes:
            if text_hash not in self.cache:
                # Check if this was originally cached
                if batch_info.get('cached', 0) > 0:
                    continue  # Originally cached, skip check
                return False  # Missing embedding
        
        return True
    
    def clear_cache(self):
        """Clear all caches"""
        self.cache.clear()
        self.batch_status.clear()
        self.batch_results.clear()
        logger.info("All caches cleared")
    
    def get_cache_stats(self) -> Dict:
        """Get cache statistics"""
        return {
            'cached_embeddings': len(self.cache),
            'pending_batches': len([b for b in self.batch_status.values() if b == 'pending']),
            'processing_batches': len([b for b in self.batch_status.values() if b == 'processing']),
            'completed_batches': len([b for b in self.batch_status.values() if b == 'completed']),
            'failed_batches': len([b for b in self.batch_status.values() if b == 'failed']),
            'queue_size': len(self.queue),
            'is_processing': self.processing
        }

# Global queue instance
embedding_queue = EmbeddingQueue()