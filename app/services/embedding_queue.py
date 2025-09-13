"""
Embedding Queue Service
"""
import asyncio
from typing import List, Dict
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
        batch_id = hashlib.md5(str(datetime.now()).encode()).hexdigest()
        
        # Deduplicate texts
        unique_texts = []
        unique_metadata = []
        text_hashes = []
        
        for text, meta in zip(texts, metadata):
            text_hash = hashlib.md5(text.encode()).hexdigest()
            text_hashes.append(text_hash)
            
            # Check if already in cache
            if text_hash not in self.cache:
                unique_texts.append(text)
                unique_metadata.append(meta)
        
        # Track batch status
        self.batch_status[batch_id] = 'pending'
        self.batch_results[batch_id] = {
            'text_hashes': text_hashes,
            'total': len(texts),
            'to_process': len(unique_texts),
            'cached': len(texts) - len(unique_texts)
        }
        
        if unique_texts:
            self.queue.append({
                'batch_id': batch_id,
                'texts': unique_texts,
                'metadata': unique_metadata,
                'text_hashes': [hashlib.md5(t.encode()).hexdigest() for t in unique_texts],
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
    
    def get_embeddings_for_batch(self, batch_id: str, texts: List[str]) -> List[List[float]]:
        """Get embeddings for a batch of texts"""
        embeddings = []
        
        for text in texts:
            text_hash = hashlib.md5(text.encode()).hexdigest()
            embedding = self.cache.get(text_hash)
            
            if embedding is not None:
                embeddings.append(embedding)
            else:
                logger.warning(f"Embedding not found for text hash: {text_hash}")
                # Return None instead of zeros - let caller decide fallback
                embeddings.append(None)
        
        return embeddings
    
    async def _process_queue(self):
        """Process queue with rate limiting and proper error handling"""
        self.processing = True
        
        try:
            while self.queue:
                # Collect up to 120 texts (max per VoyageAI request)
                batch_texts = []
                batch_metadata = []
                batch_ids = []
                batch_hashes = []
                
                while self.queue and len(batch_texts) < 120:
                    if len(self.queue[0]['texts']) + len(batch_texts) <= 120:
                        batch = self.queue.popleft()
                        batch_texts.extend(batch['texts'])
                        batch_metadata.extend(batch['metadata'])
                        batch_hashes.extend(batch['text_hashes'])
                        batch_ids.append(batch['batch_id'])
                        
                        # Update status to processing
                        self.batch_status[batch['batch_id']] = 'processing'
                    else:
                        # Partial batch processing
                        remaining = 120 - len(batch_texts)
                        batch = self.queue[0]
                        batch_texts.extend(batch['texts'][:remaining])
                        batch_metadata.extend(batch['metadata'][:remaining])
                        batch_hashes.extend(batch['text_hashes'][:remaining])
                        
                        # Update queue with remaining
                        batch['texts'] = batch['texts'][remaining:]
                        batch['metadata'] = batch['metadata'][remaining:]
                        batch['text_hashes'] = batch['text_hashes'][remaining:]
                        
                        # Mark batch as partially processing
                        self.batch_status[batch['batch_id']] = 'processing'
                        break
                
                if batch_texts:
                    logger.info(f"Processing {len(batch_texts)} texts in ONE request")
                    
                    try:
                        # Generate embeddings (single API call!)
                        embeddings = await embedding_service.generate_batch_embeddings(
                            batch_texts,
                            input_type="document",
                            show_progress=False
                        )
                        
                        # Validate embeddings
                        if not embeddings:
                            raise ValueError(f"Failed to generate any embeddings")
                        
                        # Store results in cache
                        successful = 0
                        failed_texts = []
                        
                        for i, (text_hash, embedding) in enumerate(zip(batch_hashes, embeddings)):
                            if embedding is not None and len(embedding) > 0:
                                # Additional validation
                                non_zero_count = sum(1 for v in embedding if v != 0)
                                if non_zero_count > len(embedding) * 0.1:  # At least 10% non-zero
                                    self.cache[text_hash] = embedding
                                    successful += 1
                                else:
                                    logger.error(f"Invalid embedding (too many zeros) for hash {text_hash}")
                                    failed_texts.append((text_hash, batch_texts[i]))
                            else:
                                logger.error(f"No embedding generated for hash {text_hash}")
                                failed_texts.append((text_hash, batch_texts[i] if i < len(batch_texts) else "unknown"))
                        
                        # Retry failed embeddings individually with longer wait
                        if failed_texts and len(failed_texts) < 10:  # Only retry if few failures
                            logger.info(f"Retrying {len(failed_texts)} failed embeddings individually...")
                            for text_hash, text in failed_texts:
                                try:
                                    await asyncio.sleep(21)  # Wait between retries
                                    embedding = await embedding_service.generate_embedding(text, "document")
                                    if embedding and len(embedding) > 0:
                                        self.cache[text_hash] = embedding
                                        successful += 1
                                        logger.info(f"Retry successful for hash {text_hash}")
                                except Exception as e:
                                    logger.error(f"Retry failed for hash {text_hash}: {e}")
                        
                    except Exception as e:
                        logger.error(f"Embedding generation failed: {e}")
                        
                        # Mark batches as failed
                        for batch_id in batch_ids:
                            self.batch_status[batch_id] = 'failed'
                        
                        # If rate limit error, wait longer
                        if "rate limit" in str(e).lower():
                            logger.warning("Rate limit hit, waiting 60 seconds...")
                            await asyncio.sleep(60)
                        else:
                            # For other errors, wait shorter
                            await asyncio.sleep(5)
                    
                    # Always wait between batches for rate limiting (free tier: 3 RPM)
                    # Wait 21 seconds to ensure we don't exceed 3 requests per minute
                    await asyncio.sleep(21)
                    
        finally:
            self.processing = False
            logger.info("Queue processing completed")
    
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
            'queue_size': len(self.queue)
        }

# Global queue instance
embedding_queue = EmbeddingQueue()