"""
Embedding Queue Service - Optimized for rate limits
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
    Queue system for efficient embedding generation
    """
    
    def __init__(self):
        self.queue = deque()
        self.processing = False
        self.cache = {}  # Dedupe cache
        
    async def add_batch(self, texts: List[str], metadata: List[Dict]) -> str:
        """Add texts to queue and return batch ID"""
        batch_id = hashlib.md5(str(datetime.now()).encode()).hexdigest()
        
        # Deduplicate texts
        unique_texts = []
        unique_metadata = []
        
        for text, meta in zip(texts, metadata):
            text_hash = hashlib.md5(text.encode()).hexdigest()
            if text_hash not in self.cache:
                self.cache[text_hash] = None  # Mark as pending
                unique_texts.append(text)
                unique_metadata.append(meta)
        
        if unique_texts:
            self.queue.append({
                'batch_id': batch_id,
                'texts': unique_texts,
                'metadata': unique_metadata,
                'added_at': datetime.now()
            })
            
            # Start processing if not already running
            if not self.processing:
                asyncio.create_task(self._process_queue())
        
        return batch_id
    
    async def _process_queue(self):
        """Process queue with rate limiting"""
        self.processing = True
        
        while self.queue:
            # Collect up to 120 texts (max per VoyageAI request)
            batch_texts = []
            batch_metadata = []
            batch_ids = []
            
            while self.queue and len(batch_texts) < 120:
                if len(self.queue[0]['texts']) + len(batch_texts) <= 120:
                    batch = self.queue.popleft()
                    batch_texts.extend(batch['texts'])
                    batch_metadata.extend(batch['metadata'])
                    batch_ids.append(batch['batch_id'])
                else:
                    # Partial batch processing
                    remaining = 120 - len(batch_texts)
                    batch = self.queue[0]
                    batch_texts.extend(batch['texts'][:remaining])
                    batch_metadata.extend(batch['metadata'][:remaining])
                    
                    # Update queue with remaining
                    batch['texts'] = batch['texts'][remaining:]
                    batch['metadata'] = batch['metadata'][remaining:]
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
                    
                    # Store results
                    for text, embedding in zip(batch_texts, embeddings):
                        text_hash = hashlib.md5(text.encode()).hexdigest()
                        self.cache[text_hash] = embedding
                    
                    logger.info(f"Generated {len(embeddings)} embeddings in 1 API call")
                    
                except Exception as e:
                    logger.error(f"Embedding generation failed: {e}")
                
                # Wait for rate limit (30 seconds between requests)
                await asyncio.sleep(31)
        
        self.processing = False

# Global queue instance
embedding_queue = EmbeddingQueue()