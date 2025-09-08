"""
Data Synchronization Service
Handles bulk and real-time sync of data to vector store
"""
from typing import Dict, List, Any, Optional
from datetime import datetime, date
import asyncio
import asyncpg
import hashlib
import json
from loguru import logger