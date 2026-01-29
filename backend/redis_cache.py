import redis.asyncio as redis
import json
import pickle
from typing import Optional, Any
import logging
import os

logger = logging.getLogger(__name__)

# Configuration from environment or defaults
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
CACHE_TTL = int(os.getenv("CACHE_TTL_SECONDS", "300"))

class RedisCache:
    def __init__(self):
        self.client = redis.from_url(REDIS_URL, decode_responses=True)
        self.binary_client = redis.from_url(REDIS_URL, decode_responses=False)
    
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        try:
            data = await self.client.get(key)
            if data:
                return json.loads(data)
            return None
        except Exception as e:
            logger.error(f"Redis get error: {e}")
            return None
    
    async def set(self, key: str, value: Any, ttl: int = None):
        """Set value in cache with TTL"""
        try:
            ttl = ttl or CACHE_TTL
            serialized = json.dumps(value, default=str)
            await self.client.setex(key, ttl, serialized)
        except Exception as e:
            logger.error(f"Redis set error: {e}")
    
    async def delete(self, key: str):
        """Delete key from cache"""
        try:
            await self.client.delete(key)
        except Exception as e:
            logger.error(f"Redis delete error: {e}")
    
    async def increment(self, key: str, amount: int = 1):
        """Atomic increment for counters"""
        try:
            return await self.client.incrby(key, amount)
        except Exception as e:
            logger.error(f"Redis increment error: {e}")
            return 0
    
    async def expire(self, key: str, seconds: int):
        """Set expiration on key"""
        try:
            await self.client.expire(key, seconds)
        except Exception as e:
            logger.error(f"Redis expire error: {e}")
    
    async def close(self):
        """Close connections"""
        try:
            await self.client.close()
            await self.binary_client.close()
        except Exception as e:
            logger.error(f"Redis close error: {e}")

# Global cache instance (import this in other files)
cache = RedisCache()