"""
Redis Connection Module - Async Redis Client for Whale Hunter HFT
Manages connection pooling and provides helper methods for streams and sorted sets.
"""

import redis.asyncio as aioredis
from typing import Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)


class RedisClient:
    """Async Redis client with connection pooling."""
    
    def __init__(self, host: str = "localhost", port: int = 6379, db: int = 0):
        """
        Initialize Redis client.
        
        Args:
            host: Redis host address
            port: Redis port
            db: Redis database number
        """
        self.host = host
        self.port = port
        self.db = db
        self.client: Optional[aioredis.Redis] = None
    
    async def connect(self) -> None:
        """Establish async connection to Redis."""
        try:
            self.client = await aioredis.from_url(
                f"redis://{self.host}:{self.port}/{self.db}",
                encoding="utf-8",
                decode_responses=False  # We'll handle encoding per operation
            )
            await self.client.ping()
            logger.info(f"✓ Connected to Redis at {self.host}:{self.port}")
        except Exception as e:
            logger.error(f"✗ Failed to connect to Redis: {e}")
            raise
    
    async def disconnect(self) -> None:
        """Close Redis connection."""
        if self.client:
            await self.client.close()
            logger.info("✓ Redis connection closed")
    
    async def add_to_stream(
        self,
        stream_key: str,
        data: Dict[str, Any],
        maxlen: int = 6000
    ) -> str:
        """
        Add data to Redis Stream with MAXLEN constraint.
        
        Args:
            stream_key: Stream key (e.g., 'stream:ticks:NSE_INDEX|Nifty 50')
            data: Dictionary of field-value pairs
            maxlen: Maximum stream length (approx 10 mins at 10 ticks/sec)
        
        Returns:
            Stream entry ID
        """
        return await self.client.xadd(
            stream_key,
            data,
            maxlen=maxlen,
            approximate=True  # Faster, allows ~maxlen entries
        )
    
    async def update_zset(
        self,
        zset_key: str,
        member: str,
        score: float
    ) -> int:
        """
        Update sorted set (ZSET) member with score.
        
        Args:
            zset_key: ZSET key (e.g., 'day_wall:bids:NSE_FO|12345')
            member: Member identifier (e.g., 'price:181.95')
            score: Score value (e.g., quantity 2500)
        
        Returns:
            Number of elements added (0 if updated, 1 if new)
        """
        return await self.client.zadd(zset_key, {member: score})
    
    async def get_top_walls(
        self,
        zset_key: str,
        count: int = 10
    ) -> list:
        """
        Get top N walls from ZSET (highest quantities).
        
        Args:
            zset_key: ZSET key
            count: Number of top items to retrieve
        
        Returns:
            List of (member, score) tuples
        """
        return await self.client.zrevrange(
            zset_key,
            0,
            count - 1,
            withscores=True
        )
