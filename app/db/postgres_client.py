import asyncpg
import logging
import os
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

class PostgresClient:
    def __init__(self):
        self.user = os.getenv("POSTGRES_USER", "postgres")
        self.password = os.getenv("POSTGRES_PASSWORD", "postgres")
        self.database = os.getenv("POSTGRES_DB", "upgrade_upstox")
        self.host = os.getenv("POSTGRES_HOST", "localhost")
        self.port = os.getenv("POSTGRES_PORT", "5432")
        self.pool = None

    async def connect(self):
        """Create a connection pool to PostgreSQL."""
        if not self.pool:
            try:
                dsn = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
                self.pool = await asyncpg.create_pool(dsn)
                logger.info("✅ Connected to PostgreSQL")
                await self.create_tables()
            except Exception as e:
                logger.error(f"❌ Failed to connect to PostgreSQL: {e}")
                raise

    async def disconnect(self):
        """Close the connection pool."""
        if self.pool:
            await self.pool.close()
            logger.info("🔌 Disconnected from PostgreSQL")

    async def create_tables(self):
        """Create necessary tables if they don't exist."""
        if not self.pool:
            return

        queries = [
            """
            CREATE TABLE IF NOT EXISTS market_patterns (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                instrument_key VARCHAR(50) NOT NULL,
                pattern VARCHAR(50) NOT NULL,
                signal VARCHAR(50) NOT NULL,
                price_change FLOAT,
                oi_change INTEGER,
                volume_change INTEGER,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_market_patterns_timestamp ON market_patterns(timestamp);
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_market_patterns_instrument ON market_patterns(instrument_key);
            """
        ]

        async with self.pool.acquire() as conn:
            for query in queries:
                await conn.execute(query)
        
        logger.info("✅ Tables verified/created")

    async def insert_pattern(self, data: Dict[str, Any]):
        """Insert a pattern detection result."""
        if not self.pool:
            return

        query = """
            INSERT INTO market_patterns 
            (timestamp, instrument_key, pattern, signal, price_change, oi_change, volume_change)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
        """
        
        async with self.pool.acquire() as conn:
            await conn.execute(
                query,
                data['timestamp'],
                data['instrument_key'],
                data['pattern'],
                data['signal'],
                data['metrics']['price_change'],
                data['metrics']['oi_change'],
                data['metrics']['volume_change']
            )
