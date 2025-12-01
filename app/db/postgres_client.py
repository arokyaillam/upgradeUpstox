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
            """,
            """
            CREATE TABLE IF NOT EXISTS panic_signals (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                instrument_key VARCHAR(50) NOT NULL,
                pattern VARCHAR(50) NOT NULL,
                signal VARCHAR(50) NOT NULL,
                ltp FLOAT,
                price_change_pct FLOAT,
                oi_change INTEGER,
                volume_change INTEGER,
                delta_change FLOAT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_panic_signals_timestamp ON panic_signals(timestamp);
            """,
            """
            CREATE TABLE IF NOT EXISTS order_imbalance (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                instrument_key VARCHAR(50) NOT NULL,
                tbq INTEGER,
                tsq INTEGER,
                imbalance_ratio FLOAT,
                signal VARCHAR(50) NOT NULL,
                ltp FLOAT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_order_imbalance_timestamp ON order_imbalance(timestamp);
            """,
            """
            CREATE TABLE IF NOT EXISTS greeks_momentum (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                instrument_key VARCHAR(50) NOT NULL,
                delta_velocity FLOAT,
                gamma_acceleration FLOAT,
                iv_velocity FLOAT,
                theta_acceleration FLOAT,
                momentum_score FLOAT,
                momentum_type VARCHAR(50),
                signal VARCHAR(50),
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_greeks_momentum_timestamp ON greeks_momentum(timestamp);
            """,
            """
            CREATE TABLE IF NOT EXISTS whale_alerts (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                instrument_key VARCHAR(50) NOT NULL,
                whale_type VARCHAR(50) NOT NULL,
                alert_type VARCHAR(50) NOT NULL,
                alert_value FLOAT,
                signal VARCHAR(50),
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_whale_alerts_timestamp ON whale_alerts(timestamp);
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

    async def insert_panic_signal(self, data: Dict[str, Any]):
        """Insert a panic signal."""
        if not self.pool:
            return

        query = """
            INSERT INTO panic_signals 
            (timestamp, instrument_key, pattern, signal, ltp, price_change_pct, oi_change, volume_change, delta_change)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        """
        
        async with self.pool.acquire() as conn:
            await conn.execute(
                query,
                data['timestamp'],
                data['instrument_key'],
                data['pattern'],
                data['signal'],
                data['metrics']['ltp'],
                data['metrics']['price_change_pct'],
                data['metrics']['oi_change'],
                data['metrics']['volume_change'],
                data['metrics']['delta_change']
            )

    async def insert_imbalance(self, data: Dict[str, Any]):
        """Insert an order book imbalance record."""
        if not self.pool:
            return

        query = """
            INSERT INTO order_imbalance 
            (timestamp, instrument_key, tbq, tsq, imbalance_ratio, signal, ltp)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
        """
        
        async with self.pool.acquire() as conn:
            await conn.execute(
                query,
                data['timestamp'],
                data['instrument_key'],
                data['tbq'],
                data['tsq'],
                data['imbalance_ratio'],
                data['signal'],
                data['ltp']
            )

    async def insert_greeks_momentum(self, data: Dict[str, Any]):
        """Insert a greeks momentum record."""
        if not self.pool:
            return

        query = """
            INSERT INTO greeks_momentum 
            (timestamp, instrument_key, delta_velocity, gamma_acceleration, iv_velocity, theta_acceleration, momentum_score, momentum_type, signal)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        """
        
        async with self.pool.acquire() as conn:
            await conn.execute(
                query,
                data['timestamp'],
                data['instrument_key'],
                data['delta_velocity'],
                data['gamma_acceleration'],
                data['iv_velocity'],
                data['theta_acceleration'],
                data['momentum_score'],
                data['momentum_type'],
                data['signal']
            )

    async def insert_whale_alert(self, data: Dict[str, Any]):
        """Insert a whale alert."""
        if not self.pool:
            return

        query = """
            INSERT INTO whale_alerts 
            (timestamp, instrument_key, whale_type, alert_type, alert_value, signal)
            VALUES ($1, $2, $3, $4, $5, $6)
        """
        
        async with self.pool.acquire() as conn:
            await conn.execute(
                query,
                data['timestamp'],
                data['instrument_key'],
                data['whale_type'],
                data['alert_type'],
                data['alert_value'],
                data['signal']
            )
