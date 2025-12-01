import asyncio
import asyncpg
import os
from datetime import datetime

async def check_db():
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.getenv("POSTGRES_DB", "upgrade_upstox")

    dsn = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    
    try:
        conn = await asyncpg.connect(dsn)
        
        print(f"Connected to {database}")
        
        # Check count
        count = await conn.fetchval("SELECT COUNT(*) FROM market_patterns")
        print(f"Total rows in market_patterns: {count}")
        
        # Fetch last 5 rows
        rows = await conn.fetch("SELECT * FROM market_patterns ORDER BY timestamp DESC LIMIT 5")
        
        print("\nLast 5 entries:")
        for row in rows:
            print(f"ID: {row['id']} | Time: {row['timestamp']} | Key: {row['instrument_key']} | Pattern: {row['pattern']} ({row['signal']})")
            
        await conn.close()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(check_db())
