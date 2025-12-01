import asyncio
import asyncpg
import os

async def create_database():
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    target_db = os.getenv("POSTGRES_DB", "upgrade_upstox")

    # Connect to default 'postgres' database
    dsn = f"postgresql://{user}:{password}@{host}:{port}/postgres"
    
    try:
        conn = await asyncpg.connect(dsn)
        
        # Check if database exists
        exists = await conn.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", target_db)
        
        if not exists:
            print(f"Creating database {target_db}...")
            await conn.execute(f'CREATE DATABASE "{target_db}"')
            print(f"Database {target_db} created successfully.")
        else:
            print(f"Database {target_db} already exists.")
            
        await conn.close()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(create_database())
