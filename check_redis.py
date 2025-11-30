import asyncio
import sys
import os
import json
from app.db.redis_client import RedisClient

# Fix path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), ".")))

async def check_redis():
    print("=" * 60)
    print("🔍 REDIS DATA INSPECTOR")
    print("=" * 60)
    
    redis = RedisClient()
    await redis.connect()
    
    try:
        # 1. Scan for stream keys
        print("\n1️⃣  Scanning for stream keys...")
        keys = await redis.client.keys("stream:*")
        
        if not keys:
            print("❌ No stream keys found! (Is ingestion running?)")
            return
            
        print(f"✓ Found {len(keys)} streams.")
        
        # 2. Inspect a few keys
        print("\n2️⃣  Inspecting latest data...")
        
        # Sort keys to get consistent output
        sorted_keys = sorted([k.decode('utf-8') for k in keys])
        
        # Inspect ALL keys
        for key in sorted_keys:
            label = "INDEX" if "NIFTY_50" in key else "OPTION"
            await inspect_stream(redis, key, label)
            
    finally:
        await redis.disconnect()

async def inspect_stream(redis, key, label):
    print(f"\n👉 {label} STREAM: {key}")
    
    # Get last entry
    data = await redis.client.xrevrange(key, count=1)
    
    if not data:
        print("   (Empty Stream)")
        return
        
    entry_id, entry_data = data[0]
    print(f"   ID: {entry_id.decode('utf-8')}")
    
    # Check for raw JSON
    if b'json' in entry_data:
        raw_json = entry_data[b'json'].decode('utf-8')
        parsed = json.loads(raw_json)
        print("   ✅ RAW JSON FOUND!")
        print("   Sample Data:")
        print(f"   - LTP: {parsed.get('marketFF', {}).get('ltpc', {}).get('ltp')}")
        print(f"   - LTT: {parsed.get('marketFF', {}).get('ltpc', {}).get('ltt')}")
        print(f"   - Full Size: {len(raw_json)} bytes")
    else:
        print("   ❌ RAW JSON NOT FOUND!")
        print(f"   Keys: {entry_data.keys()}")

if __name__ == "__main__":
    asyncio.run(check_redis())
