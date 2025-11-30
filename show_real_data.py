import asyncio
import sys
import os
import logging
import numpy as np

# Add project root to path
sys.path.append(os.getcwd())

# Disable logging for redis client
logging.getLogger("app.db.redis_client").setLevel(logging.WARNING)

from app.services.processor import MarketDataProcessor

async def show_real_data():
    print("="*60)
    print("REAL DATA INSPECTION TOOL")
    print("="*60)
    
    # Check for command line argument
    target_instrument = None
    if len(sys.argv) > 1:
        target_instrument = sys.argv[1]

    # Connect to Redis to find streams if needed
    from app.db.redis_client import RedisClient
    redis = RedisClient()
    await redis.connect()
    
    if not target_instrument:
        print("No instrument specified. Scanning for available streams...")
        keys = await redis.client.keys("stream:*")
        stream_names = [k.decode('utf-8').replace('stream:', '') for k in keys]
        
        if not stream_names:
            print("❌ No active streams found in Redis.")
            await redis.disconnect()
            return
            
        print(f"Found {len(stream_names)} active streams:")
        for name in stream_names[:10]: # Show first 10
            print(f" - {name}")
        if len(stream_names) > 10:
            print(f" ... and {len(stream_names)-10} more.")
            
        target_instrument = stream_names[0]
        print(f"\n⚠️ Defaulting to first stream: {target_instrument}")
        print(f"💡 Usage: uv run python show_real_data.py <INSTRUMENT_KEY>")
    
    await redis.disconnect()

    processor = MarketDataProcessor()
    await processor.connect()
    
    print(f"\nFetching data for: {target_instrument}")
    
    # Fetch last 24 hours to ensure we get data even if market is closed
    ticks = await processor.fetch_ticks(target_instrument, duration_seconds=86400)
    print(f"Fetched {len(ticks)} ticks from last 24 hours.")
    
    if not ticks:
        print("Stream is empty or key not found.")
        await processor.disconnect()
        return

    arrays = processor.get_arrays(ticks)
    
    print("\nProcessed Data (First Tick Values):")
    print("-" * 60)
    
    # Define field order for better readability
    priority_fields = ['ltt', 'ltp', 'cp', 'oi', 'volume', 'atp', 'tbq', 'tsq']
    greek_fields = ['delta', 'gamma', 'theta', 'vega', 'rho', 'iv']
    depth_fields = ['bid_prices', 'bid_qtys', 'ask_prices', 'ask_qtys']
    
    # Print Priority Fields
    for key in priority_fields:
        if key in arrays and len(arrays[key]) > 0:
            val = arrays[key][0]
            if key == 'ltt':
                print(f"{key:<12}: {val} (Timestamp)")
            elif isinstance(val, (float, np.float32, np.float64)):
                print(f"{key:<12}: {val:.4f}")
            else:
                print(f"{key:<12}: {val}")

    print("-" * 30)
    
    # Print Greeks
    for key in greek_fields:
        if key in arrays and len(arrays[key]) > 0:
            val = arrays[key][0]
            print(f"{key:<12}: {val:.4f}")

    print("-" * 30)

    # Print Depth
    for key in depth_fields:
        if key in arrays and len(arrays[key]) > 0:
            val = arrays[key][0]
            top_5 = val[:5]
            print(f"{key:<12}: {top_5} ... (Total Levels: {len(val)})")
            
    print("-" * 60)

    await processor.disconnect()

if __name__ == "__main__":
    asyncio.run(show_real_data())
