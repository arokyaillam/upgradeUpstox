"""
Advanced Upstox Data Ingestion Manager
Handles real-time market data with dynamic subscription management and smart routing.

Financial Logic:
- INDEX (Spot): Track Nifty 50 spot price and OHLC for trend analysis
- OPTIONS: Track tick data, Greeks, and whale orders for trading signals
- Dynamic subscriptions: Update option strikes as market moves
- 1-minute retention: Keep only recent data in hot storage (Redis Streams)
"""

import asyncio
import json
import ssl
import logging
import time
from typing import Optional, List, Set, Dict, Any
import websockets
import requests
from google.protobuf.json_format import MessageToDict
import sys
import os

# Fix path to allow running directly
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from app.db.redis_client import RedisClient
from app.services import MarketDataFeedV3_pb2 as pb
from app.services.instruments import SmartOptionMapper

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s'
)
logger = logging.getLogger(__name__)


class UpstoxDataManager:
    """
    Advanced real-time market data manager with dynamic subscription management.
    
    Financial Logic:
    - Ingests raw market data from Upstox
    - Pushes full JSON payload to Redis Streams
    - Stream Key: stream:{instrument_key}
    - Retention: 1 minute (Hot Storage)
    
    Data Flow:
    - Upstox WebSocket -> Protobuf Decode -> JSON -> Redis Stream
    """
    
    # TBT Data Retention: 2 Minutes (120 seconds) - Keep recent ticks for processing
    DATA_RETENTION_SECONDS = 120
    RECONNECT_DELAY = 5  # Seconds between reconnection attempts
    
    def __init__(
        self,
        access_token: str,
        redis_host: str = "localhost",
        redis_port: int = 6379
    ):
        """
        Initialize the Upstox Data Manager.
        
        Args:
            access_token: Upstox API access token
            redis_host: Redis server host
            redis_port: Redis server port
        """
        self.access_token = access_token
        self.redis_client = RedisClient(host=redis_host, port=redis_port)
        self.websocket: Optional[websockets.WebSocketClientProtocol] = None
        self.is_running = False
        
        # Subscription management
        self.index_key = "NSE_INDEX|Nifty 50"  # Always subscribed (static)
        self.active_option_keys: Set[str] = set()  # Dynamically updated
        self.key_mapping: Dict[str, str] = {}  # Map instrument_key -> readable_name (e.g. "26200_CE")
    
    async def connect_redis(self) -> None:
        """Establish connection to Redis."""
        await self.redis_client.connect()
    
    def get_authorization_url(self) -> str:
        """
        Get WebSocket authorization URL from Upstox API.
        
        Returns:
            Authorized redirect URI for WebSocket connection
        """
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {self.access_token}'
        }
        url = 'https://api.upstox.com/v3/feed/market-data-feed/authorize'
        
        try:
            response = requests.get(url=url, headers=headers)
            response.raise_for_status()
            data = response.json()
            logger.info("✓ Received WebSocket authorization")
            return data["data"]["authorized_redirect_uri"]
        except Exception as e:
            logger.error(f"✗ Authorization failed: {e}")
            raise
    
    def decode_protobuf(self, buffer: bytes) -> pb.FeedResponse:
        """
        Decode Protobuf message from WebSocket.
        
        Args:
            buffer: Binary protobuf data
        
        Returns:
            Decoded FeedResponse object
        """
        feed_response = pb.FeedResponse()
        feed_response.ParseFromString(buffer)
        return feed_response
    
    async def update_subscriptions(self, new_keys_list: List[str], guid: str = "whale_hunter_options") -> None:
        """
        Dynamically update option subscriptions while keeping INDEX always subscribed.
        
        Financial Logic:
        - As market moves, ATM strikes change (e.g., 24000 → 24100)
        - We need to unsubscribe old OTM strikes and subscribe new ATM strikes
        - INDEX (Nifty 50 spot) always remains subscribed for trend analysis
        
        Args:
            new_keys_list: List of new option instrument keys to subscribe
            guid: Unique identifier for this subscription group
        """
        new_keys_set = set(new_keys_list)
        
        # Calculate subscription changes
        keys_to_unsubscribe = self.active_option_keys - new_keys_set
        keys_to_subscribe = new_keys_set - self.active_option_keys
        
        # Unsubscribe old keys
        if keys_to_unsubscribe:
            logger.info(f"📤 Unsubscribing {len(keys_to_unsubscribe)} old option keys")
            unsubscribe_msg = {
                "guid": guid,
                "method": "unsub",
                "data": {
                    "instrumentKeys": list(keys_to_unsubscribe)
                }
            }
            await self.websocket.send(json.dumps(unsubscribe_msg).encode('utf-8'))
        
        # Subscribe to new keys (FULL mode for complete data)
        if keys_to_subscribe:
            logger.info(f"📥 Subscribing {len(keys_to_subscribe)} new option keys")
            subscribe_msg = {
                "guid": guid,
                "method": "sub",
                "data": {
                    "mode": "full_d30",  # FULL mode: get depth, greeks, OHLC
                    "instrumentKeys": list(keys_to_subscribe)
                }
            }
            await self.websocket.send(json.dumps(subscribe_msg).encode('utf-8'))
        
        # Update active set
        self.active_option_keys = new_keys_set
        logger.info(f"✓ Active subscriptions: {len(self.active_option_keys)} options + 1 index")
    
    async def process_raw_feed(
        self,
        instrument_key: str,
        feed_data: Dict[str, Any]
    ) -> None:
        """
        Stream raw feed data to Redis.
        
        Args:
            instrument_key: Instrument identifier
            feed_data: Complete feed data dictionary
        """
        # Serialize the entire feed data to JSON
        raw_json = json.dumps(feed_data)
        
        # Determine Stream Key Name
        # If we have a readable name mapping, use it (e.g. "26200_CE")
        # Otherwise fallback to instrument_key
        readable_name = self.key_mapping.get(instrument_key, instrument_key)
        
        # Stream key: stream:{readable_name}
        stream_key = f"stream:{readable_name}"
        
        # Time-based trimming (1-minute window)
        current_time_ms = int(time.time() * 1000)
        min_id_ms = current_time_ms - (self.DATA_RETENTION_SECONDS * 1000)
        
        # Use LTT for ID if available
        market_ff = feed_data.get("marketFF", {})
        ltpc = market_ff.get("ltpc", {})
        ltt_ms = int(ltpc.get("ltt", current_time_ms))
        
        await self.redis_client.client.xadd(
            stream_key,
            {"json": raw_json},
            id="*",  # Auto-generate ID to avoid collisions/out-of-order errors
            minid=f"{min_id_ms}-0"
        )
    
    async def on_message(self, message: bytes) -> None:
        """
        The Traffic Controller - Routes messages based on instrument type.
        
        Flow:
        1. Decode Protobuf message
        2. Check instrument_key
        3. Route to INDEX handler OR OPTION handlers
        4. Process concurrently for performance
        
        Args:
            message: Binary protobuf message from Upstox
        """
        try:
            # Decode protobuf
            decoded_data = self.decode_protobuf(message)
            data_dict = MessageToDict(decoded_data)
            
            # Process each feed
            feeds = data_dict.get("feeds", {})
            
            for instrument_key, feed_data in feeds.items():
                full_feed = feed_data.get("fullFeed", {})
                market_ff = full_feed.get("marketFF", {})
                
                if not market_ff:
                    continue
                
                # Push RAW JSON to Redis Stream
                # This handles both INDEX and OPTIONS uniformly
                await self.process_raw_feed(instrument_key, full_feed)
                
        except Exception as e:
            logger.error(f"✗ Error processing message: {e}", exc_info=True)
    
    async def subscribe_initial(self, option_keys: List[str]) -> None:
        """
        Subscribe to initial set of instruments (INDEX + OPTIONS).
        
        Args:
            option_keys: Initial list of option instrument keys
        """
        # Subscribe to INDEX (always FULL mode for OHLC)
        index_msg = {
            "guid": "whale_hunter_index",
            "method": "sub",
            "data": {
                "mode": "full",
                "instrumentKeys": [self.index_key]
            }
        }
        await self.websocket.send(json.dumps(index_msg).encode('utf-8'))
        logger.info(f"✓ Subscribed to INDEX: {self.index_key}")
        
        # Subscribe to OPTION keys
        if option_keys:
            await self.update_subscriptions(option_keys, guid="whale_hunter_options")
    
    async def start_stream(self, initial_option_keys: List[str]) -> None:
        """
        Start WebSocket stream with auto-reconnection logic.
        
        Args:
            initial_option_keys: Initial list of option keys to subscribe
        """
        self.is_running = True
        
        while self.is_running:
            try:
                # Get authorization URL
                ws_url = self.get_authorization_url()
                
                # Create SSL context
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
                
                # Connect to WebSocket
                async with websockets.connect(ws_url, ssl=ssl_context) as websocket:
                    self.websocket = websocket
                    logger.info("🔌 WebSocket connected")
                    
                    # Subscribe to instruments
                    await asyncio.sleep(1)  # Stabilize connection
                    await self.subscribe_initial(initial_option_keys)
                    
                    # Receive and process messages
                    while self.is_running:
                        try:
                            message = await websocket.recv()
                            await self.on_message(message)
                        except websockets.exceptions.ConnectionClosed:
                            logger.warning("⚠️  WebSocket connection closed")
                            break
                        except Exception as e:
                            logger.error(f"✗ Error receiving message: {e}")
            
            except Exception as e:
                logger.error(f"✗ WebSocket error: {e}")
            
            # Auto-reconnection logic
            if self.is_running:
                logger.info(f"🔄 Reconnecting in {self.RECONNECT_DELAY} seconds...")
                await asyncio.sleep(self.RECONNECT_DELAY)
        
        logger.info("🛑 Stream stopped")
    
    async def stop(self) -> None:
        """Gracefully stop the data manager."""
        logger.info("🛑 Stopping data manager...")
        self.is_running = False
        
        if self.websocket:
            await self.websocket.close()
        
        await self.redis_client.disconnect()
        logger.info("✓ Data manager stopped")


if __name__ == "__main__":
    import signal
    from dotenv import load_dotenv
    from app.services.instruments import SmartOptionMapper

    async def main():
        # Load environment variables
        load_dotenv()
        
        access_token = os.getenv("UPSTOX_ACCESS_TOKEN")
        if not access_token:
            logger.error("❌ UPSTOX_ACCESS_TOKEN not found in .env")
            print("Please run 'uv run get_token.py' first.")
            return

        print("=" * 60)
        print("🚀 WHALE HUNTER - INGESTION SERVICE")
        print("=" * 60)

        # Initialize Mapper
        mapper = SmartOptionMapper(access_token)
        
        # 1. Get Expiry
        default_expiry = mapper.get_weekly_expiry()
        expiry_input = input(f"\\nEnter Expiry Date (default {default_expiry}): ").strip()
        expiry = expiry_input if expiry_input else default_expiry
        
        # 2. Get Strike
        strike_input = input("Enter Center Strike (e.g., 24000): ").strip()
        if not strike_input:
            logger.error("❌ Center strike is required.")
            return
        center_strike = int(strike_input)
        
        # 3. Fetch Initial Keys
        print(f"\\n🔍 Fetching instrument keys for {expiry} @ {center_strike}...")
        try:
            keys_list, keys_dict = mapper.fetch_nifty_keys(expiry, center_strike)
        except Exception as e:
            logger.error(f"❌ Failed to fetch keys: {e}")
            return

        if not keys_list:
            logger.error("❌ No instrument keys found. Check expiry/strike.")
            return

        print(f"✓ Found {len(keys_list)} keys to subscribe.")

        # 4. Start Ingestion Service
        manager = UpstoxDataManager(access_token=access_token)
        
        # Generate Reverse Mapping (Instrument Key -> Readable Name)
        # keys_dict structure: {26200: {'CE': 'key1', 'PE': 'key2'}}
        reverse_mapping = {}
        
        # Add Index Mapping
        reverse_mapping["NSE_INDEX|Nifty 50"] = "NIFTY_50"
        
        for strike, options in keys_dict.items():
            if options.get('CE'):
                reverse_mapping[options['CE']] = f"{strike}_CE"
            if options.get('PE'):
                reverse_mapping[options['PE']] = f"{strike}_PE"
        
        manager.key_mapping = reverse_mapping
        logger.info(f"✓ Generated {len(reverse_mapping)} key mappings")
        
        # Handle graceful shutdown
        loop = asyncio.get_running_loop()
        stop_event = asyncio.Event()
        
        def signal_handler():
            print("\\n🛑 Received shutdown signal...")
            stop_event.set()
            manager.is_running = False

        try:
            # Windows signal handling is limited, but we'll try
            try:
                loop.add_signal_handler(signal.SIGINT, signal_handler)
                loop.add_signal_handler(signal.SIGTERM, signal_handler)
            except NotImplementedError:
                pass

            await manager.connect_redis()
            await manager.start_stream(initial_option_keys=keys_list)
            
        except KeyboardInterrupt:
            print("\\n🛑 User stopped the process.")
        finally:
            await manager.stop()

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
