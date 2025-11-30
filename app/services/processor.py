"""
Market Data Processor Service
Fetches raw TBT data from Redis and converts it into NumPy arrays for high-performance calculation.
"""

import json
import time
import logging
import numpy as np
from typing import Dict, List, Any, Optional
from app.db.redis_client import RedisClient

logger = logging.getLogger(__name__)

class MarketDataProcessor:
    def __init__(self, redis_host: str = "localhost", redis_port: int = 6379):
        self.redis_client = RedisClient(host=redis_host, port=redis_port)

    async def connect(self):
        """Connect to Redis."""
        await self.redis_client.connect()

    async def disconnect(self):
        """Disconnect from Redis."""
        await self.redis_client.disconnect()

    async def fetch_ticks(self, instrument_key: str, duration_seconds: int = 60) -> List[Dict[str, Any]]:
        """
        Fetch raw ticks from Redis stream for the last N seconds.
        
        Args:
            instrument_key: The instrument key (e.g., 'NSE_INDEX|Nifty 50')
            duration_seconds: How far back to fetch data (default 60s)
            
        Returns:
            List of tick dictionaries
        """
        # Handle readable names if needed, but assuming raw key for now or mapped name
        # The ingestion service uses "stream:{readable_name}"
        # We'll assume the caller passes the correct stream suffix or we can try both
        
        stream_key = f"stream:{instrument_key}"
        
        # Calculate min ID for time window
        current_time_ms = int(time.time() * 1000)
        min_id = current_time_ms - (duration_seconds * 1000)
        
        try:
            # XRANGE stream_key min_id + COUNT 10000 (safe limit)
            # We use min_id as start, '+' as end
            entries = await self.redis_client.client.xrange(stream_key, min=min_id, max="+")
            
            ticks = []
            for entry_id, data in entries:
                if b'json' in data:
                    try:
                        tick_json = json.loads(data[b'json'])
                        # Extract relevant fields from the complex Upstox structure
                        # Structure: {marketFF: {ltpc: {ltp: ...}, marketLevel: {bidAskQuote: ...}, ...}}
                        
                        # Flatten/Extract what we need for the arrays
                        market_ff = tick_json.get('marketFF', {})
                        ltpc = market_ff.get('ltpc', {})
                        market_level = market_ff.get('marketLevel', {})
                        bid_ask_quote = market_level.get('bidAskQuote', []) # List of dicts
                        
                        # Option Greeks are inside marketFF
                        option_greeks = market_ff.get('optionGreeks', {})
                        if not option_greeks:
                             # Fallback to top level
                             option_greeks = tick_json.get('optionGreeks', {})
                        
                        # Extract Volume (vtt) and OI
                        # Try marketFF first, then top level
                        volume = int(market_ff.get('vtt', 0) or 0)
                        if volume == 0:
                             volume = int(tick_json.get('vtt', 0) or 0)
                        if volume == 0:
                             volume = int(ltpc.get('volume', 0) or 0)
                             
                        oi = int(market_ff.get('oi', 0) or 0)
                        if oi == 0:
                             oi = int(tick_json.get('oi', 0) or 0)
                        
                        # IV
                        iv = float(market_ff.get('iv', 0.0))
                        if iv == 0.0:
                             iv = float(tick_json.get('iv', 0.0))
                        if iv == 0.0:
                            iv = float(option_greeks.get('iv', 0.0))
                            
                        # Extract TBQ, TSQ, ATP
                        # Try marketFF first, then top level
                        tbq = int(market_ff.get('tbq', 0) or 0)
                        if tbq == 0: tbq = int(tick_json.get('tbq', 0) or 0)
                        
                        tsq = int(market_ff.get('tsq', 0) or 0)
                        if tsq == 0: tsq = int(tick_json.get('tsq', 0) or 0)
                        
                        atp = float(market_ff.get('atp', 0.0))
                        if atp == 0.0: atp = float(tick_json.get('atp', 0.0))

                        # Safe extraction with defaults
                        tick = {
                            'ltp': float(ltpc.get('ltp', 0.0)),
                            'ltt': int(ltpc.get('ltt', 0) or 0),
                            'cp': float(ltpc.get('cp', 0.0)),
                            'oi': oi,
                            'volume': volume,
                            'atp': atp,
                            'tbq': tbq,
                            'tsq': tsq,
                            # Greeks
                            'delta': float(option_greeks.get('delta', 0.0)),
                            'gamma': float(option_greeks.get('gamma', 0.0)),
                            'theta': float(option_greeks.get('theta', 0.0)),
                            'vega': float(option_greeks.get('vega', 0.0)),
                            'rho': float(option_greeks.get('rho', 0.0)),
                            'iv': iv,
                            # Bid/Ask Depth (Will hold list of dicts temporarily)
                            'depth': []
                        }
                        
                        # Extract Full Market Depth (Up to 30 levels)
                        # Format: [{"bidQ": "225", "bidP": 25.6, "askQ": "150", "askP": 25.75}, ...]
                        if isinstance(bid_ask_quote, list):
                            tick['depth'] = bid_ask_quote
                        elif isinstance(bid_ask_quote, dict):
                             # Handle potential dict format if API changes
                             pass

                        ticks.append(tick)
                        
                    except json.JSONDecodeError:
                        continue
                        
            return ticks
            
        except Exception as e:
            logger.error(f"Error fetching ticks for {instrument_key}: {e}")
            return []

    def get_arrays(self, ticks: List[Dict[str, Any]]) -> Dict[str, np.ndarray]:
        """
        Convert list of ticks to NumPy arrays.
        
        Args:
            ticks: List of tick dictionaries
            
        Returns:
            Dictionary of NumPy arrays.
            Depth arrays (bid_prices, bid_qtys, ask_prices, ask_qtys) will be (N, 30).
        """
        if not ticks:
            return {
                'ltp': np.array([], dtype=np.float32),
                'ltt': np.array([], dtype=np.int64),
                'cp': np.array([], dtype=np.float32),
                'oi': np.array([], dtype=np.int64),
                'volume': np.array([], dtype=np.int64),
                'atp': np.array([], dtype=np.float32),
                'tbq': np.array([], dtype=np.int64),
                'tsq': np.array([], dtype=np.int64),
                'delta': np.array([], dtype=np.float32),
                'gamma': np.array([], dtype=np.float32),
                'theta': np.array([], dtype=np.float32),
                'vega': np.array([], dtype=np.float32),
                'rho': np.array([], dtype=np.float32),
                'iv': np.array([], dtype=np.float32),
                # Depth Arrays (N, 30)
                'bid_prices': np.zeros((0, 30), dtype=np.float32),
                'bid_qtys': np.zeros((0, 30), dtype=np.int64),
                'ask_prices': np.zeros((0, 30), dtype=np.float32),
                'ask_qtys': np.zeros((0, 30), dtype=np.int64),
            }

        # Pre-allocate depth arrays
        n_ticks = len(ticks)
        bid_prices = np.zeros((n_ticks, 30), dtype=np.float32)
        bid_qtys = np.zeros((n_ticks, 30), dtype=np.int64)
        ask_prices = np.zeros((n_ticks, 30), dtype=np.float32)
        ask_qtys = np.zeros((n_ticks, 30), dtype=np.int64)

        for i, t in enumerate(ticks):
            depth = t.get('depth', [])
            for j, level in enumerate(depth[:30]): # Limit to 30
                bid_prices[i, j] = float(level.get('bidP', 0.0))
                bid_qtys[i, j] = int(level.get('bidQ', 0) or 0)
                ask_prices[i, j] = float(level.get('askP', 0.0))
                ask_qtys[i, j] = int(level.get('askQ', 0) or 0)

        return {
            'ltp': np.array([t['ltp'] for t in ticks], dtype=np.float32),
            'ltt': np.array([t['ltt'] for t in ticks], dtype=np.int64),
            'cp': np.array([t['cp'] for t in ticks], dtype=np.float32),
            'oi': np.array([t['oi'] for t in ticks], dtype=np.int64),
            'volume': np.array([t['volume'] for t in ticks], dtype=np.int64),
            'atp': np.array([t['atp'] for t in ticks], dtype=np.float32),
            'tbq': np.array([t['tbq'] for t in ticks], dtype=np.int64),
            'tsq': np.array([t['tsq'] for t in ticks], dtype=np.int64),
            'delta': np.array([t['delta'] for t in ticks], dtype=np.float32),
            'gamma': np.array([t['gamma'] for t in ticks], dtype=np.float32),
            'theta': np.array([t['theta'] for t in ticks], dtype=np.float32),
            'vega': np.array([t['vega'] for t in ticks], dtype=np.float32),
            'rho': np.array([t['rho'] for t in ticks], dtype=np.float32),
            'iv': np.array([t['iv'] for t in ticks], dtype=np.float32),
            'bid_prices': bid_prices,
            'bid_qtys': bid_qtys,
            'ask_prices': ask_prices,
            'ask_qtys': ask_qtys,
        }
