import asyncio
import logging
import json
import os
import sys
from datetime import datetime

# Add app to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from app.db.redis_client import RedisClient
from app.db.postgres_client import PostgresClient
from app.services.processor import MarketDataProcessor
from app.analytics.pattern_detector import analyze_oi_pattern
from app.analytics.imbalance_detector import analyze_order_book_imbalance
from app.analytics.greeks_analyzer import analyze_greeks_momentum
from app.analytics.whale_detector import analyze_whale_activity
from app.analytics.sentiment_analyzer import analyze_market_sentiment
from app.core.time_utils import get_ist_time, get_seconds_to_next_minute

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("SignalGenerator")

class SignalGenerator:
    def __init__(self):
        self.redis = RedisClient()
        self.pg = PostgresClient()
        self.processor = MarketDataProcessor()
        self.running = False

    async def start(self):
        """Start the signal generation loop."""
        logger.info("🚀 Starting Signal Generator...")
        await self.redis.connect()
        await self.pg.connect()
        await self.processor.connect()
        self.running = True
        
        try:
            while self.running:
                # 1. Align to next minute start
                sleep_seconds = get_seconds_to_next_minute()
                if sleep_seconds < 0.1: sleep_seconds += 60 # Safety buffer
                
                logger.info(f"⏳ Waiting {sleep_seconds:.2f}s for next minute candle...")
                await asyncio.sleep(sleep_seconds)
                
                # 2. Define Time Window (Previous Minute)
                # If we just woke up at 09:31:00, we want 09:30:00 to 09:31:00
                current_time = get_ist_time()
                end_time_ms = int(current_time.timestamp() * 1000)
                start_time_ms = end_time_ms - 60000 # 60 seconds ago
                
                logger.info(f"🕒 Processing Candle: {current_time.strftime('%H:%M:%S')} (Window: -60s)")

                # 3. Scan for active streams
                cursor = b'0'
                streams = []
                while cursor:
                    cursor, keys = await self.redis.client.scan(cursor, match="stream:*", count=100)
                    streams.extend(keys)
                    if cursor == b'0':
                        break
                
                if not streams:
                    logger.warning("⚠️ No active streams found.")
                    continue

                logger.info(f"🔍 Scanning {len(streams)} streams for patterns...")
                
                for stream_key_bytes in streams:
                    stream_key = stream_key_bytes.decode('utf-8')
                    instrument_key = stream_key.replace("stream:", "")
                    
                    # 4. Fetch Data (Exact Previous Minute)
                    ticks = await self.processor.fetch_ticks_range(instrument_key, start_time_ms, end_time_ms)
                    
                    if not ticks:
                        continue
                        
                    # 5. Convert to Arrays
                    arrays = self.processor.get_arrays(ticks)
                    
                    # 6. Analyze Pattern (Metric 1)
                    result = analyze_oi_pattern(arrays)
                    
                    # 7. Analyze Imbalance (Metric 2)
                    imbalance_result = analyze_order_book_imbalance(arrays)
                    
                    # 8. Analyze Greeks Momentum (Metric 4)
                    greeks_result = analyze_greeks_momentum(arrays)
                    
                    # 9. Analyze Whale Activity (Metric 3)
                    whale_alerts = analyze_whale_activity(arrays)
                    
                    # 10. Publish Signal (ALL Patterns)
                    signal_payload = {
                        "timestamp": current_time, # Keep as datetime object for PG (IST aware)
                        "instrument_key": instrument_key,
                        "pattern": result['pattern'],
                        "signal": result['signal'],
                        "metrics": {
                            "price_change": result['price_change'],
                            "oi_change": result['oi_change'],
                            "volume_change": result['volume_change'],
                            "ltp": result['ltp'],
                            "price_change_pct": result['price_change_pct'],
                            "delta_change": result['delta_change'],
                            "momentum_score": greeks_result['momentum_score']
                        }
                    }
                    
                    # Publish to Redis Pub/Sub (Convert datetime to str for JSON)
                    redis_payload = signal_payload.copy()
                    redis_payload['timestamp'] = redis_payload['timestamp'].isoformat()
                    await self.redis.client.publish("trade_signals", json.dumps(redis_payload))
                    
                    # Insert into PostgreSQL (Main Table)
                    await self.pg.insert_pattern(signal_payload)
                    
                    # Insert into PostgreSQL (Panic Table) if Panic Detected
                    if result.get('is_panic', False):
                        await self.pg.insert_panic_signal(signal_payload)
                        logger.info(f"🔥 PANIC SIGNAL: {instrument_key} | {result['pattern']} | OI Drop: {result['oi_change']} | Price Jump: {result['price_change_pct']:.2f}%")
                    
                    # Insert into PostgreSQL (Imbalance Table)
                    imbalance_payload = {
                        "timestamp": current_time,
                        "instrument_key": instrument_key,
                        "tbq": imbalance_result['tbq'],
                        "tsq": imbalance_result['tsq'],
                        "imbalance_ratio": imbalance_result['imbalance_ratio'],
                        "signal": imbalance_result['signal'],
                        "ltp": imbalance_result['ltp']
                    }
                    await self.pg.insert_imbalance(imbalance_payload)
                    
                    # Insert into PostgreSQL (Greeks Momentum Table)
                    greeks_payload = {
                        "timestamp": current_time,
                        "instrument_key": instrument_key,
                        "delta_velocity": greeks_result['delta_velocity'],
                        "gamma_acceleration": greeks_result['gamma_acceleration'],
                        "iv_velocity": greeks_result['iv_velocity'],
                        "theta_acceleration": greeks_result['theta_acceleration'],
                        "momentum_score": greeks_result['momentum_score'],
                        "momentum_type": greeks_result['momentum_type'],
                        "signal": greeks_result['signal']
                    }
                    await self.pg.insert_greeks_momentum(greeks_payload)
                    
                    # Insert into PostgreSQL (Whale Alerts Table)
                    for alert in whale_alerts:
                        whale_payload = {
                            "timestamp": current_time,
                            "instrument_key": instrument_key,
                            "whale_type": alert['whale_type'],
                            "alert_type": alert['alert_type'],
                            "alert_value": alert['alert_value'],
                            "signal": alert['signal']
                        }
                        await self.pg.insert_whale_alert(whale_payload)
                        logger.info(f"🐋 WHALE ALERT: {instrument_key} | {alert['whale_type']} | {alert['alert_type']} ({alert['alert_value']})")
                    
                    # 11. Ultimate Sentiment Analysis (Metric 5)
                    # Fetch recent data from PG to include context
                    pg_data = await self.pg.get_recent_signals(instrument_key, limit=10)
                    
                    # Analyze Sentiment
                    sentiment_result = analyze_market_sentiment(pg_data, result['ltp'])
                    
                    # Insert into PostgreSQL (Sentiment Table)
                    sentiment_payload = {
                        "timestamp": current_time,
                        "instrument_key": instrument_key,
                        "sentiment": sentiment_result['sentiment'],
                        "sentiment_score": sentiment_result['sentiment_score'],
                        "components": sentiment_result['components'],
                        "support_resistance": sentiment_result['support_resistance'],
                        "trade_setup": sentiment_result['trade_setup'],
                        "market_regime": sentiment_result['market_regime'],
                        "key_insights": sentiment_result['key_insights']
                    }
                    await self.pg.insert_market_sentiment(sentiment_payload)
                    
                    if result['pattern'] not in ["Low Volume", "Neutral", "Insufficient Data"]:
                        logger.info(f"🚨 SIGNAL: {instrument_key} | {result['pattern']} ({result['signal']}) | OI Chg: {result['oi_change']}")
                    else:
                        logger.debug(f"ℹ️  UPDATE: {instrument_key} | {result['pattern']} | OI Chg: {result['oi_change']}")
                        
        except asyncio.CancelledError:
            logger.info("🛑 Signal Generator stopping...")
        except Exception as e:
            logger.error(f"❌ Error in signal loop: {e}", exc_info=True)
        finally:
            await self.stop()

    async def stop(self):
        self.running = False
        await self.redis.disconnect()
        await self.pg.disconnect()
        await self.processor.disconnect()
        logger.info("👋 Signal Generator stopped.")

if __name__ == "__main__":
    generator = SignalGenerator()
    try:
        asyncio.run(generator.start())
    except KeyboardInterrupt:
        pass
