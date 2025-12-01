from fastapi import FastAPI, WebSocket, WebSocketDisconnect, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import asyncio
import json
import logging
import os
import subprocess
from typing import Optional
from app.db.redis_client import RedisClient
from app.db.postgres_client import PostgresClient

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("API")

app = FastAPI(title="Whale Hunter API")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

redis_client = RedisClient()
pg_client = PostgresClient()

# Global variable to track ingestion process
ingestion_process: Optional[subprocess.Popen] = None

# Request models
class TokenRequest(BaseModel):
    api_key: str
    api_secret: str

class IngestionRequest(BaseModel):
    expiry_date: str  # YYYY-MM-DD format
    strike_price: int

@app.on_event("startup")
async def startup_event():
    await redis_client.connect()
    await pg_client.connect()
    logger.info("✅ API Startup: Redis & Postgres Connected")

    # Log all registered routes
    logger.info("📋 Registered API Endpoints:")
    for route in app.routes:
        if hasattr(route, "methods"):
            logger.info(f"   {list(route.methods)} {route.path}")
    logger.info("📖 API Documentation: http://localhost:8001/docs")

@app.on_event("shutdown")
async def shutdown_event():
    await redis_client.disconnect()
    await pg_client.disconnect()
    logger.info("🛑 API Shutdown: Redis & Postgres Disconnected")

@app.websocket("/ws/signals")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    logger.info("🔌 WebSocket Connected")
    
    # Create a new PubSub instance for this connection
    pubsub = redis_client.client.pubsub()
    await pubsub.subscribe("trade_signals")
    
    try:
        async for message in pubsub.listen():
            if message["type"] == "message":
                data = message["data"]
                # data is bytes because decode_responses=False in RedisClient
                if isinstance(data, bytes):
                    data = data.decode("utf-8")
                
                # Forward to WebSocket
                await websocket.send_text(data)
                
    except WebSocketDisconnect:
        logger.info("🔌 WebSocket Disconnected")
    except Exception as e:
        logger.error(f"❌ WebSocket Error: {e}")
    finally:
        # Cleanup
        try:
            await pubsub.unsubscribe("trade_signals")
            await pubsub.close()
        except Exception as e:
            logger.error(f"Error closing pubsub: {e}")

@app.get("/")
async def root():
    """API health check."""
    return {"status": "ok", "message": "Whale Hunter API is running"}

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "api": "running"}

@app.get("/dashboard/history")
async def get_dashboard_history():
    """Fetch historical data for dashboard initialization."""
    try:
        data = await pg_client.get_dashboard_data(limit=50)

        # Convert datetime objects to ISO strings for JSON serialization
        for key, rows in data.items():
            for row in rows:
                if 'timestamp' in row and row['timestamp']:
                    row['timestamp'] = row['timestamp'].isoformat()
                if 'created_at' in row and row['created_at']:
                    row['created_at'] = row['created_at'].isoformat()

        return data
    except Exception as e:
        logger.error(f"❌ Failed to fetch history: {e}")
        return {}

@app.post("/save-credentials")
async def save_credentials(request: TokenRequest):
    """Save Upstox API credentials to .env file."""
    try:
        # Update .env file with credentials
        env_path = ".env"

        # Read current .env
        with open(env_path, 'r') as f:
            lines = f.readlines()

        # Update API key and secret
        updated_key = False
        updated_secret = False

        for i, line in enumerate(lines):
            if line.startswith('UPSTOX_API_KEY='):
                lines[i] = f'UPSTOX_API_KEY={request.api_key}\n'
                updated_key = True
            elif line.startswith('UPSTOX_API_SECRET='):
                lines[i] = f'UPSTOX_API_SECRET={request.api_secret}\n'
                updated_secret = True

        if not updated_key:
            lines.append(f'\nUPSTOX_API_KEY={request.api_key}\n')
        if not updated_secret:
            lines.append(f'UPSTOX_API_SECRET={request.api_secret}\n')

        # Write back
        with open(env_path, 'w') as f:
            f.writelines(lines)

        logger.info("✅ Credentials saved to .env")
        return {
            "status": "success",
            "message": "Credentials saved. Please run: python get_token.py"
        }

    except Exception as e:
        logger.error(f"❌ Error saving credentials: {e}")
        return {"status": "error", "detail": str(e)}

@app.post("/start-ingestion")
async def start_ingestion(request: IngestionRequest, background_tasks: BackgroundTasks):
    """Start data ingestion with specified parameters."""
    global ingestion_process

    try:
        # Stop existing ingestion if running
        if ingestion_process and ingestion_process.poll() is None:
            logger.info("⏹️ Stopping existing ingestion...")
            ingestion_process.terminate()
            ingestion_process.wait(timeout=5)

        # Save ingestion config to env
        env_path = ".env"

        with open(env_path, 'r') as f:
            lines = f.readlines()

        updated_expiry = False
        updated_strike = False

        for i, line in enumerate(lines):
            if line.startswith('NIFTY_EXPIRY='):
                lines[i] = f'NIFTY_EXPIRY={request.expiry_date}\n'
                updated_expiry = True
            elif line.startswith('NIFTY_STRIKE='):
                lines[i] = f'NIFTY_STRIKE={request.strike_price}\n'
                updated_strike = True

        if not updated_expiry:
            lines.append(f'\nNIFTY_EXPIRY={request.expiry_date}\n')
        if not updated_strike:
            lines.append(f'NIFTY_STRIKE={request.strike_price}\n')

        with open(env_path, 'w') as f:
            f.writelines(lines)

        # Start ingestion process
        logger.info(f"🚀 Starting ingestion: Expiry={request.expiry_date}, Strike={request.strike_price}")
        ingestion_process = subprocess.Popen(
            ['python', '-m', 'app.services.ingestion'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        return {"status": "success", "message": "Ingestion started"}

    except Exception as e:
        logger.error(f"❌ Error starting ingestion: {e}")
        return {"status": "error", "detail": str(e)}

@app.post("/stop-ingestion")
async def stop_ingestion():
    """Stop running data ingestion."""
    global ingestion_process

    try:
        if ingestion_process and ingestion_process.poll() is None:
            logger.info("⏹️ Stopping ingestion...")
            ingestion_process.terminate()
            ingestion_process.wait(timeout=5)
            return {"status": "success", "message": "Ingestion stopped"}
        else:
            return {"status": "info", "message": "No ingestion process running"}

    except Exception as e:
        logger.error(f"❌ Error stopping ingestion: {e}")
        return {"status": "error", "detail": str(e)}
