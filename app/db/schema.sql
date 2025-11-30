-- Whale Hunter HFT - Advanced Database Schema

-- 1. Market History (1-Minute Candles with Advanced Metrics)
CREATE TABLE IF NOT EXISTS market_history_1m (
    id SERIAL PRIMARY KEY,
    instrument_key VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    
    -- Core OHLCV
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume BIGINT NOT NULL,
    oi BIGINT,
    oi_change BIGINT,
    
    -- Greeks & Volatility
    delta DOUBLE PRECISION,
    gamma DOUBLE PRECISION,
    theta DOUBLE PRECISION,
    vega DOUBLE PRECISION,
    iv DOUBLE PRECISION,
    iv_change DOUBLE PRECISION,
    iv_percentile DOUBLE PRECISION,
    
    -- Whale Detection
    whale_vol BIGINT,
    whale_impact_score DOUBLE PRECISION,
    whale_side VARCHAR(10), -- 'BID', 'ASK', 'NEUTRAL'
    absorption_strength BIGINT,
    distribution_strength BIGINT,
    
    -- Seller Behavior
    seller_panic_score DOUBLE PRECISION,
    profit_booking_score DOUBLE PRECISION,
    seller_exhaustion DOUBLE PRECISION,
    
    -- Position Analysis
    oi_velocity DOUBLE PRECISION, -- OI change per minute
    price_change_pct DOUBLE PRECISION,
    oi_price_corr DOUBLE PRECISION,
    position_type VARCHAR(5), -- 'LB', 'SB', 'SC', 'LU'
    
    -- Walls (Support/Resistance)
    wall_price DOUBLE PRECISION,
    wall_qty BIGINT,
    wall_side VARCHAR(10), -- 'BID' or 'ASK'
    
    -- Sentiment & Valuation
    sentiment VARCHAR(10), -- 'Bull', 'Bear', 'Neutral'
    pcr DOUBLE PRECISION,
    msv DOUBLE PRECISION, -- Market Sentiment Value (Price - VWAP)
    intrinsic_value DOUBLE PRECISION,
    extrinsic_value DOUBLE PRECISION,
    
    -- Composite Scores
    momentum_score DOUBLE PRECISION,
    breakout_prob DOUBLE PRECISION,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for fast retrieval
CREATE INDEX IF NOT EXISTS idx_market_history_key_ts ON market_history_1m (instrument_key, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_market_history_ts ON market_history_1m (timestamp DESC);

-- 2. Trade Signals (Strategy Output)
CREATE TABLE IF NOT EXISTS trade_signals (
    id SERIAL PRIMARY KEY,
    instrument_key VARCHAR(50) NOT NULL,
    signal_type VARCHAR(10) NOT NULL, -- 'BUY', 'SELL'
    price DOUBLE PRECISION NOT NULL,
    confidence DOUBLE PRECISION,
    reason TEXT,
    
    -- Signal Metrics Snapshot
    iv_at_signal DOUBLE PRECISION,
    delta_at_signal DOUBLE PRECISION,
    whale_score_at_signal DOUBLE PRECISION,
    
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_signals_ts ON trade_signals (timestamp DESC);
