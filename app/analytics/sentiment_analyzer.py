import numpy as np
from typing import Dict, List, Any

def analyze_market_sentiment(pg_data: Dict[str, List[Dict[str, Any]]], ltp: float) -> Dict[str, Any]:
    """
    Analyze Market Sentiment using historical data from PostgreSQL.
    
    Combines:
    1. Market Patterns (Trend)
    2. Panic Signals (Extreme Events)
    3. Order Imbalance (Pressure)
    4. Greeks Momentum (Acceleration)
    5. Whale Alerts (Smart Money)
    
    Returns:
    - Sentiment Score (-100 to +100)
    - Support/Resistance Levels
    - Trade Setup (Entry, Target, Stop)
    - Market Regime
    """
    
    # 1. Calculate Sentiment Score
    score = 0.0
    insights = []
    
    # A. Pattern Analysis (Last 5 signals)
    patterns = pg_data.get('patterns', [])[:5]
    pattern_score = 0
    for p in patterns:
        sig = p.get('signal', '')
        if 'Bullish' in sig or 'Buy' in sig: pattern_score += 10
        elif 'Bearish' in sig or 'Sell' in sig: pattern_score -= 10
    score += pattern_score
    if pattern_score > 20: insights.append(f"Recent patterns are Bullish ({pattern_score})")
    elif pattern_score < -20: insights.append(f"Recent patterns are Bearish ({pattern_score})")

    # B. Panic Analysis (Any recent panic?)
    panics = pg_data.get('panic', [])
    if panics:
        last_panic = panics[0]
        if 'Buy' in last_panic.get('signal', ''):
            score += 30
            insights.append("⚠️ Recent Panic Buy detected!")
        elif 'Sell' in last_panic.get('signal', ''):
            score -= 30
            insights.append("⚠️ Recent Panic Sell detected!")

    # C. Imbalance Analysis (Average Ratio)
    imbalances = pg_data.get('imbalance', [])
    if imbalances:
        avg_ratio = np.mean([r['imbalance_ratio'] for r in imbalances])
        score += avg_ratio * 50 # Ratio is -1 to 1, so +/- 50
        if avg_ratio > 0.3: insights.append("Order Book is Bid Heavy")
        elif avg_ratio < -0.3: insights.append("Order Book is Ask Heavy")

    # D. Greeks Momentum (Last score)
    greeks = pg_data.get('greeks', [])
    if greeks:
        last_greek = greeks[0]
        mom_score = last_greek.get('momentum_score', 50)
        # Normalize 0-100 to -20 to +20
        score += (mom_score - 50) * 0.4
        if mom_score > 70: insights.append("Greeks Momentum is Explosive")

    # E. Whale Alerts (Net Count)
    whales = pg_data.get('whales', [])
    whale_score = 0
    bullish_whales = 0
    bearish_whales = 0
    for w in whales:
        if 'Bullish' in w.get('signal', ''): 
            whale_score += 15
            bullish_whales += 1
        elif 'Bearish' in w.get('signal', ''): 
            whale_score -= 15
            bearish_whales += 1
    score += whale_score
    if bullish_whales > bearish_whales: insights.append(f"Whales are Net Bullish ({bullish_whales} vs {bearish_whales})")
    elif bearish_whales > bullish_whales: insights.append(f"Whales are Net Bearish ({bearish_whales} vs {bullish_whales})")

    # Clamp Score
    score = max(-100.0, min(100.0, score))
    
    # Determine Sentiment Label
    sentiment = "Neutral"
    if score >= 60: sentiment = "Extreme Bullish 🚀"
    elif score >= 20: sentiment = "Bullish 🟢"
    elif score <= -60: sentiment = "Extreme Bearish 🩸"
    elif score <= -20: sentiment = "Bearish 🔴"

    # 2. Support & Resistance (Dynamic from Whales)
    # Find clusters of whale activity or just recent min/max from patterns
    support = ltp * 0.99 # Default fallback
    resistance = ltp * 1.01 # Default fallback
    
    # Try to find walls from whale alerts
    bid_walls = [w['alert_value'] for w in whales if w['alert_type'] == 'Bid Wall']
    ask_walls = [w['alert_value'] for w in whales if w['alert_type'] == 'Ask Wall']
    
    # Note: Whale alerts store quantity in alert_value, not price. 
    # We need price. But current whale alert schema stores 'alert_value' as the quantity/metric.
    # We assume 'ltp' was stored in patterns. Let's use recent price action for S/R for now.
    
    recent_prices = [p['metrics']['ltp'] for p in pg_data.get('patterns', []) if 'metrics' in p]
    if not recent_prices and imbalances:
        recent_prices = [i['ltp'] for i in imbalances]
        
    if recent_prices:
        support = min(recent_prices)
        resistance = max(recent_prices)
        
        # Widen if too tight
        if resistance - support < (ltp * 0.001):
            support = ltp * 0.995
            resistance = ltp * 1.005

    # 3. Trade Setup
    trade_setup = {
        "entry_price": ltp,
        "signal": "WAIT",
        "target": 0.0,
        "stop_loss": 0.0,
        "rr_ratio": 0.0
    }
    
    if score > 20: # Bullish Setup
        trade_setup["signal"] = "BUY"
        trade_setup["stop_loss"] = support - (ltp * 0.001)
        trade_setup["target"] = resistance + (ltp * 0.002)
        risk = ltp - trade_setup["stop_loss"]
        reward = trade_setup["target"] - ltp
        if risk > 0: trade_setup["rr_ratio"] = round(reward / risk, 2)
        
    elif score < -20: # Bearish Setup
        trade_setup["signal"] = "SELL"
        trade_setup["stop_loss"] = resistance + (ltp * 0.001)
        trade_setup["target"] = support - (ltp * 0.002)
        risk = trade_setup["stop_loss"] - ltp
        reward = ltp - trade_setup["target"]
        if risk > 0: trade_setup["rr_ratio"] = round(reward / risk, 2)

    # 4. Market Regime
    # Volatility based on recent price range
    regime = "Sideways / Low Conviction"
    if abs(score) > 50:
        regime = "One-Sided Trend"
    elif panics or (greeks and greeks[0].get('iv_velocity', 0) > 0.0005):
        regime = "High Volatility / Choppy"

    return {
        "sentiment": sentiment,
        "sentiment_score": round(score, 2),
        "components": {
            "pattern_score": pattern_score,
            "whale_score": whale_score,
            "imbalance_score": round(avg_ratio * 50, 2) if imbalances else 0
        },
        "support_resistance": {
            "support": round(support, 2),
            "resistance": round(resistance, 2),
            "current_price": ltp
        },
        "trade_setup": trade_setup,
        "market_regime": regime,
        "key_insights": insights[:5] # Top 5
    }
