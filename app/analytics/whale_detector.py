import numpy as np
from typing import Dict, List, Any

def analyze_whale_activity(arrays: Dict[str, np.ndarray]) -> List[Dict[str, Any]]:
    """
    Detect Whale Activity (OI Jumps, Volume Spikes, Order Walls).
    
    Returns a list of alerts found in the current window.
    """
    alerts = []
    
    # Thresholds
    MEGA_WHALE_THRESHOLD = 20000
    LARGE_WHALE_THRESHOLD = 10000
    MEDIUM_WHALE_THRESHOLD = 5000
    SMALL_WHALE_THRESHOLD = 2000
    
    VOLUME_SPIKE_MULTIPLIER = 10
    ORDER_WALL_THRESHOLD = 50000
    
    # 1. OI Jumps (Tick-by-Tick)
    if len(arrays['oi']) >= 2:
        oi_diffs = np.diff(arrays['oi'])
        max_oi_jump = np.max(oi_diffs) if len(oi_diffs) > 0 else 0
        min_oi_drop = np.min(oi_diffs) if len(oi_diffs) > 0 else 0
        
        # Check for Bullish Whale (OI Jump)
        if max_oi_jump >= SMALL_WHALE_THRESHOLD:
            whale_type = "Small Whale"
            if max_oi_jump >= MEGA_WHALE_THRESHOLD: whale_type = "Mega Whale"
            elif max_oi_jump >= LARGE_WHALE_THRESHOLD: whale_type = "Large Whale"
            elif max_oi_jump >= MEDIUM_WHALE_THRESHOLD: whale_type = "Medium Whale"
            
            alerts.append({
                'whale_type': whale_type,
                'alert_type': 'OI Jump',
                'alert_value': float(max_oi_jump),
                'signal': 'Bullish'
            })
            
        # Check for Bearish Whale (OI Drop - Unwinding)
        if min_oi_drop <= -SMALL_WHALE_THRESHOLD:
            whale_type = "Small Whale"
            if min_oi_drop <= -MEGA_WHALE_THRESHOLD: whale_type = "Mega Whale"
            elif min_oi_drop <= -LARGE_WHALE_THRESHOLD: whale_type = "Large Whale"
            elif min_oi_drop <= -MEDIUM_WHALE_THRESHOLD: whale_type = "Medium Whale"
            
            alerts.append({
                'whale_type': whale_type,
                'alert_type': 'OI Drop',
                'alert_value': float(min_oi_drop),
                'signal': 'Bearish' # Or Bullish if Short Covering? Context matters, but large exit is significant.
            })

    # 2. Volume Spikes
    # Compare max single-tick volume to average
    if len(arrays['volume']) >= 2:
        # Volume is cumulative, so we need diffs
        vol_diffs = np.diff(arrays['volume'])
        if len(vol_diffs) > 0:
            avg_vol = np.mean(vol_diffs)
            max_vol = np.max(vol_diffs)
            
            if avg_vol > 0 and max_vol > (avg_vol * VOLUME_SPIKE_MULTIPLIER) and max_vol > 1000:
                alerts.append({
                    'whale_type': 'Volume Whale',
                    'alert_type': 'Volume Spike',
                    'alert_value': float(max_vol),
                    'signal': 'Neutral' # Needs price context
                })

    # 3. Order Walls (Depth)
    # Check max bid/ask qty at any level
    if arrays['bid_qtys'].size > 0:
        max_bid_wall = np.max(arrays['bid_qtys'])
        if max_bid_wall >= ORDER_WALL_THRESHOLD:
            alerts.append({
                'whale_type': 'Limit Whale',
                'alert_type': 'Bid Wall',
                'alert_value': float(max_bid_wall),
                'signal': 'Bullish Support'
            })
            
    if arrays['ask_qtys'].size > 0:
        max_ask_wall = np.max(arrays['ask_qtys'])
        if max_ask_wall >= ORDER_WALL_THRESHOLD:
            alerts.append({
                'whale_type': 'Limit Whale',
                'alert_type': 'Ask Wall',
                'alert_value': float(max_ask_wall),
                'signal': 'Bearish Resistance'
            })
            
    return alerts
