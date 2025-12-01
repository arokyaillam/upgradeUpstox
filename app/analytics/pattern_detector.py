import numpy as np
from typing import Dict, Any

def analyze_oi_pattern(
    arrays: Dict[str, np.ndarray], 
    oi_threshold: int = 500, 
    volume_threshold: int = 1000,
    min_oi_drop: int = -5000,
    min_price_jump_pct: float = 1.0,
    min_delta_change: float = 0.05,
    min_volume_multiplier: float = 2.0
) -> Dict[str, Any]:
    """
    Analyze OI, Volume, and Price patterns, including Panic Detection.
    """
    if len(arrays['ltp']) < 2:
        return {
            'pattern': 'Insufficient Data',
            'signal': 'Neutral',
            'is_panic': False,
            'price_change': 0.0,
            'oi_change': 0,
            'volume_change': 0,
            'ltp': 0.0,
            'price_change_pct': 0.0,
            'delta_change': 0.0
        }

    # Calculate changes
    ltp_start = float(arrays['ltp'][0])
    ltp_end = float(arrays['ltp'][-1])
    price_change = ltp_end - ltp_start
    
    # Avoid division by zero
    price_change_pct = (price_change / ltp_start * 100) if ltp_start > 0 else 0.0
    
    oi_change = int(arrays['oi'][-1] - arrays['oi'][0])
    volume_change = int(arrays['volume'][-1] - arrays['volume'][0])
    
    # Delta Change (if available)
    delta_change = 0.0
    if 'delta' in arrays and len(arrays['delta']) >= 2:
        delta_change = float(arrays['delta'][-1] - arrays['delta'][0])
    
    # Determine Pattern
    pattern = "Neutral"
    signal = "Churn"
    is_panic = False
    
    # 0. Panic Detection (Highest Priority)
    # Logic: Rapid OI decrease + Sharp price increase + Volume surge
    if (oi_change < min_oi_drop and 
        price_change_pct > min_price_jump_pct and 
        volume_change > (volume_threshold * min_volume_multiplier)):
        
        pattern = "Panic (Short Covering)"
        signal = "🚀 PANIC BUY"
        is_panic = True
        
    # 1. Low Volume Check
    elif volume_change < volume_threshold:
        pattern = "Low Volume"
        signal = "Skip"
    
    # 2. Neutral / Churn Check (Small OI Change)
    elif abs(oi_change) <= oi_threshold:
        pattern = "Neutral"
        signal = "Churn"
        
    # 3. Directional Patterns
    else:
        if price_change > 0:
            if oi_change > oi_threshold:
                pattern = "Long Buildup"
                signal = "Bullish"
            elif oi_change < -oi_threshold:
                pattern = "Short Covering"
                signal = "🔥 Very Bullish"
        elif price_change < 0:
            if oi_change > oi_threshold:
                pattern = "Short Buildup"
                signal = "Bearish"
            elif oi_change < -oi_threshold:
                pattern = "Long Unwinding"
                signal = "Bearish"
                
    return {
        'pattern': pattern,
        'signal': signal,
        'is_panic': is_panic,
        'price_change': price_change,
        'oi_change': oi_change,
        'volume_change': volume_change,
        'ltp': ltp_end,
        'price_change_pct': price_change_pct,
        'delta_change': delta_change
    }
