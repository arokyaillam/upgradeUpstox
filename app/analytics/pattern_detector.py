import numpy as np
from typing import Dict, Any

def analyze_oi_pattern(arrays: Dict[str, np.ndarray], oi_threshold: int = 500, volume_threshold: int = 1000) -> Dict[str, Any]:
    """
    Analyze OI, Volume, and Price patterns.
    
    Patterns:
    - Long Buildup: Price Up, OI Up
    - Short Buildup: Price Down, OI Up
    - Short Covering: Price Up, OI Down
    - Long Unwinding: Price Down, OI Down
    
    Args:
        arrays: Dictionary of NumPy arrays from get_arrays
        oi_threshold: Minimum OI change to be considered significant
        volume_threshold: Minimum volume to be considered significant
        
    Returns:
        Dictionary containing pattern, signal, and metrics.
    """
    if len(arrays['ltp']) < 2:
        return {
            'pattern': 'Insufficient Data',
            'signal': 'Neutral',
            'price_change': 0.0,
            'oi_change': 0,
            'volume_change': 0
        }

    # Calculate changes
    # Use first and last valid elements
    price_change = float(arrays['ltp'][-1] - arrays['ltp'][0])
    oi_change = int(arrays['oi'][-1] - arrays['oi'][0])
    
    # Volume is cumulative (vtt), so change is last - first
    # If volume resets or is per-tick, this logic might need adjustment, 
    # but assuming standard cumulative volume for the day.
    volume_change = int(arrays['volume'][-1] - arrays['volume'][0])
    
    # Determine Pattern
    pattern = "Neutral"
    signal = "Churn"
    
    # 1. Low Volume Check
    if volume_change < volume_threshold:
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
        'price_change': price_change,
        'oi_change': oi_change,
        'volume_change': volume_change
    }
