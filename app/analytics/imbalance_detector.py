import numpy as np
from typing import Dict, Any

def analyze_order_book_imbalance(arrays: Dict[str, np.ndarray]) -> Dict[str, Any]:
    """
    Analyze Total Buy Quantity (TBQ) vs Total Sell Quantity (TSQ).
    
    Formula:
    Ratio = (TBQ - TSQ) / (TBQ + TSQ)
    
    Range: -1.0 (All Sell) to +1.0 (All Buy)
    
    Signals:
    - Ratio > 0.2: Bullish (Bid Heavy)
    - Ratio < -0.2: Bearish (Ask Heavy)
    - Else: Neutral
    """
    if len(arrays['tbq']) == 0 or len(arrays['tsq']) == 0:
        return {
            'signal': 'Insufficient Data',
            'imbalance_ratio': 0.0,
            'tbq': 0,
            'tsq': 0,
            'ltp': 0.0
        }

    # Get latest values
    tbq = int(arrays['tbq'][-1])
    tsq = int(arrays['tsq'][-1])
    ltp = float(arrays['ltp'][-1]) if len(arrays['ltp']) > 0 else 0.0
    
    total_qty = tbq + tsq
    
    if total_qty == 0:
        return {
            'signal': 'Neutral',
            'imbalance_ratio': 0.0,
            'tbq': 0,
            'tsq': 0,
            'ltp': ltp
        }
        
    imbalance_ratio = (tbq - tsq) / total_qty
    
    signal = "Neutral"
    if imbalance_ratio > 0.2:
        signal = "Bullish (Bid Heavy)"
    elif imbalance_ratio < -0.2:
        signal = "Bearish (Ask Heavy)"
        
    return {
        'signal': signal,
        'imbalance_ratio': imbalance_ratio,
        'tbq': tbq,
        'tsq': tsq,
        'ltp': ltp
    }
