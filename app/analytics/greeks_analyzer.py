import numpy as np
from typing import Dict, Any

def analyze_greeks_momentum(arrays: Dict[str, np.ndarray], duration_seconds: int = 60) -> Dict[str, Any]:
    """
    Analyze Greeks Momentum (Velocity & Acceleration).
    
    Metrics:
    1. Delta Velocity (Change in Delta per second)
    2. Gamma Acceleration (Change in Gamma per second - Risk Explosion)
    3. IV Velocity (Change in IV per second)
    4. Theta Acceleration (Change in Theta per second)
    
    Thresholds (per second):
    - HIGH_DELTA_VELOCITY = 0.001
    - HIGH_GAMMA_SPIKE = 0.0001
    - HIGH_IV_VELOCITY = 0.0005
    """
    
    # Defaults
    result = {
        'delta_velocity': 0.0,
        'gamma_acceleration': 0.0,
        'iv_velocity': 0.0,
        'theta_acceleration': 0.0,
        'momentum_score': 0.0,
        'momentum_type': 'Neutral',
        'signal': 'WAIT'
    }
    
    if len(arrays['delta']) < 2:
        return result

    # 1. Calculate Velocities (Change per second)
    # Assuming arrays cover 'duration_seconds'
    
    # Delta Velocity
    delta_start = float(arrays['delta'][0])
    delta_end = float(arrays['delta'][-1])
    delta_velocity = (delta_end - delta_start) / duration_seconds
    
    # Gamma Acceleration (Change in Gamma)
    gamma_start = float(arrays['gamma'][0])
    gamma_end = float(arrays['gamma'][-1])
    gamma_acceleration = (gamma_end - gamma_start) / duration_seconds
    
    # IV Velocity
    iv_start = float(arrays['iv'][0])
    iv_end = float(arrays['iv'][-1])
    iv_velocity = (iv_end - iv_start) / duration_seconds
    
    # Theta Acceleration
    theta_start = float(arrays['theta'][0])
    theta_end = float(arrays['theta'][-1])
    theta_acceleration = (theta_end - theta_start) / duration_seconds
    
    # 2. Scoring Logic (0-100)
    score = 50.0 # Start at Neutral
    
    # Thresholds
    HIGH_DELTA_VELOCITY = 0.001
    HIGH_GAMMA_SPIKE = 0.0001
    HIGH_IV_VELOCITY = 0.0005
    
    # Delta Contribution (+/- 20)
    if abs(delta_velocity) >= HIGH_DELTA_VELOCITY:
        score += 20.0 * np.sign(delta_velocity)
    else:
        score += (delta_velocity / HIGH_DELTA_VELOCITY) * 20.0
        
    # Gamma Contribution (+/- 15) - Gamma usually positive for long options, but here we track change
    # Rising Gamma is risky/explosive
    if abs(gamma_acceleration) >= HIGH_GAMMA_SPIKE:
        score += 15.0 * np.sign(gamma_acceleration)
    else:
        score += (gamma_acceleration / HIGH_GAMMA_SPIKE) * 15.0
        
    # IV Contribution (+/- 10)
    if abs(iv_velocity) >= HIGH_IV_VELOCITY:
        score += 10.0 * np.sign(iv_velocity)
        
    # Clamp Score
    score = max(0.0, min(100.0, score))
    
    # 3. Determine Momentum Type & Signal
    momentum_type = "Neutral"
    signal = "WAIT"
    
    if score >= 80:
        momentum_type = "Explosive Bullish"
        signal = "STRONG BUY"
    elif score >= 65:
        momentum_type = "Strong Bullish"
        signal = "BUY"
    elif score >= 55:
        momentum_type = "Moderate Bullish"
        signal = "BUY/WAIT"
    elif score <= 20:
        momentum_type = "Explosive Bearish"
        signal = "STRONG SELL"
    elif score <= 35:
        momentum_type = "Strong Bearish"
        signal = "SELL"
    elif score <= 45:
        momentum_type = "Moderate Bearish"
        signal = "SELL/WAIT"
        
    return {
        'delta_velocity': delta_velocity,
        'gamma_acceleration': gamma_acceleration,
        'iv_velocity': iv_velocity,
        'theta_acceleration': theta_acceleration,
        'momentum_score': score,
        'momentum_type': momentum_type,
        'signal': signal
    }
