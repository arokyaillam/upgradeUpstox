import sys
import os
import numpy as np

# Add app to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.analytics.greeks_analyzer import analyze_greeks_momentum

def test_greeks_analyzer():
    # Scenario 1: Explosive Bullish
    # Delta Velocity High (+0.002)
    # Gamma Acceleration High (+0.0002)
    # IV Velocity High (+0.001)
    
    arrays_explosive = {
        'delta': np.array([0.5, 0.62]), # +0.12 in 60s = 0.002/s
        'gamma': np.array([0.01, 0.022]), # +0.012 in 60s = 0.0002/s
        'iv': np.array([20.0, 20.06]), # +0.06 in 60s = 0.001/s
        'theta': np.array([-10.0, -10.0])
    }
    
    result = analyze_greeks_momentum(arrays_explosive)
    print(f"\nTest 1 (Explosive Bullish):")
    print(f"Score: {result['momentum_score']}")
    print(f"Type: {result['momentum_type']}")
    print(f"Signal: {result['signal']}")
    
    assert result['momentum_type'] == "Explosive Bullish"
    assert result['signal'] == "STRONG BUY"
    assert result['momentum_score'] >= 80

    # Scenario 2: Neutral
    arrays_neutral = {
        'delta': np.array([0.5, 0.5]),
        'gamma': np.array([0.01, 0.01]),
        'iv': np.array([20.0, 20.0]),
        'theta': np.array([-10.0, -10.0])
    }
    
    result = analyze_greeks_momentum(arrays_neutral)
    print(f"\nTest 2 (Neutral):")
    print(f"Score: {result['momentum_score']}")
    print(f"Type: {result['momentum_type']}")
    
    assert result['momentum_type'] == "Neutral"
    assert result['momentum_score'] == 50.0

if __name__ == "__main__":
    try:
        test_greeks_analyzer()
        print("\n✅ All tests passed!")
    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
    except Exception as e:
        print(f"\n❌ An error occurred: {e}")
