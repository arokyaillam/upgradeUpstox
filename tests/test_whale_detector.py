import sys
import os
import numpy as np

# Add app to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.analytics.whale_detector import analyze_whale_activity

def test_whale_detector():
    # Scenario 1: Mega Whale OI Jump
    # OI Jumps by 25000 in one tick
    arrays_oi = {
        'oi': np.array([100000, 125000], dtype=np.int64),
        'volume': np.array([1000, 2000], dtype=np.int64),
        'bid_qtys': np.array([], dtype=np.int64),
        'ask_qtys': np.array([], dtype=np.int64)
    }
    
    alerts = analyze_whale_activity(arrays_oi)
    print(f"\nTest 1 (Mega Whale OI): {alerts}")
    assert len(alerts) == 1
    assert alerts[0]['whale_type'] == "Mega Whale"
    assert alerts[0]['alert_type'] == "OI Jump"
    assert alerts[0]['signal'] == "Bullish"

    # Scenario 2: Bid Wall
    # Bid Qty at a level is 60000
    arrays_wall = {
        'oi': np.array([100000, 100100], dtype=np.int64),
        'volume': np.array([1000, 1200], dtype=np.int64),
        'bid_qtys': np.array([1000, 60000, 500], dtype=np.int64),
        'ask_qtys': np.array([1000, 2000], dtype=np.int64)
    }
    
    alerts = analyze_whale_activity(arrays_wall)
    print(f"\nTest 2 (Bid Wall): {alerts}")
    found_wall = False
    for alert in alerts:
        if alert['alert_type'] == "Bid Wall":
            assert alert['whale_type'] == "Limit Whale"
            assert alert['alert_value'] == 60000.0
            found_wall = True
    assert found_wall

if __name__ == "__main__":
    try:
        test_whale_detector()
        print("\n✅ All tests passed!")
    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
    except Exception as e:
        print(f"\n❌ An error occurred: {e}")
