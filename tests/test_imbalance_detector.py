import sys
import os
import numpy as np

# Add app to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.analytics.imbalance_detector import analyze_order_book_imbalance

def test_imbalance_detector():
    # Scenario 1: Bullish (Bid Heavy)
    # TBQ = 2000, TSQ = 1000
    # Ratio = (2000 - 1000) / 3000 = 0.33 (> 0.2)
    arrays_bullish = {
        'tbq': np.array([2000], dtype=np.int64),
        'tsq': np.array([1000], dtype=np.int64),
        'ltp': np.array([100.0], dtype=np.float32)
    }
    result = analyze_order_book_imbalance(arrays_bullish)
    print(f"\nTest 1 (Bullish): {result}")
    assert result['signal'] == "Bullish (Bid Heavy)"
    assert result['imbalance_ratio'] > 0.2

    # Scenario 2: Bearish (Ask Heavy)
    # TBQ = 1000, TSQ = 2000
    # Ratio = (1000 - 2000) / 3000 = -0.33 (< -0.2)
    arrays_bearish = {
        'tbq': np.array([1000], dtype=np.int64),
        'tsq': np.array([2000], dtype=np.int64),
        'ltp': np.array([100.0], dtype=np.float32)
    }
    result = analyze_order_book_imbalance(arrays_bearish)
    print(f"Test 2 (Bearish): {result}")
    assert result['signal'] == "Bearish (Ask Heavy)"
    assert result['imbalance_ratio'] < -0.2

    # Scenario 3: Neutral
    # TBQ = 1000, TSQ = 1000
    # Ratio = 0
    arrays_neutral = {
        'tbq': np.array([1000], dtype=np.int64),
        'tsq': np.array([1000], dtype=np.int64),
        'ltp': np.array([100.0], dtype=np.float32)
    }
    result = analyze_order_book_imbalance(arrays_neutral)
    print(f"Test 3 (Neutral): {result}")
    assert result['signal'] == "Neutral"
    assert result['imbalance_ratio'] == 0.0

if __name__ == "__main__":
    try:
        test_imbalance_detector()
        print("\n✅ All tests passed!")
    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
    except Exception as e:
        print(f"\n❌ An error occurred: {e}")
