import sys
import os
import numpy as np

# Add app to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.analytics.pattern_detector import analyze_oi_pattern

def test_pattern_detector():
    # Base arrays
    base_arrays = {
        'ltp': np.array([100.0, 105.0]), # Price Up
        'oi': np.array([1000, 2000]),    # OI Up (+1000)
        'volume': np.array([1000, 5000]), # Vol Change 4000
    }
    
    # Test 1: Long Buildup
    # Price Up (100->105), OI Up (1000->2000), Vol High (4000)
    result = analyze_oi_pattern(base_arrays, oi_threshold=500, volume_threshold=1000)
    print(f"Test 1 (Long Buildup): {result}")
    assert result['pattern'] == 'Long Buildup'
    assert result['signal'] == 'Bullish'

    # Test 2: Short Buildup
    # Price Down, OI Up, Vol High
    arrays = base_arrays.copy()
    arrays['ltp'] = np.array([100.0, 95.0])
    result = analyze_oi_pattern(arrays, oi_threshold=500, volume_threshold=1000)
    print(f"Test 2 (Short Buildup): {result}")
    assert result['pattern'] == 'Short Buildup'
    assert result['signal'] == 'Bearish'

    # Test 3: Short Covering
    # Price Up, OI Down
    arrays = base_arrays.copy()
    arrays['ltp'] = np.array([100.0, 105.0])
    arrays['oi'] = np.array([2000, 1000]) # OI Down (-1000)
    result = analyze_oi_pattern(arrays, oi_threshold=500, volume_threshold=1000)
    print(f"Test 3 (Short Covering): {result}")
    assert result['pattern'] == 'Short Covering'
    assert result['signal'] == '🔥 Very Bullish'

    # Test 4: Long Unwinding
    # Price Down, OI Down
    arrays = base_arrays.copy()
    arrays['ltp'] = np.array([100.0, 95.0])
    arrays['oi'] = np.array([2000, 1000]) # OI Down (-1000)
    result = analyze_oi_pattern(arrays, oi_threshold=500, volume_threshold=1000)
    print(f"Test 4 (Long Unwinding): {result}")
    assert result['pattern'] == 'Long Unwinding'
    assert result['signal'] == 'Bearish'

    # Test 5: Low Volume
    arrays = base_arrays.copy()
    arrays['volume'] = np.array([1000, 1500]) # Vol Change 500 < 1000
    result = analyze_oi_pattern(arrays, oi_threshold=500, volume_threshold=1000)
    print(f"Test 5 (Low Volume): {result}")
    assert result['pattern'] == 'Low Volume'
    assert result['signal'] == 'Skip'

    # Test 6: Neutral (Churn)
    arrays = base_arrays.copy()
    arrays['volume'] = np.array([1000, 5000]) # High Vol
import sys
import os
import numpy as np

# Add app to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.analytics.pattern_detector import analyze_oi_pattern

def test_pattern_detector():
    # Base arrays
    base_arrays = {
        'ltp': np.array([100.0, 105.0]), # Price Up
        'oi': np.array([1000, 2000]),    # OI Up (+1000)
        'volume': np.array([1000, 5000]), # Vol Change 4000
    }
    
    # Test 1: Long Buildup
    # Price Up (100->105), OI Up (1000->2000), Vol High (4000)
    result = analyze_oi_pattern(base_arrays, oi_threshold=500, volume_threshold=1000)
    print(f"Test 1 (Long Buildup): {result}")
    assert result['pattern'] == 'Long Buildup'
    assert result['signal'] == 'Bullish'

    # Test 2: Short Buildup
    # Price Down, OI Up, Vol High
    arrays = base_arrays.copy()
    arrays['ltp'] = np.array([100.0, 95.0])
    result = analyze_oi_pattern(arrays, oi_threshold=500, volume_threshold=1000)
    print(f"Test 2 (Short Buildup): {result}")
    assert result['pattern'] == 'Short Buildup'
    assert result['signal'] == 'Bearish'

    # Test 3: Short Covering
    # Price Up, OI Down
    arrays = base_arrays.copy()
    arrays['ltp'] = np.array([100.0, 105.0])
    arrays['oi'] = np.array([2000, 1000]) # OI Down (-1000)
    result = analyze_oi_pattern(arrays, oi_threshold=500, volume_threshold=1000)
    print(f"Test 3 (Short Covering): {result}")
    assert result['pattern'] == 'Short Covering'
    assert result['signal'] == '🔥 Very Bullish'

    # Test 4: Long Unwinding
    # Price Down, OI Down
    arrays = base_arrays.copy()
    arrays['ltp'] = np.array([100.0, 95.0])
    arrays['oi'] = np.array([2000, 1000]) # OI Down (-1000)
    result = analyze_oi_pattern(arrays, oi_threshold=500, volume_threshold=1000)
    print(f"Test 4 (Long Unwinding): {result}")
    assert result['pattern'] == 'Long Unwinding'
    assert result['signal'] == 'Bearish'

    # Test 5: Low Volume
    arrays = base_arrays.copy()
    arrays['volume'] = np.array([1000, 1500]) # Vol Change 500 < 1000
    result = analyze_oi_pattern(arrays, oi_threshold=500, volume_threshold=1000)
    print(f"Test 5 (Low Volume): {result}")
    assert result['pattern'] == 'Low Volume'
    assert result['signal'] == 'Skip'

    # Test 6: Neutral (Churn)
    arrays = base_arrays.copy()
    arrays['volume'] = np.array([1000, 5000]) # High Vol
    arrays['oi'] = np.array([1000, 1200]) # OI Change 200 < 500
    result = analyze_oi_pattern(arrays, oi_threshold=500, volume_threshold=1000)
    print(f"Test 6 (Neutral): {result}")
    assert result['pattern'] == 'Neutral'
    assert result['signal'] == 'Churn'

def test_panic_detection():
    """Test Panic Detection (Short Covering with high intensity)."""
    # Scenario:
    # OI drops significantly (-6000)
    # Price jumps significantly (+2.0%)
    # Volume is high (3000)
    
    arrays = {
        'ltp': np.array([100.0, 102.0], dtype=np.float32), # 2% jump
        'oi': np.array([10000, 4000], dtype=np.int64),     # -6000 drop
        'volume': np.array([1000, 4000], dtype=np.int64),  # 3000 change
        'delta': np.array([0.5, 0.6], dtype=np.float32)
    }
    
    result = analyze_oi_pattern(arrays)
    
    print("\nTest: Panic Detection")
    print(f"Pattern: {result['pattern']}")
    print(f"Signal: {result['signal']}")
    print(f"Is Panic: {result['is_panic']}")
    
    assert result['pattern'] == "Panic (Short Covering)"
    assert result['signal'] == "🚀 PANIC BUY"
    assert result['is_panic'] is True

if __name__ == "__main__":
    try:
        test_pattern_detector()
        test_panic_detection()
        print("\nAll tests passed!")
    except AssertionError as e:
        print(f"\nTest failed: {e}")
    except Exception as e:
        print(f"\nAn error occurred: {e}")
