import sys
import os
import numpy as np

# Add app to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.analytics.sentiment_analyzer import analyze_market_sentiment

def test_sentiment_analyzer():
    # Mock PG Data
    pg_data = {
        'patterns': [
            {'signal': '🔥 Very Bullish'}, # Was 'Short Covering'
            {'signal': 'Bullish'},         # Was 'Long Buildup'
            {'metrics': {'ltp': 100.0}}
        ],
        'panic': [
            {'signal': 'Panic Buy'}       # Bullish (+30)
        ],
        'imbalance': [
            {'imbalance_ratio': 0.4, 'ltp': 100.0} # Bullish (+20)
        ],
        'greeks': [
            {'momentum_score': 85.0}      # Bullish (+14)
        ],
        'whales': [
            {'signal': 'Bullish', 'alert_type': 'OI Jump', 'alert_value': 5000},
            {'signal': 'Bullish', 'alert_type': 'Bid Wall', 'alert_value': 60000} # (+15)
        ]
    }
    
    # Total Score Expectation:
    # Pattern: +20
    # Panic: +30
    # Imbalance: +20
    # Greeks: +14
    # Whales: +30 (2 bullish)
    # Total: ~114 (Clamped to 100)
    
    ltp = 100.0
    result = analyze_market_sentiment(pg_data, ltp)
    
    print(f"\nTest 1 (Extreme Bullish):")
    print(f"Sentiment: {result['sentiment']}")
    print(f"Score: {result['sentiment_score']}")
    print(f"Insights: {result['key_insights']}")
    print(f"Trade Setup: {result['trade_setup']}")
    
    # Allow for small float differences
    assert result['sentiment_score'] >= 99.0, f"Expected 100, got {result['sentiment_score']}"
    assert "Extreme Bullish" in result['sentiment']
    assert result['trade_setup']['signal'] == "BUY"
    
    # Test 2: Bearish Scenario
    pg_data_bearish = {
        'patterns': [{'signal': 'Bearish'}, {'signal': 'Bearish'}], # -20
        'panic': [],
        'imbalance': [{'imbalance_ratio': -0.5, 'ltp': 100.0}], # -25
        'greeks': [{'momentum_score': 30.0}], # (30-50)*0.4 = -8
        'whales': [{'signal': 'Bearish', 'alert_type': 'OI Drop', 'alert_value': 5000}] # -15
    }
    # Total: -20 - 25 - 8 - 15 = -68
    
    result_bearish = analyze_market_sentiment(pg_data_bearish, ltp)
    print(f"\nTest 2 (Bearish):")
    print(f"Sentiment: {result_bearish['sentiment']}")
    print(f"Score: {result_bearish['sentiment_score']}")
    
    assert result_bearish['sentiment_score'] <= -60.0, f"Expected <= -60, got {result_bearish['sentiment_score']}"
    assert "Bearish" in result_bearish['sentiment']

if __name__ == "__main__":
    try:
        test_sentiment_analyzer()
        print("\n✅ All tests passed!")
    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
    except Exception as e:
        print(f"\n❌ An error occurred: {e}")
        import traceback
        traceback.print_exc()
