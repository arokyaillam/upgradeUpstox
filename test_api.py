"""
Test API Connectivity
Quick script to verify API endpoints are accessible.
"""

import requests
import sys

API_BASE_URL = "http://localhost:8001"

def test_endpoint(method, path, description):
    """Test a single endpoint."""
    url = f"{API_BASE_URL}{path}"
    print(f"\n🔍 Testing {method} {path}")
    print(f"   Description: {description}")

    try:
        if method == "GET":
            response = requests.get(url, timeout=5)
        elif method == "POST":
            response = requests.post(url, json={}, timeout=5)

        print(f"   ✅ Status: {response.status_code}")
        if response.status_code == 200:
            print(f"   📄 Response: {response.json()}")
            return True
        else:
            print(f"   ⚠️  Response: {response.text[:100]}")
            return False

    except requests.exceptions.ConnectionError:
        print(f"   ❌ Connection Error: Cannot connect to {API_BASE_URL}")
        print(f"      Is the API server running?")
        print(f"      Run: uvicorn app.api.main:app --port 8001")
        return False

    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False


def main():
    """Run all API tests."""
    print("=" * 60)
    print("🔬 API CONNECTIVITY TEST")
    print("=" * 60)

    tests = [
        ("GET", "/", "Root endpoint"),
        ("GET", "/health", "Health check"),
        ("GET", "/dashboard/history", "Dashboard history data"),
    ]

    results = []
    for method, path, description in tests:
        result = test_endpoint(method, path, description)
        results.append(result)

    # Summary
    print("\n" + "=" * 60)
    print("📊 TEST SUMMARY")
    print("=" * 60)

    passed = sum(results)
    total = len(results)

    if passed == total:
        print(f"✅ All tests passed ({passed}/{total})")
        print("\n🎉 API is running correctly!")
        print(f"📖 View API docs at: {API_BASE_URL}/docs")
        return 0
    else:
        print(f"❌ {total - passed} tests failed ({passed}/{total} passed)")
        print("\n⚠️  API server may not be running or configured correctly")
        print(f"\n💡 To start API server:")
        print(f"   cd upgradeUpstox")
        print(f"   uv run uvicorn app.api.main:app --host 0.0.0.0 --port 8001")
        return 1


if __name__ == "__main__":
    sys.exit(main())
