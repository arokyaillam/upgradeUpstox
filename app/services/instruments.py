"""
Smart Option Mapper - Dynamic Instrument Key Fetcher
Uses Upstox Option Chain API to fetch instrument keys for option strikes.

Financial Logic:
- Fetches option chain data for a given underlying (e.g., Nifty 50)
- Identifies key strike prices around ATM (At-The-Money)
- Extracts both CE (Call) and PE (Put) instrument keys
- Eliminates need for heavy CSV master file downloads
"""

import os
from typing import Dict, List, Tuple, Optional
from datetime import datetime
import requests
from dotenv import load_dotenv

load_dotenv()


class SmartOptionMapper:
    """
    Dynamically fetch option instrument keys using Upstox Option Chain API.
    
    Financial Context:
    - Center Strike: ATM (At-The-Money) strike price
    - ATM ± 50/100: Near-the-money strikes for hedging strategies
    - CE: Call Option (bullish bet)
    - PE: Put Option (bearish bet)
    """
    
    def __init__(self, access_token: str):
        """
        Initialize the Option Mapper with Upstox API client.
        
        Args:
            access_token: Upstox API access token
        """
        self.access_token = access_token
    
    def fetch_nifty_keys(
        self,
        expiry_date: str,
        center_strike: int
    ) -> Tuple[List[str], Dict[int, Dict[str, str]]]:
        """
        Fetch Nifty option instrument keys for multiple strikes.
        
        Financial Logic:
        - Center Strike: ATM strike (e.g., 24000)
        - We fetch 5 strikes: ATM, ATM+50, ATM+100, ATM-50, ATM-100
        - Each strike has both CE and PE options
        - Total: 10 instrument keys (5 strikes × 2 option types)
        
        Args:
            expiry_date: Option expiry date in 'YYYY-MM-DD' format (e.g., '2025-11-27')
            center_strike: Center/ATM strike price (e.g., 24000)
        
        Returns:
            Tuple containing:
            - List[str]: Flat list of instrument keys for ingestion
                        ['NSE_FO|12345', 'NSE_FO|12346', ...]
            - Dict[int, Dict[str, str]]: Structured dict for dashboard
                        {24000: {'CE': 'NSE_FO|12345', 'PE': 'NSE_FO|12346'}, ...}
        """
        import requests
        
        # Define strikes to fetch (Center ± 0, 50, 100)
        strikes_to_fetch = [
            center_strike - 100,  # ITM Put / OTM Call
            center_strike - 50,   # Slightly ITM Put / OTM Call
            center_strike,        # ATM (At-The-Money)
            center_strike + 50,   # Slightly OTM Put / ITM Call
            center_strike + 100,  # OTM Put / ITM Call
        ]
        
        print(f"🎯 Fetching Nifty options for expiry: {expiry_date}")
        print(f"📊 Center Strike: {center_strike}")
        print(f"🎲 Target Strikes: {strikes_to_fetch}\n")
        
        try:
            # Call Upstox Option Chain API (REST endpoint)
            instrument_key = "NSE_INDEX|Nifty 50"
            url = "https://api.upstox.com/v2/option/chain"
            
            headers = {
                'Accept': 'application/json',
                'Authorization': f'Bearer {self.access_token}'
            }
            
            params = {
                'instrument_key': instrument_key,
                'expiry_date': expiry_date
            }
            
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            # Parse option chain data
            option_chain_data = data.get('data', [])
            
            # Process option chain
            keys_list = []
            keys_dict = {}
            
            for strike_price in strikes_to_fetch:
                strike_data = self._find_strike_in_chain(
                    option_chain_data,
                    strike_price
                )
                
                if strike_data:
                    ce_key = strike_data.get('CE')
                    pe_key = strike_data.get('PE')
                    
                    # Add to dict (for dashboard)
                    keys_dict[strike_price] = {
                        'CE': ce_key,
                        'PE': pe_key
                    }
                    
                    # Add to list (for ingestion)
                    if ce_key:
                        keys_list.append(ce_key)
                    if pe_key:
                        keys_list.append(pe_key)
                    
                    print(f"✓ Strike {strike_price:,}: CE={ce_key}, PE={pe_key}")
                else:
                    print(f"✗ Strike {strike_price:,}: Not found in option chain")
            
            print(f"\n✅ Fetched {len(keys_list)} instrument keys")
            return keys_list, keys_dict
        
        except requests.exceptions.HTTPError as e:
            print(f"❌ HTTP Error: {e}")
            print(f"   Status Code: {e.response.status_code}")
            print(f"   Response: {e.response.text[:200]}")
            raise
        except Exception as e:
            print(f"❌ Error fetching option chain: {e}")
            raise
    
    def _find_strike_in_chain(
        self,
        option_chain_data: List[Dict],
        strike_price: int
    ) -> Optional[Dict]:
        """
        Find specific strike price in option chain response.
        
        Args:
            option_chain_data: List of option chain data from API
            strike_price: Strike price to search for
        
        Returns:
            Dictionary with 'CE' and 'PE' instrument keys, or None
        """
        # Option chain data structure from Upstox API
        # Response format: [{strike_price: 24000, call_options: {...}, put_options: {...}}, ...]
        
        for item in option_chain_data:
            if item.get('strike_price') == strike_price:
                # Extract CE and PE instrument keys
                result = {}
                
                call_options = item.get('call_options', {})
                put_options = item.get('put_options', {})
                
                result['CE'] = call_options.get('instrument_key')
                result['PE'] = put_options.get('instrument_key')  # Fixed: removed extra space
                
                return result
        
        return None
    
    def get_weekly_expiry(self) -> str:
        """
        Get the nearest weekly expiry date for Nifty.
        
        Financial Context:
        - Nifty has weekly expiries every Thursday
        - If today is Thursday after 3:30 PM, return next Thursday
        
        Returns:
            Expiry date in 'YYYY-MM-DD' format
        """
        from datetime import datetime, timedelta
        
        today = datetime.now()
        
        # Thursday is weekday 3 (0=Monday)
        days_until_thursday = (3 - today.weekday()) % 7
        
        if days_until_thursday == 0:
            # Today is Thursday, check if market is closed
            if today.hour >= 15 and today.minute >= 30:
                # After 3:30 PM, use next Thursday
                days_until_thursday = 7
        
        next_thursday = today + timedelta(days=days_until_thursday)
        return next_thursday.strftime('%Y-%m-%d')


# ============================================================================
# TEST / EXECUTION BLOCK
# ============================================================================

if __name__ == "__main__":
    """
    Test the SmartOptionMapper with user-provided data.
    
    Usage:
        # With arguments
        uv run app/services/instruments.py --expiry 2025-11-28 --strike 24000
        
        # Interactive (will prompt)
        uv run app/services/instruments.py
    """
    import argparse
    import json
    
    print("=" * 70)
    print("🐋 WHALE HUNTER - Smart Option Mapper Test")
    print("=" * 70)
    print()
    
    # Get access token from environment
    access_token = os.getenv("UPSTOX_ACCESS_TOKEN")
    
    if not access_token or access_token == "your_token_here":
        print("❌ ERROR: UPSTOX_ACCESS_TOKEN not found in .env")
        print("\nPlease add a valid Upstox access token to .env file:")
        print("   UPSTOX_ACCESS_TOKEN=your_token_here")
        print("\nRun: uv run get_token.py (to generate a fresh token)")
        exit(1)
    
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Fetch Nifty option keys')
    parser.add_argument('--expiry', type=str, help='Expiry date (YYYY-MM-DD)')
    parser.add_argument('--strike', type=int, help='Center strike price')
    parser.add_argument('--debug', action='store_true', help='Show API response')
    args = parser.parse_args()
    
    # Get expiry date
    if args.expiry:
        test_expiry = args.expiry
    else:
        print("📅 Enter Expiry Date (YYYY-MM-DD):")
        print("   Examples: 2025-11-28 (weekly), 2025-12-26 (monthly)")
        test_expiry = input("   Expiry: ").strip() or "2025-11-28"
    
    # Get strike price
    if args.strike:
        test_strike = args.strike
    else:
        print("\n🎯 Enter Center/ATM Strike Price:")
        print("   Current Nifty ~23,700-24,500 (check market)")
        strike_input = input("   Strike: ").strip() or "24000"
        test_strike = int(strike_input)
    
    print()
    print(f"📅 Expiry: {test_expiry}")
    print(f"🎯 Strike: {test_strike:,}")
    print()
    
    # Initialize mapper
    mapper = SmartOptionMapper(access_token)
    
    # DEBUG: First, let's see what the API actually returns
    if args.debug:
        print("🔍 DEBUG MODE: Fetching raw API response...")
        print("-" * 70)
        
        url = "https://api.upstox.com/v2/option/chain"
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {access_token}'
        }
        params = {
            'instrument_key': "NSE_INDEX|Nifty 50",
            'expiry_date': test_expiry
        }
        
        response = requests.get(url, headers=headers, params=params)
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"\n📦 API Response Structure:")
            print(json.dumps(data, indent=2)[:1000] + "...\n")  # First 1000 chars
            
            # Show first strike data structure
            if 'data' in data and len(data['data']) > 0:
                print(f"📊 First Strike Sample:")
                print(json.dumps(data['data'][0], indent=2))
            else:
                print("⚠️  No data returned or different structure")
        else:
            print(f"❌ Error: {response.text}")
        
        print("-" * 70)
        print()
    
    try:
        # Fetch option keys
        keys_list, keys_dict = mapper.fetch_nifty_keys(
            expiry_date=test_expiry,
            center_strike=test_strike
        )
        
        print("\n" + "=" * 70)
        print("📋 RESULTS")
        print("=" * 70)
        
        if len(keys_list) > 0:
            # Display list format (for ingestion)
            print("\n1️⃣  Instrument Keys List (for Ingestion Service):")
            print("   " + "-" * 66)
            for idx, key in enumerate(keys_list, 1):
                print(f"   {idx:2d}. {key}")
            
            # Display dict format (for dashboard)
            print("\n2️⃣  Structured Dict (for Dashboard):")
            print("   " + "-" * 66)
            for strike, options in sorted(keys_dict.items()):
                print(f"   Strike {strike:,}:")
                print(f"      CE: {options['CE']}")
                print(f"      PE: {options['PE']}")
                print()
        else:
            print("\n⚠️  No instrument keys found!")
            print("\nPossible reasons:")
            print("  1. Invalid expiry date (must be future date)")
            print("  2. Strike prices don't exist for this expiry")
            print("  3. API response structure different than expected")
            print("\n💡 Try running with --debug flag to see raw API response:")
            print("   uv run app/services/instruments.py --debug")
        
        print("=" * 70)
        print("✅ Test completed!")
        print("=" * 70)
    
    except requests.exceptions.HTTPError as e:
        print(f"\n❌ HTTP Error: {e}")
        print(f"   Status: {e.response.status_code}")
        print(f"   Response: {e.response.text[:500]}")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
