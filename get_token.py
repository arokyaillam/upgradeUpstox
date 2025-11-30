"""
Upstox OAuth Token Generator
Generates a fresh access token using OAuth 2.0 flow.

Run this script to get a new access token when your current one expires.
"""

import os
from dotenv import load_dotenv
import webbrowser
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import requests

load_dotenv()

# OAuth configuration
API_KEY = os.getenv("UPSTOX_API_KEY")
API_SECRET = os.getenv("UPSTOX_API_SECRET")
REDIRECT_URI = os.getenv("UPSTOX_REDIRECT_URI", "http://localhost:8000/callback")

# Global variable to store auth code
auth_code = None


class CallbackHandler(BaseHTTPRequestHandler):
    """HTTP server to handle OAuth callback."""
    
    def do_GET(self):
        global auth_code
        
        # Parse the callback URL
        parsed_url = urlparse(self.path)
        
        if parsed_url.path == "/callback":
            # Extract auth code from query parameters
            params = parse_qs(parsed_url.query)
            auth_code = params.get('code', [None])[0]
            
            # Send response to browser
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            
            if auth_code:
                response_html = """
                <html>
                <body style="font-family: Arial; text-align: center; padding: 50px;">
                    <h1 style="color: green;">✓ Authorization Successful!</h1>
                    <p>You can close this window and return to the terminal.</p>
                </body>
                </html>
                """
                self.wfile.write(response_html.encode('utf-8'))
            else:
                response_html = """
                <html>
                <body style="font-family: Arial; text-align: center; padding: 50px;">
                    <h1 style="color: red;">✗ Authorization Failed</h1>
                    <p>No authorization code received.</p>
                </body>
                </html>
                """
                self.wfile.write(response_html.encode('utf-8'))
    
    def log_message(self, format, *args):
        # Suppress default logging
        pass


def get_authorization_code():
    """Step 1: Get authorization code via browser."""
    global auth_code
    
    # Build authorization URL
    auth_url = (
        f"https://api.upstox.com/v2/login/authorization/dialog"
        f"?client_id={API_KEY}"
        f"&redirect_uri={REDIRECT_URI}"
        f"&response_type=code"
    )
    
    print("=" * 60)
    print("🔐 UPSTOX OAUTH FLOW")
    print("=" * 60)
    print("\n1️⃣  Opening browser for authorization...")
    print(f"   URL: {auth_url}\n")
    
    # Open browser
    webbrowser.open(auth_url)
    
    # Start local server to receive callback
    print("2️⃣  Waiting for callback on http://localhost:8000...")
    server = HTTPServer(('localhost', 8000), CallbackHandler)
    
    # Wait for one request (the callback)
    while auth_code is None:
        server.handle_request()
    
    print(f"✓ Received authorization code: {auth_code[:20]}...\n")
    return auth_code


def exchange_code_for_token(code):
    """Step 2: Exchange authorization code for access token."""
    print("3️⃣  Exchanging code for access token...")
    
    url = "https://api.upstox.com/v2/login/authorization/token"
    
    data = {
        'code': code,
        'client_id': API_KEY,
        'client_secret': API_SECRET,
        'redirect_uri': REDIRECT_URI,
        'grant_type': 'authorization_code'
    }
    
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    
    try:
        response = requests.post(url, data=data, headers=headers)
        response.raise_for_status()
        token_data = response.json()
        
        access_token = token_data.get('access_token')
        print(f"✓ Access token received!\n")
        
        return access_token
    
    except Exception as e:
        print(f"❌ Error getting token: {e}")
        return None


def update_env_file(token):
    """Step 3: Update .env file with new token."""
    print("4️⃣  Updating .env file...")
    
    env_path = ".env"
    
    # Read current .env
    with open(env_path, 'r') as f:
        lines = f.readlines()
    
    # Update UPSTOX_ACCESS_TOKEN line
    updated = False
    for i, line in enumerate(lines):
        if line.startswith('UPSTOX_ACCESS_TOKEN='):
            lines[i] = f'UPSTOX_ACCESS_TOKEN={token}\n'
            updated = True
            break
    
    # If not found, add it
    if not updated:
        lines.append(f'\nUPSTOX_ACCESS_TOKEN={token}\n')
    
    # Write back
    with open(env_path, 'w') as f:
        f.writelines(lines)
    
    print(f"✓ Updated .env file\n")


def main():
    """Main execution."""
    if not API_KEY or API_KEY == "your_api_key_here":
        print("❌ ERROR: UPSTOX_API_KEY not configured in .env")
        print("\nPlease add your Upstox API credentials to .env:")
        print("  UPSTOX_API_KEY=your_api_key")
        print("  UPSTOX_API_SECRET=your_api_secret")
        return
    
    if not API_SECRET or API_SECRET == "your_api_secret_here":
        print("❌ ERROR: UPSTOX_API_SECRET not configured in .env")
        return
    
    try:
        # Step 1: Get authorization code
        code = get_authorization_code()
        
        if not code:
            print("❌ Failed to get authorization code")
            return
        
        # Step 2: Exchange for access token
        token = exchange_code_for_token(code)
        
        if not token:
            print("❌ Failed to get access token")
            return
        
        # Step 3: Update .env file
        update_env_file(token)
        
        print("=" * 60)
        print("🎉 SUCCESS!")
        print("=" * 60)
        print("\n✅ New access token saved to .env")
        print("✅ You can now run: uv run run_ingestion.py")
        print("\n⚠️  Note: Upstox tokens expire daily at market close.")
        print("   Re-run this script tomorrow before trading hours.\n")
    
    except KeyboardInterrupt:
        print("\n\n⏸️  Process cancelled by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")


if __name__ == "__main__":
    main()
