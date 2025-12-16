"""
TradingView to Dhan Webhook Bridge with Dynamic Security ID Lookup
Credentials from Google Cloud Secret Manager (Vault)
"""

from fastapi import FastAPI, Request, HTTPException
import json
import os
import pandas as pd
import urllib.request
from dhanhq import dhanhq
from datetime import datetime

# Import Google Cloud Secret Manager
try:
    from google.cloud import secretmanager
    HAS_SECRET_MANAGER = True
except ImportError:
    HAS_SECRET_MANAGER = False
    print("⚠️  google-cloud-secret-manager not installed")

app = FastAPI()

def access_secret_version(secret_id, version_id="latest"):
    """
    Fetch a secret version from Google Cloud Secret Manager
    
    Args:
        secret_id: Name of the secret (e.g., 'dhan_secret')
        version_id: Version number or 'latest' (default: 'latest')
    
    Returns:
        Decoded secret value as string
    """
    try:
        if not HAS_SECRET_MANAGER:
            return None
        
        project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "trading-7988088871")
        client = secretmanager.SecretManagerServiceClient()
        
        # Build the resource name of the secret version
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        
        # Access the secret version
        response = client.access_secret_version(request={"name": name})
        
        # Return the decoded payload
        return response.payload.data.decode('UTF-8')
    except Exception as e:
        print(f"❌ Failed to access {secret_id} (version: {version_id}): {type(e).__name__}: {str(e)}")
        return None


def get_secret_from_vault(secret_id, version_id="latest"):
    """Fetch secret from Google Cloud Secret Manager (Vault)"""
    try:
        if not HAS_SECRET_MANAGER:
            print(f"⚠️  Secret Manager library not available for {secret_id}")
            return get_secret_from_env(secret_id)
        
        project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "trading-7988088871")
        print(f"🔐 Attempting to fetch {secret_id} from GCP project: {project_id}")
        print(f"   Secret path: projects/{project_id}/secrets/{secret_id}/versions/{version_id}")
        
        secret_value = access_secret_version(secret_id, version_id)
        
        if secret_value:
            print(f"✅ Successfully fetched {secret_id} (version: {version_id}) from Cloud Secret Manager")
            return secret_value
        else:
            print(f"⚠️  No value returned for {secret_id}")
            return get_secret_from_env(secret_id)
            
    except Exception as e:
        print(f"❌ Failed to fetch {secret_id} from Secret Manager: {type(e).__name__}: {str(e)}")
        print(f"   Falling back to environment variables...")
        return get_secret_from_env(secret_id)

def get_secret_from_env(secret_id):
    """Fallback to environment variables"""
    fallback_map = {
        "dhan_secret": "DHAN_SECRET",
        "dhan_client_id": "CLIENT_ID",
        "dhan_access_token": "ACCESS_TOKEN"
    }
    env_var = fallback_map.get(secret_id, "")
    value = os.getenv(env_var, "")
    if value:
        print(f"✅ Using {secret_id} from environment variable {env_var}")
    else:
        print(f"❌ {secret_id} not found in environment variable {env_var}")
    return value

# Configuration - Load from Cloud Secret Manager (Vault) with fallback to env vars
print("📋 Loading configuration from Cloud Secret Manager...")
CONFIG = {
    "DHAN_SECRET": get_secret_from_vault("dhan_secret", version_id="latest"),
    "CLIENT_ID": get_secret_from_vault("dhan_client_id", version_id="latest"),
    "ACCESS_TOKEN": get_secret_from_vault("dhan_access_token", version_id="1"),  # Using version 1
}

print("\n📊 Configuration Status:")
print(f"   DHAN_SECRET: {'✅ Set' if CONFIG['DHAN_SECRET'] else '❌ Missing'}")
print(f"   CLIENT_ID: {'✅ Set' if CONFIG['CLIENT_ID'] else '❌ Missing'}")
print(f"   ACCESS_TOKEN: {'✅ Set' if CONFIG['ACCESS_TOKEN'] else '❌ Missing'}")

if not all([CONFIG["DHAN_SECRET"], CONFIG["CLIENT_ID"], CONFIG["ACCESS_TOKEN"]]):
    print("\n⚠️  WARNING: Some credentials are missing!")
    print("   In Cloud Run, ensure these secrets exist in Secret Manager:")
    print("   - dhan_secret")
    print("   - dhan_client_id")
    print("   - dhan_access_token")

# Initialize Dhan connection
dhan = None
DHAN_ERROR = None

try:
    if CONFIG["CLIENT_ID"] and CONFIG["ACCESS_TOKEN"]:
        dhan = dhanhq(CONFIG["CLIENT_ID"], CONFIG["ACCESS_TOKEN"])
        print("✅ Dhan connection initialized")
    else:
        DHAN_ERROR = "Missing CLIENT_ID or ACCESS_TOKEN"
        print(f"⚠️  {DHAN_ERROR}")
except Exception as e:
    DHAN_ERROR = str(e)
    print(f"⚠️  Could not initialize Dhan: {DHAN_ERROR}")

# CSV file path for security IDs
SECURITY_CSV_PATH = os.path.join(os.path.dirname(__file__), "security_id_list.csv")
CSV_DOWNLOAD_URL = os.getenv("CSV_DOWNLOAD_URL", "")

# Cache to store CSV data
csv_cache = {"data": None, "error": None, "source": None}
symbol_cache = {}


def create_empty_csv():
    """Create empty CSV with headers for future lookups"""
    try:
        df = pd.DataFrame(columns=['SEM_EXM_EXCH_ID', 'SEM_TRADING_SYMBOL', 'SEM_SERIES', 'SEM_SMST_SECURITY_ID'])
        df.to_csv(SECURITY_CSV_PATH, index=False)
        print(f"✅ Empty CSV created at {SECURITY_CSV_PATH}")
        return True
    except Exception as e:
        print(f"❌ Failed to create CSV: {str(e)}")
        return False


def save_security_id_to_csv(exchange, symbol, instrument, security_id):
    """Save a newly fetched security ID to CSV"""
    try:
        if os.path.exists(SECURITY_CSV_PATH):
            df = pd.read_csv(SECURITY_CSV_PATH)
        else:
            df = pd.DataFrame(columns=['SEM_EXM_EXCH_ID', 'SEM_TRADING_SYMBOL', 'SEM_SERIES', 'SEM_SMST_SECURITY_ID'])
        
        existing = df[
            (df['SEM_EXM_EXCH_ID'] == exchange) & 
            (df['SEM_TRADING_SYMBOL'] == symbol) & 
            (df['SEM_SERIES'] == instrument)
        ]
        
        if existing.empty:
            new_row = {
                'SEM_EXM_EXCH_ID': exchange,
                'SEM_TRADING_SYMBOL': symbol,
                'SEM_SERIES': instrument,
                'SEM_SMST_SECURITY_ID': str(security_id)
            }
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
            df.to_csv(SECURITY_CSV_PATH, index=False)
            print(f"💾 Saved {symbol} (ID: {security_id}) to CSV")
            return True
        return False
    except Exception as e:
        print(f"⚠️  Failed to save to CSV: {str(e)}")
        return False


def fetch_security_id_from_dhan(symbol, instrument="EQ", exchange="NSE"):
    """Fetch security ID from Dhan API"""
    if not dhan:
        print(f"❌ Dhan connection not available")
        return None
    
    try:
        print(f"🔍 Fetching security ID from Dhan API for {symbol} ({instrument})...")
        security_list = dhan.fetch_security_list()
        
        if security_list is not None:
            if isinstance(security_list, list):
                df = pd.DataFrame(security_list)
            else:
                df = security_list.copy()
            
            if 'SEM_TRADING_SYMBOL' in df.columns:
                matches = df[
                    (df['SEM_TRADING_SYMBOL'] == symbol) & 
                    (df['SEM_SERIES'] == instrument)
                ]
                
                if not matches.empty:
                    security_id = str(matches.iloc[0]['SEM_SMST_SECURITY_ID'])
                    print(f"✅ Found security ID {security_id} for {symbol}")
                    return security_id
        
        print(f"❌ Could not find security ID for {symbol} from Dhan API")
        return None
        
    except Exception as e:
        print(f"❌ Error fetching from Dhan API: {str(e)}")
        return None


def download_csv(url, destination):
    """Download CSV file from URL"""
    try:
        print(f"📥 Downloading CSV from {url}...")
        urllib.request.urlretrieve(url, destination)
        print(f"✅ CSV downloaded successfully to {destination}")
        return True
    except Exception as e:
        print(f"❌ CSV download failed: {str(e)}")
        return False


def load_csv_cache():
    """Load CSV into cache on startup"""
    global csv_cache
    try:
        if os.path.exists(SECURITY_CSV_PATH):
            csv_cache["data"] = pd.read_csv(SECURITY_CSV_PATH, low_memory=False)
            csv_cache["error"] = None
            csv_cache["source"] = "local_file"
            print(f"✅ CSV loaded from local file: {len(csv_cache['data'])} records")
            return True
        
        if CSV_DOWNLOAD_URL:
            print(f"📍 CSV file not found locally, attempting download...")
            if download_csv(CSV_DOWNLOAD_URL, SECURITY_CSV_PATH):
                csv_cache["data"] = pd.read_csv(SECURITY_CSV_PATH, low_memory=False)
                csv_cache["error"] = None
                csv_cache["source"] = "downloaded"
                print(f"✅ CSV loaded from download: {len(csv_cache['data'])} records")
                return True
        
        print(f"📋 CSV not available, will fetch from Dhan API as needed")
        create_empty_csv()
        csv_cache["source"] = "none"
        csv_cache["error"] = "CSV not available initially - will fetch from Dhan API"
        return False
        
    except Exception as e:
        csv_cache["error"] = f"Error loading CSV: {str(e)}"
        csv_cache["source"] = "error"
        print(f"❌ {csv_cache['error']}")
        return False


def get_security_id(ticker_symbol, instrument="EQ", exchange="NSE"):
    """Get security ID with priority: memory cache -> CSV cache -> Dhan API"""
    global csv_cache, symbol_cache
    
    cache_key = f"{exchange}:{ticker_symbol}:{instrument}"
    
    if cache_key in symbol_cache:
        print(f"✅ Found {ticker_symbol} in memory cache: {symbol_cache[cache_key]}")
        return symbol_cache[cache_key]
    
    if csv_cache["data"] is not None:
        try:
            filtered = csv_cache["data"][
                (csv_cache["data"]['SEM_EXM_EXCH_ID'] == exchange) & 
                (csv_cache["data"]['SEM_TRADING_SYMBOL'] == ticker_symbol) & 
                (csv_cache["data"]['SEM_SERIES'] == instrument)
            ]
            
            if not filtered.empty:
                security_id = str(filtered.iloc[0]['SEM_SMST_SECURITY_ID'])
                symbol_cache[cache_key] = security_id
                print(f"✅ Found {ticker_symbol} in CSV cache: {security_id}")
                return security_id
        except Exception as e:
            print(f"⚠️  CSV lookup error: {str(e)}")
    
    print(f"📍 {ticker_symbol} not in CSV, fetching from Dhan API...")
    security_id = fetch_security_id_from_dhan(ticker_symbol, instrument, exchange)
    
    if security_id:
        save_security_id_to_csv(exchange, ticker_symbol, instrument, security_id)
        symbol_cache[cache_key] = security_id
        if os.path.exists(SECURITY_CSV_PATH):
            csv_cache["data"] = pd.read_csv(SECURITY_CSV_PATH, low_memory=False)
        return security_id
    
    return None


@app.post("/webhook")
async def receive_alert(request: Request):
    """Receive TradingView alert and place order"""
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    
    if payload.get("secret") != CONFIG["DHAN_SECRET"]:
        raise HTTPException(status_code=401, detail="Invalid secret")
    
    symbol = payload.get("symbol", "").upper()
    exchange = payload.get("exchange", "NSE")
    strategy = payload.get("strategy", {})
    action = strategy.get("action", "").upper()
    quantity = strategy.get("abs_qty", 0)
    instrument = payload.get("instrument", "EQ")
    
    print(f"\n{'='*60}")
    print(f"🎯 Webhook Request: {symbol} | {action} | Qty: {quantity}")
    print(f"   Exchange: {exchange} | Instrument: {instrument}")
    print(f"{'='*60}")
    
    if not dhan:
        return {
            "success": False,
            "message": "Dhan connection not initialized",
            "details": DHAN_ERROR
        }
    
    security_id = get_security_id(symbol, instrument, exchange)
    
    if not security_id:
        print(f"❌ Could not resolve security ID for {symbol}")
        return {
            "success": False,
            "message": f"Symbol '{symbol}' not found",
            "details": {
                "symbol": symbol,
                "instrument": instrument,
                "exchange": exchange,
                "csv_records": len(csv_cache["data"]) if csv_cache["data"] is not None else 0
            }
        }
    
    print(f"✅ Security ID resolved: {security_id}")
    
    if action not in ["BUY", "SELL"] or not quantity or int(quantity) <= 0:
        return {"success": False, "message": "Invalid action or quantity"}
    
    try:
        transaction_type = dhan.BUY if action == "BUY" else dhan.SELL
        print(f"📤 Placing order: {action} {quantity} shares via Dhan API...")
        
        print(f"\n📋 Dhan API Request Details:")
        print(f"   exchange_segment: NSE (dhan.NSE)")
        print(f"   transaction_type: {action} (dhan.{'BUY' if action == 'BUY' else 'SELL'})")
        print(f"   quantity: {int(quantity)}")
        print(f"   order_type: MARKET (dhan.MARKET)")
        print(f"   product_type: INTRA (dhan.INTRA)")
        print(f"   price: 0")
        
        response = dhan.place_order(
            security_id=security_id,
            exchange_segment=dhan.NSE,
            transaction_type=transaction_type,
            quantity=int(quantity),
            order_type=dhan.MARKET,
            product_type=dhan.INTRA,
            price=0
        )
        
        print(f"\n📡 Dhan API Response: {response}\n")
        
        if isinstance(response, dict) and response.get('status') == 'success':
            order_id = response.get('data', {}).get('orderId', 'unknown')
            print(f"✅ Order placed successfully: {order_id}")
            return {
                "success": True,
                "message": "Order placed successfully",
                "order_id": order_id,
                "symbol": symbol,
                "action": action,
                "quantity": int(quantity),
                "security_id": security_id
            }
        else:
            print(f"❌ Dhan API returned error: {response}")
            return {
                "success": False, 
                "message": "Order failed",
                "response": str(response)
            }
            
    except Exception as e:
        print(f"❌ Error placing order: {str(e)}")
        return {
            "success": False,
            "message": f"Error placing order: {str(e)}",
            "symbol": symbol,
            "security_id": security_id
        }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat()
    }


@app.get("/status")
async def status():
    """Check application and CSV status"""
    csv_status_map = {
        "local_file": "✅ Loaded from local file",
        "downloaded": "✅ Loaded from download",
        "none": "⏳ Ready to fetch from Dhan API",
        "error": "❌ Error loading",
        None: "⏳ Not loaded"
    }
    
    return {
        "app_status": "running",
        "dhan_status": "✅ Connected" if dhan else "❌ Not connected",
        "dhan_error": DHAN_ERROR,
        "csv_status": csv_status_map.get(csv_cache["source"], "unknown"),
        "csv_source": csv_cache["source"],
        "csv_records": len(csv_cache["data"]) if csv_cache["data"] is not None else 0,
        "memory_cache_symbols": len(symbol_cache),
        "timestamp": datetime.now().isoformat()
    }


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "name": "TradingView to Dhan Bridge",
        "version": "2.1",
        "status": "Ready",
        "features": [
            "Dynamic security ID lookup from Dhan API",
            "Automatic CSV caching for frequent lookups",
            "Environment variable configuration",
            "Health check endpoint"
        ]
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    host = os.getenv("HOST", "0.0.0.0")
    
    print("🚀 TradingView to Dhan Bridge v2.1 Starting...")
    print(f"📍 Server: {host}:{port}")
    print("📋 Loading CSV cache...")
    load_csv_cache()
    print("✨ Features:")
    print("   - Fetches security IDs from Dhan API dynamically")
    print("   - Saves to CSV for fast future lookups")
    print("   - Environment variable configuration")
    print(f"🔗 Webhook: http://localhost:{port}/webhook")
    print(f"📊 Status: http://localhost:{port}/status")
    print(f"❤️  Health: http://localhost:{port}/health")
    uvicorn.run(app, host=host, port=port)
