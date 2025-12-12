"""TradingView to Dhan Webhook Bridge with Symbol Lookup"""

from fastapi import FastAPI, Request, HTTPException
from dhanhq import dhanhq
import os
import json
import logging
import csv
from io import StringIO
from urllib.request import urlopen
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CLIENT_ID = os.getenv("DHAN_CLIENT_ID", "1108320935")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN", "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzY1NTk5MjExLCJpYXQiOjE3NjU1MTI4MTEsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA4MzIwOTM1In0.j-Gd1HA2djfVj4bE_3WF0Ev5aEqxN3Wbuv6DkVLpqKOXhmq12pmHrQv8npuC1kJyGsKye8Qu1PmTYb3iNYtF9A")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "NzDyE")

# Cache file settings
CACHE_FILE = "/tmp/dhan_instruments_cache.csv"
CACHE_EXPIRY_HOURS = 24

SYMBOL_TO_ID = {
    "HDFCBANK": "1333",
    "INFY": "1594",
    "TCS": "11536",
    "TMPV": "3456",
    "WHIRLPOOL": "18011",
}


def is_cache_valid():
    """Check if cached instrument file is still valid"""
    if not os.path.exists(CACHE_FILE):
        return False
    file_age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(CACHE_FILE))
    return file_age < timedelta(hours=CACHE_EXPIRY_HOURS)


def download_instruments():
    """Download Dhan's instrument master CSV and cache it"""
    try:
        if is_cache_valid():
            with open(CACHE_FILE, 'r') as f:
                logger.info("📦 Using cached instrument data")
                return f.read()
        
        url = "https://images.dhan.co/api-data/api-scrip-master.csv"
        logger.info("📥 Downloading instrument master...")
        
        with urlopen(url, timeout=10) as response:
            data = response.read().decode('utf-8')
        
        # Cache the data
        with open(CACHE_FILE, 'w') as f:
            f.write(data)
        
        logger.info("✅ Downloaded and cached instrument data")
        return data
    except Exception as e:
        logger.warning(f"⚠️  Failed to download instruments: {e}")
        # Try to use cache even if expired
        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE, 'r') as f:
                logger.info("📦 Using expired cache as fallback")
                return f.read()
        return None


def get_security_id_from_sheet(symbol):
    """Get security ID from Dhan's instrument master sheet"""
    data = download_instruments()
    if not data:
        logger.warning("⚠️  Could not load instrument data, using hardcoded mapping")
        return SYMBOL_TO_ID.get(symbol)
    
    try:
        reader = csv.reader(StringIO(data))
        header = next(reader)
        
        idx_exchange = header.index('SEM_EXM_EXCH_ID')
        idx_type = header.index('SEM_EXCH_INSTRUMENT_TYPE')
        idx_sec_id = header.index('SEM_SMST_SECURITY_ID')
        idx_symbol = header.index('SEM_TRADING_SYMBOL')
        
        for row in reader:
            exchange = row[idx_exchange].strip()
            instr_type = row[idx_type].strip()
            base_symbol = row[idx_symbol].strip().split('-')[0]
            
            # NSE equity spot only
            if exchange == 'NSE' and instr_type == 'ES' and base_symbol == symbol:
                sec_id = row[idx_sec_id].strip()
                logger.info(f"📊 Found {symbol} -> Security ID: {sec_id} (from sheet)")
                return sec_id
        
        logger.warning(f"⚠️  {symbol} not found in sheet, using hardcoded mapping")
        return SYMBOL_TO_ID.get(symbol)
        
    except Exception as e:
        logger.error(f"❌ Error parsing instruments: {e}")
        logger.warning(f"⚠️  Using hardcoded mapping for {symbol}")
        return SYMBOL_TO_ID.get(symbol)

app = FastAPI()
dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)


@app.post("/webhook")
async def webhook(request: Request):
    """Receive TradingView alert and place order on Dhan"""
    payload = await request.json()
    logger.info(f"📨 Webhook received: {json.dumps(payload, indent=2)}")
    
    if payload.get("secret") != WEBHOOK_SECRET:
        logger.warning(f"❌ Invalid secret: {payload.get('secret')}")
        raise HTTPException(status_code=401, detail="Invalid secret")
    
    symbol = payload.get("symbol", "").upper()
    strategy = payload.get("strategy", {})
    action = strategy.get("action", "").upper()
    
    logger.info(f"Extracted: symbol={symbol}, action={action}")
    
    # STEP 1: Extract raw contracts from nested structure
    order_info = strategy.get("order", {})
    raw_contracts = order_info.get("contracts", 0)
    
    # STEP 2: Calculate absolute quantity (handle negative contracts)
    # If contracts is positive, use it; if negative, multiply by -1
    try:
        contracts_num = float(raw_contracts)
        quantity = int(abs(contracts_num))  # Absolute value: convert to positive if negative
    except (ValueError, TypeError):
        logger.warning(f"❌ Invalid quantity: {raw_contracts}")
        return {"success": False, "error": "Invalid quantity: must be a number"}
    
    # STEP 3: Extract position info for message
    position_size = strategy.get("position_size", 0)
    
    logger.info(f"Quantity calculation: contracts={raw_contracts} → abs_qty={quantity}")
    
    if not symbol or not action or quantity <= 0:
        logger.warning(f"❌ Invalid input: symbol={symbol}, action={action}, qty={quantity}")
        return {"success": False, "error": "Invalid input"}
    
    if action not in ["BUY", "SELL"]:
        logger.warning(f"❌ Invalid action: {action}")
        return {"success": False, "error": "Invalid action"}
    
    # STEP 4: Create alert message
    alert_message = f"{action} @ {quantity} filled on {symbol}. New strategy position is {position_size}"
    logger.info(f"📝 Alert: {alert_message}")
    
    # Get security ID from sheet or hardcoded mapping
    sec_id = get_security_id_from_sheet(symbol)
    
    if not sec_id:
        logger.warning(f"❌ Unknown symbol: {symbol}")
        return {"success": False, "error": f"Symbol {symbol} not found"}
    
    try:
        logger.info(f"📊 Placing {action} order: symbol={symbol}, sec_id={sec_id}, qty={quantity}")
        
        response = dhan.place_order(
            security_id=sec_id,
            exchange_segment=dhan.NSE,
            transaction_type=dhan.BUY if action == "BUY" else dhan.SELL,
            quantity=quantity,
            order_type=dhan.MARKET,
            product_type=dhan.INTRA,
            price=0
        )
        
        logger.info(f"📈 Dhan response: {json.dumps(response, indent=2)}")
        
        if response.get("status") == "success":
            order_data = response.get("data", {})
            order_id = order_data.get("orderId")
            order_status = order_data.get("orderStatus", "UNKNOWN")
            error_desc = order_data.get("omsErrorDescription", "")
            
            logger.info(f"Order ID: {order_id}, Status: {order_status}")
            
            if order_status == "REJECTED":
                logger.error(f"❌ Order rejected: {error_desc}")
                return {
                    "success": False,
                    "order_id": order_id,
                    "error": error_desc or "Order rejected by exchange"
                }
            
            logger.info(f"✅ Order placed: {order_id}, Status: {order_status}")
            return {
                "success": True,
                "order_id": order_id,
                "symbol": symbol,
                "action": action,
                "quantity": quantity,
                "order_status": order_status
            }
        
        error_msg = response.get("error", "Unknown error")
        logger.error(f"❌ Order failed: {error_msg}")
        return {"success": False, "error": error_msg}
    except Exception as e:
        logger.error(f"❌ Exception: {str(e)}", exc_info=True)
        return {"success": False, "error": str(e)}


@app.get("/health")
async def health():
    return {"status": "ok"}
