from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
import httpx
import os
from google.cloud import secretmanager

app = FastAPI()
secret_client = secretmanager.SecretManagerServiceClient()

# --------- Helpers for Secret Manager --------- #

def _project_id() -> str:
    # In Cloud Run this env var is available
    return os.environ.get("GCP_PROJECT") or os.environ.get("GOOGLE_CLOUD_PROJECT")

def get_secret(secret_id: str) -> str:
    """
    Read latest version of a secret from Secret Manager.
    """
    name = f"projects/{_project_id()}/secrets/{secret_id}/versions/latest"
    response = secret_client.access_secret_version(name=name)
    return response.payload.data.decode("utf-8")

def set_secret(secret_id: str, value: str) -> None:
    """
    Add a new version to an existing secret.
    """
    parent = f"projects/{_project_id()}/secrets/{secret_id}"
    secret_client.add_secret_version(
        parent=parent,
        payload={"data": value.encode("utf-8")},
    )

# --------- Webhook endpoint from TradingView --------- #

@app.post("/webhook")
async def tradingview_webhook(request: Request):
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    # 1) Validate secret
    tv_secret = payload.get("secret")
    expected_secret = get_secret("TV_WEBHOOK_SECRET")
    if tv_secret != expected_secret:
        raise HTTPException(status_code=401, detail="Invalid secret")

    strategy = payload.get("strategy", {}) or {}
    action = (strategy.get("action") or "").lower()
    abs_qty_str = strategy.get("abs_qty") or "0"

    try:
        abs_qty = int(float(abs_qty_str))
    except ValueError:
        abs_qty = 0

    if action not in ("buy", "sell") or abs_qty <= 0:
        # Nothing to send to Dhan, just ack
        return {"success": True, "message": "No trade action / qty <= 0"}

    # 2) Map TradingView action -> Dhan transactionType
    transaction_type = "B" if action == "buy" else "S"

    # TODO: you will later compute these (symbol, strike, expiry) from your strategy logic.
    # For now we use your example NIFTY CE order from Dhan portal:
    dhan_order = {
        "secret": expected_secret,                      # same TradingView secret Dhan expects
        "alertType": "multi_leg_order",
        "order_legs": [
            {
                "transactionType": transaction_type,    # "B" or "S"
                "orderType": "MKT",
                "quantity": str(abs_qty),               # use strategy qty from TV
                "exchange": "NSE",
                "symbol": "NIFTY",
                "instrument": "OPT",
                "productType": "I",
                "sort_order": "1",
                "price": "0",
                "option_type": "CE",
                "strike_price": "26150.0",
                "expiry_date": "2025-12-09"
            }
        ]
    }

    dhan_webhook_url = get_secret("DHAN_TV_WEBHOOK_URL")

    # 3) Send to Dhan webhook
    async with httpx.AsyncClient(timeout=5.0) as client:
        try:
            resp = await client.post(dhan_webhook_url, json=dhan_order)
        except httpx.RequestError as e:
            # Network / DNS / timeout issues
            raise HTTPException(status_code=502, detail=f"Dhan webhook error: {e}")

    # Forward relevant info back to TV/logs
    return JSONResponse(
        {
            "success": resp.status_code == 200,
            "status_code": resp.status_code,
            "dhan_response": resp.text,
        },
        status_code=200,
    )

# --------- Daily token refresh endpoint (called by Cloud Scheduler) --------- #

@app.post("/refresh-dhan-token")
async def refresh_dhan_token():
    """
    Refresh Dhan API access token via /v2/RenewToken and store in Secret Manager.

    This assumes DHAN_ACCESS_TOKEN initially holds a VALID token
    generated from Dhan Web, then we keep renewing it daily.
    """
    dhan_client_id = get_secret("DHAN_CLIENT_ID")
    current_token = get_secret("DHAN_ACCESS_TOKEN")

    url = "https://api.dhan.co/v2/RenewToken"
    headers = {
        "access-token": current_token,
        "dhanClientId": dhan_client_id,
    }

    async with httpx.AsyncClient(timeout=5.0) as client:
        resp = await client.get(url, headers=headers)

    if resp.status_code != 200:
        # Log the error body so you can debug from Cloud Logging
        return JSONResponse(
            {
                "success": False,
                "message": "Failed to renew token",
                "status_code": resp.status_code,
                "body": resp.text,
            },
            status_code=500,
        )

    data = resp.json()
    # According to community examples, the new token field is 'token':contentReference[oaicite:2]{index=2}
    new_token = data.get("token") or data.get("accessToken")

    if not new_token:
        return JSONResponse(
            {
                "success": False,
                "message": "RenewToken response missing token field",
                "raw": data,
            },
            status_code=500,
        )

    # Store new token as a new version
    set_secret("DHAN_ACCESS_TOKEN", new_token)

    return {
        "success": True,
        "message": "Dhan access token renewed",
    }
