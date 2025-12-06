from fastapi import FastAPI, Request

app = FastAPI()

@app.post("/webhook")
async def tradingview_webhook(request: Request):
    data = await request.json()
    print("TradingView Data:", data)
    return {"success": True, "received": data}