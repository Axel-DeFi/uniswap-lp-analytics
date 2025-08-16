# FastAPI skeleton (comments in English)
from fastapi import FastAPI

app = FastAPI(title="Uniswap LP Analytics API")

@app.get("/healthz")
async def healthz():
    return {"status": "ok"}
