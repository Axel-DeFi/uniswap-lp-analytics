from typing import Optional, List, Literal, Dict, Any, Tuple
import os, time, io, csv
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy import text, bindparam, Integer, SmallInteger, String
from dotenv import load_dotenv
from pathlib import Path

ROOT = Path("/Users/axel/Dev/open-source/uniswap-lp-analytics")
load_dotenv(ROOT / ".env", override=True)
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set in .env")

engine = create_async_engine(DATABASE_URL, future=True)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

app = FastAPI(title="Uniswap LP Analytics API", version="0.6.0")

class TokenOut(BaseModel):
    address: str
    symbol: Optional[str] = None
    decimals: int

class PoolOut(BaseModel):
    id: str
    version: int
    chain_id: int
    token0: TokenOut
    token1: TokenOut
    fee_tier_bps: int
    tick_spacing: int
    created_at_ts: int

class Page(BaseModel):
    page: int = Field(..., ge=1)
    page_size: int = Field(..., ge=1, le=100)
    total: int
    items: List

class TTLCache:
    def __init__(self, ttl_seconds: int, maxsize: int):
        self.ttl = ttl_seconds
        self.maxsize = maxsize
        self._store: Dict[Any, Tuple[float, Any]] = {}

    def get(self, key):
        rec = self._store.get(key)
        if not rec:
            return None
        exp, val = rec
        if exp < time.time():
            self._store.pop(key, None)
            return None
        return val

    def set(self, key, val):
        if len(self._store) >= self.maxsize:
            self._store.pop(next(iter(self._store)))
        self._store[key] = (time.time() + self.ttl, val)

cache_tokens = TTLCache(ttl_seconds=60, maxsize=200)
cache_pools  = TTLCache(ttl_seconds=30, maxsize=500)
cache_metrics= TTLCache(ttl_seconds=30, maxsize=200)
cache_top    = TTLCache(ttl_seconds=15, maxsize=200)

@app.get("/health")
async def health():
    return {"ok": True}

# ---------- TOKENS ----------
@app.get("/tokens", response_model=Page)
async def list_tokens(
    q: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    key = ("tokens", q or "", page, page_size)
    cached = cache_tokens.get(key)
    if cached:
        return Page(page=page, page_size=page_size, total=cached["total"], items=cached["items"])

    limit = page_size
    offset = (page - 1) * page_size
    where = "(:q is null or t.symbol ilike '%' || :q || '%')"

    count_sql = text(f"select count(*) from tokens t where {where}").bindparams(
        bindparam("q", type_=String),
    )
    data_sql = text(f"""
      select t.address, t.symbol, t.decimals
      from tokens t
      where {where}
      order by coalesce(t.symbol,'') asc, t.address asc
      limit :limit offset :offset
    """).bindparams(
        bindparam("q", type_=String),
        bindparam("limit", type_=Integer),
        bindparam("offset", type_=Integer),
    )
    params = {"q": q, "limit": limit, "offset": offset}

    async with SessionLocal() as session:
        total = (await session.execute(count_sql, params)).scalar_one()
        rows = (await session.execute(data_sql, params)).mappings().all()

    items = [TokenOut(address=r["address"], symbol=r["symbol"], decimals=r["decimals"]) for r in rows]
    payload = {"total": total, "items": items}
    cache_tokens.set(key, payload)
    return Page(page=page, page_size=page_size, total=total, items=items)

# ---------- POOLS LIST ----------
@app.get("/pools", response_model=Page)
async def list_pools(
    version: Optional[int] = Query(None),
    token: Optional[str] = Query(None),
    token_symbol: Optional[str] = Query(None),
    fee_min: Optional[int] = Query(None, ge=0),
    fee_max: Optional[int] = Query(None, ge=0),
    order_by: Literal["created_at_ts", "fee_tier_bps"] = "created_at_ts",
    order_dir: Literal["asc", "desc"] = "desc",
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    token_norm = token.lower() if token else None
    if token_norm and (not token_norm.startswith("0x") or len(token_norm) != 42):
        raise HTTPException(status_code=400, detail="Invalid token address")

    key = ("pools", version, token_norm or "", token_symbol or "", fee_min, fee_max, order_by, order_dir, page, page_size)
    cached = cache_pools.get(key)
    if cached:
        return Page(page=page, page_size=page_size, total=cached["total"], items=cached["items"])

    limit = page_size
    offset = (page - 1) * page_size

    allowed_cols = {"created_at_ts": "p.created_at_ts", "fee_tier_bps": "p.fee_tier_bps"}
    col_sql = allowed_cols[order_by]
    dir_sql = "ASC" if order_dir.lower() == "asc" else "DESC"

    where = """
      (:version is null or p.version = :version)
      and (:token is null or p.token0_id = :token or p.token1_id = :token)
      and (:token_symbol is null or t0.symbol ilike :ts_like or t1.symbol ilike :ts_like)
      and (:fee_min is null or p.fee_tier_bps >= :fee_min)
      and (:fee_max is null or p.fee_tier_bps <= :fee_max)
    """

    count_sql = text(f"""
      select count(*) from pools p
      join tokens t0 on t0.id = p.token0_id
      join tokens t1 on t1.id = p.token1_id
      where {where}
    """).bindparams(
        bindparam("version", type_=SmallInteger),
        bindparam("token", type_=String),
        bindparam("token_symbol", type_=String),
        bindparam("ts_like", type_=String),
        bindparam("fee_min", type_=Integer),
        bindparam("fee_max", type_=Integer),
    )

    data_sql = text(f"""
      select
        p.id, p.version, p.chain_id, p.fee_tier_bps, p.tick_spacing, p.created_at_ts,
        t0.address as t0_addr, t0.symbol as t0_sym, t0.decimals as t0_dec,
        t1.address as t1_addr, t1.symbol as t1_sym, t1.decimals as t1_dec
      from pools p
      join tokens t0 on t0.id = p.token0_id
      join tokens t1 on t1.id = p.token1_id
      where {where}
      order by {col_sql} {dir_sql}
      limit :limit offset :offset
    """).bindparams(
        bindparam("version", type_=SmallInteger),
        bindparam("token", type_=String),
        bindparam("token_symbol", type_=String),
        bindparam("ts_like", type_=String),
        bindparam("fee_min", type_=Integer),
        bindparam("fee_max", type_=Integer),
        bindparam("limit", type_=Integer),
        bindparam("offset", type_=Integer),
    )

    ts_like = f"%{token_symbol}%" if token_symbol else None
    params = {
        "version": version,
        "token": token_norm,
        "token_symbol": token_symbol,
        "ts_like": ts_like,
        "fee_min": fee_min,
        "fee_max": fee_max,
        "limit": limit,
        "offset": offset,
    }

    async with SessionLocal() as session:
        total = (await session.execute(count_sql, params)).scalar_one()
        rows = (await session.execute(data_sql, params)).mappings().all()

    items = []
    for r in rows:
        items.append({
            "id": r["id"],
            "version": r["version"],
            "chain_id": r["chain_id"],
            "token0": {"address": r["t0_addr"], "symbol": r["t0_sym"], "decimals": r["t0_dec"]},
            "token1": {"address": r["t1_addr"], "symbol": r["t1_sym"], "decimals": r["t1_dec"]},
            "fee_tier_bps": r["fee_tier_bps"],
            "tick_spacing": r["tick_spacing"],
            "created_at_ts": r["created_at_ts"],
        })

    payload = {"total": total, "items": items}
    cache_pools.set(key, payload)
    return Page(page=page, page_size=page_size, total=total, items=items)

# ---------- METRICS SUMMARY ----------
@app.get("/metrics/summary")
async def metrics_summary(
    version: Optional[int] = Query(None),
    token: Optional[str] = Query(None),
    token_symbol: Optional[str] = Query(None),
    fee_min: Optional[int] = Query(None, ge=0),
    fee_max: Optional[int] = Query(None, ge=0),
):
    token_norm = token.lower() if token else None
    key = ("metrics_summary", version, token_norm or "", token_symbol or "", fee_min, fee_max)
    cached = cache_metrics.get(key)
    if cached:
        return cached

    where = """
      (:version is null or p.version = :version)
      and (:token is null or p.token0_id = :token or p.token1_id = :token)
      and (:token_symbol is null or t0.symbol ilike :ts_like or t1.symbol ilike :ts_like)
      and (:fee_min is null or p.fee_tier_bps >= :fee_min)
      and (:fee_max is null or p.fee_tier_bps <= :fee_max)
    """
    ts_like = f"%{token_symbol}%" if token_symbol else None

    total_sql = text(f"""
      select count(*) from pools p
      join tokens t0 on t0.id = p.token0_id
      join tokens t1 on t1.id = p.token1_id
      where {where}
    """).bindparams(
        bindparam("version", type_=SmallInteger),
        bindparam("token", type_=String),
        bindparam("token_symbol", type_=String),
        bindparam("ts_like", type_=String),
        bindparam("fee_min", type_=Integer),
        bindparam("fee_max", type_=Integer),
    )
    by_ver_sql = text(f"""
      select p.version as version, count(*) as cnt
      from pools p
      join tokens t0 on t0.id = p.token0_id
      join tokens t1 on t1.id = p.token1_id
      where {where}
      group by p.version
      order by p.version asc
    """).bindparams(
        bindparam("version", type_=SmallInteger),
        bindparam("token", type_=String),
        bindparam("token_symbol", type_=String),
        bindparam("ts_like", type_=String),
        bindparam("fee_min", type_=Integer),
        bindparam("fee_max", type_=Integer),
    )
    by_fee_sql = text(f"""
      select p.fee_tier_bps as fee, count(*) as cnt
      from pools p
      join tokens t0 on t0.id = p.token0_id
      join tokens t1 on t1.id = p.token1_id
      where {where}
      group by p.fee_tier_bps
      order by p.fee_tier_bps asc
    """).bindparams(
        bindparam("version", type_=SmallInteger),
        bindparam("token", type_=String),
        bindparam("token_symbol", type_=String),
        bindparam("ts_like", type_=String),
        bindparam("fee_min", type_=Integer),
        bindparam("fee_max", type_=Integer),
    )

    params = {
        "version": version,
        "token": token_norm,
        "token_symbol": token_symbol,
        "ts_like": ts_like,
        "fee_min": fee_min,
        "fee_max": fee_max,
    }

    async with SessionLocal() as session:
        total = (await session.execute(total_sql, params)).scalar_one()
        by_ver = (await session.execute(by_ver_sql, params)).mappings().all()
        by_fee = (await session.execute(by_fee_sql, params)).mappings().all()

    result = {
        "total": total,
        "by_version": [{"version": int(r["version"]), "count": int(r["cnt"])} for r in by_ver],
        "by_fee_tier": [{"fee_tier_bps": int(r["fee"]), "count": int(r["cnt"])} for r in by_fee],
        "cache_ttl_seconds": cache_metrics.ttl,
    }
    cache_metrics.set(key, result)
    return result

# ---------- helpers ----------
def _window_threshold(window: str, lookback: int, since_day_id: Optional[int], since_hour_id: Optional[int]):
    if window == "day":
        th = int(since_day_id if since_day_id is not None else (time.time() // 86400) - lookback)
        return th, "pool_day_data", "date"
    else:
        th = int(since_hour_id if since_hour_id is not None else (time.time() // 3600) - lookback)
        return th, "pool_hour_data", "hour_start_unix"

# ---------- TOP FEES ----------
@app.get("/pools/top_fees", response_model=Page)
async def pools_top_fees(
    window: Literal["day","hour"] = Query("day"),
    lookback: int = Query(30, ge=1),
    since_day_id: Optional[int] = Query(None),
    since_hour_id: Optional[int] = Query(None),
    version: Optional[int] = Query(None),
    token: Optional[str] = Query(None),
    token_symbol: Optional[str] = Query(None),
    fee_min: Optional[int] = Query(None, ge=0),
    fee_max: Optional[int] = Query(None, ge=0),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    limit: Optional[int] = Query(None, ge=1, le=100),
):
    token_norm = token.lower() if token else None
    if token_norm and (not token_norm.startswith("0x") or len(token_norm) != 42):
        raise HTTPException(status_code=400, detail="Invalid token address")
    if limit:
        page_size = limit

    limit_q = page_size
    offset = (page - 1) * page_size
    th, agg_table, agg_field = _window_threshold(window, lookback, since_day_id, since_hour_id)

    where = f"""
      (:version is null or p.version = :version)
      and (:token is null or p.token0_id = :token or p.token1_id = :token)
      and (:token_symbol is null or t0.symbol ilike :ts_like or t1.symbol ilike :ts_like)
      and (:fee_min is null or p.fee_tier_bps >= :fee_min)
      and (:fee_max is null or p.fee_tier_bps <= :fee_max)
      and a.{agg_field} >= :th
    """
    ts_like = f"%{token_symbol}%" if token_symbol else None
    params_base = {
        "version": version, "token": token_norm, "token_symbol": token_symbol, "ts_like": ts_like,
        "fee_min": fee_min, "fee_max": fee_max, "th": th, "limit": limit_q, "offset": offset
    }

    count_sql = text(f"""
      select count(*) from (
        select p.id
        from {agg_table} a
        join pools p on p.id = a.pool_id
        join tokens t0 on t0.id = p.token0_id
        join tokens t1 on t1.id = p.token1_id
        where {where}
        group by p.id
      ) s
    """).bindparams(
        bindparam("version", type_=SmallInteger), bindparam("token", type_=String),
        bindparam("token_symbol", type_=String), bindparam("ts_like", type_=String),
        bindparam("fee_min", type_=Integer), bindparam("fee_max", type_=Integer),
        bindparam("th", type_=Integer)
    )

    data_sql = text(f"""
      select
        p.id, p.version, p.chain_id, p.fee_tier_bps, p.tick_spacing, p.created_at_ts,
        t0.address as t0_addr, t0.symbol as t0_sym, t0.decimals as t0_dec,
        t1.address as t1_addr, t1.symbol as t1_sym, t1.decimals as t1_dec,
        sum(coalesce(a.approx_fee_token0,0) + coalesce(a.approx_fee_token1,0)) as fees_sum
      from {agg_table} a
      join pools p on p.id = a.pool_id
      join tokens t0 on t0.id = p.token0_id
      join tokens t1 on t1.id = p.token1_id
      where {where}
      group by p.id, p.version, p.chain_id, p.fee_tier_bps, p.tick_spacing, p.created_at_ts,
               t0.address, t0.symbol, t0.decimals, t1.address, t1.symbol, t1.decimals
      order by fees_sum desc
      limit :limit offset :offset
    """).bindparams(
        bindparam("version", type_=SmallInteger), bindparam("token", type_=String),
        bindparam("token_symbol", type_=String), bindparam("ts_like", type_=String),
        bindparam("fee_min", type_=Integer), bindparam("fee_max", type_=Integer),
        bindparam("th", type_=Integer), bindparam("limit", type_=Integer), bindparam("offset", type_=Integer)
    )

    async with SessionLocal() as session:
        total = (await session.execute(count_sql, params_base)).scalar_one()
        rows = (await session.execute(data_sql, params_base)).mappings().all()

    items = []
    for r in rows:
        items.append({
            "pool": {
                "id": r["id"], "version": r["version"], "chain_id": r["chain_id"],
                "token0": {"address": r["t0_addr"], "symbol": r["t0_sym"], "decimals": r["t0_dec"]},
                "token1": {"address": r["t1_addr"], "symbol": r["t1_sym"], "decimals": r["t1_dec"]},
                "fee_tier_bps": r["fee_tier_bps"], "tick_spacing": r["tick_spacing"], "created_at_ts": r["created_at_ts"]
            },
            "fees_sum": float(r["fees_sum"]),
            "window": window
        })

    return Page(page=page, page_size=page_size, total=total, items=items)

# ---------- TOP VOLUME ----------
@app.get("/pools/top_volume", response_model=Page)
async def pools_top_volume(
    window: Literal["day","hour"] = Query("day"),
    side: Literal["both","token0","token1"] = Query("both"),
    lookback: int = Query(30, ge=1),
    since_day_id: Optional[int] = Query(None),
    since_hour_id: Optional[int] = Query(None),
    version: Optional[int] = Query(None),
    token: Optional[str] = Query(None),
    token_symbol: Optional[str] = Query(None),
    fee_min: Optional[int] = Query(None, ge=0),
    fee_max: Optional[int] = Query(None, ge=0),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    limit: Optional[int] = Query(None, ge=1, le=100),
):
    token_norm = token.lower() if token else None
    if token_norm and (not token_norm.startswith("0x") or len(token_norm) != 42):
        raise HTTPException(status_code=400, detail="Invalid token address")
    if limit:
        page_size = limit

    limit_q = page_size
    offset = (page - 1) * page_size
    th, agg_table, agg_field = _window_threshold(window, lookback, since_day_id, since_hour_id)

    vol_expr = {
        "both": "sum(coalesce(a.volume_token0,0) + coalesce(a.volume_token1,0))",
        "token0": "sum(coalesce(a.volume_token0,0))",
        "token1": "sum(coalesce(a.volume_token1,0))",
    }[side]

    where = f"""
      (:version is null or p.version = :version)
      and (:token is null or p.token0_id = :token or p.token1_id = :token)
      and (:token_symbol is null or t0.symbol ilike :ts_like or t1.symbol ilike :ts_like)
      and (:fee_min is null or p.fee_tier_bps >= :fee_min)
      and (:fee_max is null or p.fee_tier_bps <= :fee_max)
      and a.{agg_field} >= :th
    """
    ts_like = f"%{token_symbol}%" if token_symbol else None
    params_base = {
        "version": version, "token": token_norm, "token_symbol": token_symbol, "ts_like": ts_like,
        "fee_min": fee_min, "fee_max": fee_max, "th": th, "limit": limit_q, "offset": offset
    }

    count_sql = text(f"""
      select count(*) from (
        select p.id
        from {agg_table} a
        join pools p on p.id = a.pool_id
        join tokens t0 on t0.id = p.token0_id
        join tokens t1 on t1.id = p.token1_id
        where {where}
        group by p.id
      ) s
    """).bindparams(
        bindparam("version", type_=SmallInteger), bindparam("token", type_=String),
        bindparam("token_symbol", type_=String), bindparam("ts_like", type_=String),
        bindparam("fee_min", type_=Integer), bindparam("fee_max", type_=Integer),
        bindparam("th", type_=Integer)
    )

    data_sql = text(f"""
      select
        p.id, p.version, p.chain_id, p.fee_tier_bps, p.tick_spacing, p.created_at_ts,
        t0.address as t0_addr, t0.symbol as t0_sym, t0.decimals as t0_dec,
        t1.address as t1_addr, t1.symbol as t1_sym, t1.decimals as t1_dec,
        {vol_expr} as volume_sum,
        sum(coalesce(a.swap_count,0)) as swaps
      from {agg_table} a
      join pools p on p.id = a.pool_id
      join tokens t0 on t0.id = p.token0_id
      join tokens t1 on t1.id = p.token1_id
      where {where}
      group by p.id, p.version, p.chain_id, p.fee_tier_bps, p.tick_spacing, p.created_at_ts,
               t0.address, t0.symbol, t0.decimals, t1.address, t1.symbol, t1.decimals
      order by volume_sum desc
      limit :limit offset :offset
    """).bindparams(
        bindparam("version", type_=SmallInteger), bindparam("token", type_=String),
        bindparam("token_symbol", type_=String), bindparam("ts_like", type_=String),
        bindparam("fee_min", type_=Integer), bindparam("fee_max", type_=Integer),
        bindparam("th", type_=Integer), bindparam("limit", type_=Integer), bindparam("offset", type_=Integer)
    )

    async with SessionLocal() as session:
        total = (await session.execute(count_sql, params_base)).scalar_one()
        rows = (await session.execute(data_sql, params_base)).mappings().all()

    items = []
    for r in rows:
        items.append({
            "pool": {
                "id": r["id"], "version": r["version"], "chain_id": r["chain_id"],
                "token0": {"address": r["t0_addr"], "symbol": r["t0_sym"], "decimals": r["t0_dec"]},
                "token1": {"address": r["t1_addr"], "symbol": r["t1_sym"], "decimals": r["t1_dec"]},
                "fee_tier_bps": r["fee_tier_bps"], "tick_spacing": r["tick_spacing"], "created_at_ts": r["created_at_ts"]
            },
            "volume_sum": float(r["volume_sum"]),
            "swaps_sum": int(r["swaps"]),
            "side": side,
            "window": window
        })

    return Page(page=page, page_size=page_size, total=total, items=items)

# ---------- CSV EXPORTS ----------
@app.get("/export/top_fees.csv")
async def export_top_fees_csv(
    window: Literal["day","hour"] = Query("day"),
    lookback: int = Query(30, ge=1),
    since_day_id: Optional[int] = Query(None),
    since_hour_id: Optional[int] = Query(None),
    version: Optional[int] = Query(None),
    token: Optional[str] = Query(None),
    token_symbol: Optional[str] = Query(None),
    fee_min: Optional[int] = Query(None, ge=0),
    fee_max: Optional[int] = Query(None, ge=0),
    limit: int = Query(100, ge=1, le=100),
):
    token_norm = token.lower() if token else None
    th, agg_table, agg_field = _window_threshold(window, lookback, since_day_id, since_hour_id)

    where = f"""
      (:version is null or p.version = :version)
      and (:token is null or p.token0_id = :token or p.token1_id = :token)
      and (:token_symbol is null or t0.symbol ilike :ts_like or t1.symbol ilike :ts_like)
      and (:fee_min is null or p.fee_tier_bps >= :fee_min)
      and (:fee_max is null or p.fee_tier_bps <= :fee_max)
      and a.{agg_field} >= :th
    """
    ts_like = f"%{token_symbol}%" if token_symbol else None
    params = {
        "version": version, "token": token_norm, "token_symbol": token_symbol, "ts_like": ts_like,
        "fee_min": fee_min, "fee_max": fee_max, "th": th, "limit": limit
    }

    sql = text(f"""
      select
        p.id, p.version, p.chain_id, p.fee_tier_bps, p.tick_spacing, p.created_at_ts,
        t0.address as t0_addr, t0.symbol as t0_sym,
        t1.address as t1_addr, t1.symbol as t1_sym,
        sum(coalesce(a.approx_fee_token0,0) + coalesce(a.approx_fee_token1,0)) as fees_sum
      from {agg_table} a
      join pools p on p.id = a.pool_id
      join tokens t0 on t0.id = p.token0_id
      join tokens t1 on t1.id = p.token1_id
      where {where}
      group by p.id, p.version, p.chain_id, p.fee_tier_bps, p.tick_spacing, p.created_at_ts,
               t0.address, t0.symbol, t1.address, t1.symbol
      order by fees_sum desc
      limit :limit
    """).bindparams(
        bindparam("version", type_=SmallInteger), bindparam("token", type_=String),
        bindparam("token_symbol", type_=String), bindparam("ts_like", type_=String),
        bindparam("fee_min", type_=Integer), bindparam("fee_max", type_=Integer),
        bindparam("th", type_=Integer), bindparam("limit", type_=Integer)
    )

    async with SessionLocal() as session:
        rows = (await session.execute(sql, params)).mappings().all()

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["pool_id","version","chain_id","token0","token1","fee_tier_bps","tick_spacing","created_at_ts","fees_sum","window"])
    for r in rows:
        w.writerow([r["id"], r["version"], r["chain_id"],
                    (r["t0_sym"] or r["t0_addr"]), (r["t1_sym"] or r["t1_addr"]),
                    r["fee_tier_bps"], r["tick_spacing"], r["created_at_ts"],
                    float(r["fees_sum"]), window])
    buf.seek(0)
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv")

@app.get("/export/top_volume.csv")
async def export_top_volume_csv(
    window: Literal["day","hour"] = Query("day"),
    side: Literal["both","token0","token1"] = Query("both"),
    lookback: int = Query(30, ge=1),
    since_day_id: Optional[int] = Query(None),
    since_hour_id: Optional[int] = Query(None),
    version: Optional[int] = Query(None),
    token: Optional[str] = Query(None),
    token_symbol: Optional[str] = Query(None),
    fee_min: Optional[int] = Query(None, ge=0),
    fee_max: Optional[int] = Query(None, ge=0),
    limit: int = Query(100, ge=1, le=100),
):
    token_norm = token.lower() if token else None
    th, agg_table, agg_field = _window_threshold(window, lookback, since_day_id, since_hour_id)

    vol_expr = {
        "both": "sum(coalesce(a.volume_token0,0) + coalesce(a.volume_token1,0))",
        "token0": "sum(coalesce(a.volume_token0,0))",
        "token1": "sum(coalesce(a.volume_token1,0))",
    }[side]

    where = f"""
      (:version is null or p.version = :version)
      and (:token is null or p.token0_id = :token or p.token1_id = :token)
      and (:token_symbol is null or t0.symbol ilike :ts_like or t1.symbol ilike :ts_like)
      and (:fee_min is null or p.fee_tier_bps >= :fee_min)
      and (:fee_max is null or p.fee_tier_bps <= :fee_max)
      and a.{agg_field} >= :th
    """
    ts_like = f"%{token_symbol}%" if token_symbol else None
    params = {
        "version": version, "token": token_norm, "token_symbol": token_symbol, "ts_like": ts_like,
        "fee_min": fee_min, "fee_max": fee_max, "th": th, "limit": limit
    }

    sql = text(f"""
      select
        p.id, p.version, p.chain_id, p.fee_tier_bps, p.tick_spacing, p.created_at_ts,
        t0.address as t0_addr, t0.symbol as t0_sym,
        t1.address as t1_addr, t1.symbol as t1_sym,
        {vol_expr} as volume_sum,
        sum(coalesce(a.swap_count,0)) as swaps
      from {agg_table} a
      join pools p on p.id = a.pool_id
      join tokens t0 on t0.id = p.token0_id
      join tokens t1 on t1.id = p.token1_id
      where {where}
      group by p.id, p.version, p.chain_id, p.fee_tier_bps, p.tick_spacing, p.created_at_ts,
               t0.address, t0.symbol, t1.address, t1.symbol
      order by volume_sum desc
      limit :limit
    """).bindparams(
        bindparam("version", type_=SmallInteger), bindparam("token", type_=String),
        bindparam("token_symbol", type_=String), bindparam("ts_like", type_=String),
        bindparam("fee_min", type_=Integer), bindparam("fee_max", type_=Integer),
        bindparam("th", type_=Integer), bindparam("limit", type_=Integer)
    )

    async with SessionLocal() as session:
        rows = (await session.execute(sql, params)).mappings().all()

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["pool_id","version","chain_id","token0","token1","fee_tier_bps","tick_spacing","created_at_ts","volume_sum","swaps_sum","side","window"])
    for r in rows:
        w.writerow([r["id"], r["version"], r["chain_id"],
                    (r["t0_sym"] or r["t0_addr"]), (r["t1_sym"] or r["t1_addr"]),
                    r["fee_tier_bps"], r["tick_spacing"], r["created_at_ts"],
                    float(r["volume_sum"]), int(r["swaps"]), side, window])
    buf.seek(0)
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv")

from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pathlib import Path as _Path

STATIC_DIR = _Path("/Users/axel/Dev/open-source/uniswap-lp-analytics/backend/api/static")
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    @app.get("/")
    async def index():
        return FileResponse(STATIC_DIR / "index.html")

# --------- Per-pool aggregation (JSON) ---------
from typing import TypedDict

class PoolAggRow(TypedDict):
    bucket: int
    volume_token0: float
    volume_token1: float
    approx_fee_token0: float
    approx_fee_token1: float
    swap_count: int

@app.get("/pools/{pool_id}/agg")
async def pool_agg(
    pool_id: str,
    window: Literal["day","hour"] = Query("day"),
    lookback: int = Query(30, ge=1),
    since_day_id: Optional[int] = Query(None),
    since_hour_id: Optional[int] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
):
    th, agg_table, agg_field = _window_threshold(window, lookback, since_day_id, since_hour_id)

    pool_sql = text("""
      select
        p.id, p.version, p.chain_id, p.fee_tier_bps, p.tick_spacing, p.created_at_ts,
        t0.address as t0_addr, t0.symbol as t0_sym, t0.decimals as t0_dec,
        t1.address as t1_addr, t1.symbol as t1_sym, t1.decimals as t1_dec
      from pools p
      join tokens t0 on t0.id = p.token0_id
      join tokens t1 on t1.id = p.token1_id
      where p.id = :pool_id
    """).bindparams(bindparam("pool_id", type_=String))

    data_sql = text(f"""
      select
        a.{agg_field} as bucket,
        coalesce(a.volume_token0,0) as volume_token0,
        coalesce(a.volume_token1,0) as volume_token1,
        coalesce(a.approx_fee_token0,0) as approx_fee_token0,
        coalesce(a.approx_fee_token1,0) as approx_fee_token1,
        coalesce(a.swap_count,0) as swap_count
      from {agg_table} a
      where a.pool_id = :pool_id and a.{agg_field} >= :th
      order by a.{agg_field} desc
      limit :limit
    """).bindparams(
        bindparam("pool_id", type_=String),
        bindparam("th", type_=Integer),
        bindparam("limit", type_=Integer),
    )

    async with SessionLocal() as session:
        prow = (await session.execute(pool_sql, {"pool_id": pool_id})).mappings().first()
        if not prow:
            raise HTTPException(status_code=404, detail="Pool not found")
        rows = (await session.execute(data_sql, {"pool_id": pool_id, "th": th, "limit": limit})).mappings().all()

    pool = {
        "id": prow["id"], "version": prow["version"], "chain_id": prow["chain_id"],
        "token0": {"address": prow["t0_addr"], "symbol": prow["t0_sym"], "decimals": prow["t0_dec"]},
        "token1": {"address": prow["t1_addr"], "symbol": prow["t1_sym"], "decimals": prow["t1_dec"]},
        "fee_tier_bps": prow["fee_tier_bps"], "tick_spacing": prow["tick_spacing"], "created_at_ts": prow["created_at_ts"],
    }
    out_rows: List[PoolAggRow] = []
    for r in rows:
        out_rows.append(PoolAggRow(
            bucket=int(r["bucket"]),
            volume_token0=float(r["volume_token0"]),
            volume_token1=float(r["volume_token1"]),
            approx_fee_token0=float(r["approx_fee_token0"]),
            approx_fee_token1=float(r["approx_fee_token1"]),
            swap_count=int(r["swap_count"]),
        ))
    return {"pool": pool, "window": window, "rows": out_rows}

# --------- Per-pool aggregation (CSV export) ---------
@app.get("/export/pool_agg.csv")
async def export_pool_agg_csv(
    pool_id: str = Query(...),
    window: Literal["day","hour"] = Query("day"),
    lookback: int = Query(30, ge=1),
    since_day_id: Optional[int] = Query(None),
    since_hour_id: Optional[int] = Query(None),
    limit: int = Query(100, ge=1, le=10000),
):
    th, agg_table, agg_field = _window_threshold(window, lookback, since_day_id, since_hour_id)

    sql = text(f"""
      select
        a.{agg_field} as bucket,
        coalesce(a.volume_token0,0) as volume_token0,
        coalesce(a.volume_token1,0) as volume_token1,
        coalesce(a.approx_fee_token0,0) as approx_fee_token0,
        coalesce(a.approx_fee_token1,0) as approx_fee_token1,
        coalesce(a.swap_count,0) as swap_count
      from {agg_table} a
      where a.pool_id = :pool_id and a.{agg_field} >= :th
      order by a.{agg_field} desc
      limit :limit
    """).bindparams(
        bindparam("pool_id", type_=String),
        bindparam("th", type_=Integer),
        bindparam("limit", type_=Integer),
    )

    async with SessionLocal() as session:
        rows = (await session.execute(sql, {"pool_id": pool_id, "th": th, "limit": limit})).mappings().all()

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["pool_id","window","bucket","volume_token0","volume_token1","approx_fee_token0","approx_fee_token1","swap_count"])
    for r in rows:
        w.writerow([pool_id, window, int(r["bucket"]), float(r["volume_token0"]), float(r["volume_token1"]),
                    float(r["approx_fee_token0"]), float(r["approx_fee_token1"]), int(r["swap_count"])])
    buf.seek(0)
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv")
import os, httpx

@app.get("/sync/status")
async def sync_status():
    async with SessionLocal() as session:
        pools = (await session.execute(text("select count(*) from pools"))).scalar_one()
        day_rows = (await session.execute(text("select count(*) from pool_day_data"))).scalar_one()
        hour_rows = (await session.execute(text("select count(*) from pool_hour_data"))).scalar_one()
        max_day = (await session.execute(text("select coalesce(max(date),0) from pool_day_data"))).scalar_one()
        max_hour = (await session.execute(text("select coalesce(max(hour_start_unix),0) from pool_hour_data"))).scalar_one()

    endpoint = os.environ.get("GRAPH_ENDPOINT")
    sg_block = None
    has_err = None
    if endpoint:
        q = {"query": "{ _meta { block { number } hasIndexingErrors } }"}
        try:
            async with httpx.AsyncClient(timeout=10) as ac:
                r = await ac.post(endpoint, json=q)
                j = r.json()
                m = (j.get("data") or {}).get("_meta") or {}
                b = (m.get("block") or {}).get("number")
                sg_block = b
                has_err = m.get("hasIndexingErrors")
        except Exception:
            pass

    return {
        "db": {
            "pools": int(pools),
            "pool_day_rows": int(day_rows),
            "pool_hour_rows": int(hour_rows),
            "max_day_id": int(max_day),
            "max_hour_id": int(max_hour),
        },
        "subgraph": {
            "endpoint": endpoint,
            "block_number": sg_block,
            "hasIndexingErrors": has_err,
        },
    }
