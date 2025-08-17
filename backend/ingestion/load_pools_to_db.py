#!/usr/bin/env python3
# Load pools and tokens from a JSON produced by list_pools_unified.py into PostgreSQL.

import os, json, argparse, asyncio
from pathlib import Path
from typing import Dict, Any, List
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

ROOT = Path("/Users/axel/Dev/open-source/uniswap-lp-analytics")

def laddr(a: str) -> str:
    return a.lower().strip()

def to_int(x) -> int:
    if x is None:
        return 0
    if isinstance(x, int):
        return x
    return int(str(x))

async def upsert_tokens(conn, rows: List[Dict[str, Any]]):
    sql = text("""
    insert into tokens (id, address, symbol, name, decimals, chain_id, created_at_ts)
    values (:id, :address, :symbol, :name, :decimals, :chain_id, :created_at_ts)
    on conflict (id) do update set
      symbol = coalesce(excluded.symbol, tokens.symbol),
      name = coalesce(excluded.name, tokens.name),
      decimals = excluded.decimals
    """)
    if rows:
        await conn.execute(sql, rows)

async def upsert_pools(conn, rows: List[Dict[str, Any]]):
    sql = text("""
    insert into pools (id, version, chain_id, token0_id, token1_id, fee_tier_bps, tick_spacing, created_at_ts)
    values (:id, :version, :chain_id, :token0_id, :token1_id, :fee_tier_bps, :tick_spacing, :created_at_ts)
    on conflict (id) do update set
      version = excluded.version,
      chain_id = excluded.chain_id,
      token0_id = excluded.token0_id,
      token1_id = excluded.token1_id,
      fee_tier_bps = excluded.fee_tier_bps,
      tick_spacing = excluded.tick_spacing,
      created_at_ts = excluded.created_at_ts
    """)
    if rows:
        await conn.execute(sql, rows)

async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Path to pools JSON (produced by list_pools_unified.py)")
    ap.add_argument("--chain-id", type=int, default=1)
    args = ap.parse_args()

    load_dotenv(ROOT / ".env", override=True)
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise SystemExit("DATABASE_URL is not set in .env")

    data = json.loads(Path(args.input).read_text(encoding="utf-8"))
    pools = data.get("pools", [])
    if not pools:
        print("No pools in input JSON")
        return

    # Prepare token upserts (collect first-seen createdAt for token rows)
    token_rows: Dict[str, Dict[str, Any]] = {}
    for p in pools:
        created = to_int(p.get("createdAtTimestamp"))
        for side in ("token0", "token1"):
            t = p.get(side) or {}
            tid = laddr(t.get("id", ""))
            if not tid:
                continue
            row = token_rows.get(tid)
            candidate = {
                "id": tid,
                "address": tid,
                "symbol": t.get("symbol"),
                "name": t.get("name"),
                "decimals": to_int(t.get("decimals") or 18),
                "chain_id": args.chain_id,
                "created_at_ts": created or 0,
            }
            if row is None or candidate["created_at_ts"] < row["created_at_ts"]:
                token_rows[tid] = candidate

    pool_rows: List[Dict[str, Any]] = []
    for p in pools:
        pool_rows.append({
            "id": laddr(p["id"]),
            "version": int(p.get("version") or data.get("version") or 0) if str(data.get("version")).isdigit() else int(p.get("version") or 0),
            "chain_id": args.chain_id,
            "token0_id": laddr((p.get("token0") or {}).get("id", "")),
            "token1_id": laddr((p.get("token1") or {}).get("id", "")),
            "fee_tier_bps": to_int(p.get("feeTierBps")),
            "tick_spacing": to_int(p.get("tickSpacing")),
            "created_at_ts": to_int(p.get("createdAtTimestamp")),
        })

    engine = create_async_engine(db_url, future=True)
    async with engine.begin() as conn:
        await upsert_tokens(conn, list(token_rows.values()))
        await upsert_pools(conn, pool_rows)
    await engine.dispose()
    print(f"Upserted tokens: {len(token_rows)}; pools: {len(pool_rows)}")

if __name__ == "__main__":
    asyncio.run(main())
