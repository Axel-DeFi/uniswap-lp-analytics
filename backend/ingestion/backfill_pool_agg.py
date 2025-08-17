#!/usr/bin/env python3
# Backfill daily/hourly aggregates for pools from our subgraph into Postgres, pool-by-pool.

import os, sys, argparse, asyncio
from pathlib import Path
from typing import List, Dict, Any, Optional
from decimal import Decimal
from dotenv import load_dotenv
from gql import Client, gql
from gql.transport.httpx import HTTPXAsyncTransport
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

ROOT = Path("/Users/axel/Dev/open-source/uniswap-lp-analytics")

Q_DAY_ONE = gql("""
query DayAggOne($first:Int!, $skip:Int!, $poolId:String!){
  poolDayDatas(
    first:$first, skip:$skip,
    where:{ pool: $poolId },
    orderBy: date, orderDirection: asc
  ){
    id
    date
    pool { id feeTierBps }
    volumeToken0
    volumeToken1
    swapCount
  }
}
""")

Q_HOUR_ONE = gql("""
query HourAggOne($first:Int!, $skip:Int!, $poolId:String!){
  poolHourDatas(
    first:$first, skip:$skip,
    where:{ pool: $poolId },
    orderBy: hourStartUnix, orderDirection: asc
  ){
    id
    hourStartUnix
    pool { id feeTierBps }
    volumeToken0
    volumeToken1
    swapCount
  }
}
""")

def to_dec(x: Optional[str]) -> Optional[Decimal]:
    if x is None: return None
    return Decimal(str(x))

def approx_fee(amount: Optional[Decimal], fee_bps: int) -> Optional[Decimal]:
    if amount is None: return None
    return (amount * Decimal(fee_bps) / Decimal(10000))

async def fetch_all_for_pool(session: Client, pool_id: str, page_size: int):
    day_rows: List[Dict[str, Any]] = []
    hour_rows: List[Dict[str, Any]] = []

    skip = 0
    while True:
        data = await session.execute(Q_DAY_ONE, variable_values={"first": page_size, "skip": skip, "poolId": pool_id})
        rows = data.get("poolDayDatas", [])
        if not rows:
            break
        day_rows.extend(rows)
        if len(rows) < page_size:
            break
        skip += page_size

    skip = 0
    while True:
        data = await session.execute(Q_HOUR_ONE, variable_values={"first": page_size, "skip": skip, "poolId": pool_id})
        rows = data.get("poolHourDatas", [])
        if not rows:
            break
        hour_rows.extend(rows)
        if len(rows) < page_size:
            break
        skip += page_size

    return day_rows, hour_rows

async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--version", type=int, default=3, choices=[3,4])
    ap.add_argument("--page-size", type=int, default=500)
    ap.add_argument("--timeout", type=float, default=60.0)
    args = ap.parse_args()

    load_dotenv(ROOT / ".env", override=True)
    endpoint = os.environ.get("GRAPH_ENDPOINT")
    if not endpoint:
        print("ERROR: GRAPH_ENDPOINT is not set", file=sys.stderr); sys.exit(2)
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL is not set", file=sys.stderr); sys.exit(2)

    engine = create_async_engine(db_url, future=True)

    async with engine.begin() as conn:
        rows = (await conn.execute(text("select id, fee_tier_bps from pools where version = :v order by created_at_ts desc"), {"v": args.version})).mappings().all()
    if not rows:
        print("No pools found for selected version"); sys.exit(0)

    pool_ids = [r["id"] for r in rows]
    fee_map = {r["id"]: int(r["fee_tier_bps"]) for r in rows}

    transport = HTTPXAsyncTransport(url=endpoint, timeout=args.timeout)
    client = Client(transport=transport, fetch_schema_from_transport=False, execute_timeout=args.timeout + 30)

    up_day = text("""
    insert into pool_day_data (id, pool_id, date, volume_token0, volume_token1, approx_fee_token0, approx_fee_token1, swap_count)
    values (:id, :pool_id, :date, :v0, :v1, :f0, :f1, :sc)
    on conflict (id) do update set
      volume_token0 = excluded.volume_token0,
      volume_token1 = excluded.volume_token1,
      approx_fee_token0 = excluded.approx_fee_token0,
      approx_fee_token1 = excluded.approx_fee_token1,
      swap_count = excluded.swap_count
    """)

    up_hour = text("""
    insert into pool_hour_data (id, pool_id, hour_start_unix, volume_token0, volume_token1, approx_fee_token0, approx_fee_token1, swap_count)
    values (:id, :pool_id, :hs, :v0, :v1, :f0, :f1, :sc)
    on conflict (id) do update set
      volume_token0 = excluded.volume_token0,
      volume_token1 = excluded.volume_token1,
      approx_fee_token0 = excluded.approx_fee_token0,
      approx_fee_token1 = excluded.approx_fee_token1,
      swap_count = excluded.swap_count
    """)

    total_day = total_hour = 0

    async with client as session:
        async with engine.begin() as conn:
            for i, pid in enumerate(pool_ids, 1):
                drows, hrows = await fetch_all_for_pool(session, pid, args.page_size)

                # Map and upsert day rows
                if drows:
                    payload = []
                    for r in drows:
                        fee = fee_map.get(pid, int(r["pool"]["feeTierBps"]))
                        v0 = to_dec(r.get("volumeToken0"))
                        v1 = to_dec(r.get("volumeToken1"))
                        payload.append({
                            "id": r["id"],
                            "pool_id": pid,
                            "date": int(r["date"]),
                            "v0": v0, "v1": v1,
                            "f0": approx_fee(v0, fee),
                            "f1": approx_fee(v1, fee),
                            "sc": int(r.get("swapCount") or 0),
                        })
                    await conn.execute(up_day, payload)
                    total_day += len(payload)

                # Map and upsert hour rows
                if hrows:
                    payload = []
                    for r in hrows:
                        fee = fee_map.get(pid, int(r["pool"]["feeTierBps"]))
                        v0 = to_dec(r.get("volumeToken0"))
                        v1 = to_dec(r.get("volumeToken1"))
                        payload.append({
                            "id": r["id"],
                            "pool_id": pid,
                            "hs": int(r["hourStartUnix"]),
                            "v0": v0, "v1": v1,
                            "f0": approx_fee(v0, fee),
                            "f1": approx_fee(v1, fee),
                            "sc": int(r.get("swapCount") or 0),
                        })
                    await conn.execute(up_hour, payload)
                    total_hour += len(payload)

                # Small progress line
                print(f"[{i}/{len(pool_ids)}] pool {pid} -> days={len(drows)}, hours={len(hrows)}")

    await engine.dispose()
    print(f"Upserted day rows: {total_day}; hour rows: {total_hour}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
