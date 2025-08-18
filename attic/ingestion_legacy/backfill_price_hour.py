import os
import sys
import asyncio
from typing import List, Optional, Tuple

from dotenv import load_dotenv
from gql import Client, gql
from gql.transport.httpx import HTTPXAsyncTransport

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

Q_PRICE_HOUR = gql("""
query PriceHour($pool: ID!, $first: Int!, $skip: Int!, $since: Int) {
  poolPriceHours(
    first: $first,
    skip: $skip,
    where: { pool: $pool, hourStartUnix_gt: $since }
    orderBy: hourStartUnix,
    orderDirection: asc
  ) {
    hourStartUnix
    sqrtPriceX96
    price0
    price1
    liquidity
    updatedAt
  }
}
""")

SQL_DB_LAST_HOUR = text("""
select coalesce(max(hour_start_unix), -1) as last_hour
from pool_price_hour
where pool_id = :pool_id
""")

SQL_SELECT_POOLS_BY_PAIR_ADDRS = text("""
select p.id
from pools p
where p.version = :version
  and (
    (p.token0_id = :a and p.token1_id = :b) or
    (p.token0_id = :b and p.token1_id = :a)
  )
""")

SQL_SELECT_POOLS_ALL = text("""
select id from pools
where version = :version
order by created_at_ts asc
""")

SQL_UPSERT = text("""
insert into pool_price_hour(
  pool_id, hour_start_unix, sqrt_price_x96, price0, price1, liquidity, updated_at
) values (
  :pool_id, :hour_start_unix, :sqrt_price_x96, :price0, :price1, :liquidity, :updated_at
)
on conflict (pool_id, hour_start_unix) do update
set sqrt_price_x96 = excluded.sqrt_price_x96,
    price0 = excluded.price0,
    price1 = excluded.price1,
    liquidity = excluded.liquidity,
    updated_at = excluded.updated_at
""")

def parse_pairs_addrs(arg: Optional[str]) -> List[Tuple[str, str]]:
  if not arg:
    return []
  out: List[Tuple[str, str]] = []
  for item in arg.split(","):
    item = item.strip()
    if not item:
      continue
    if "/" not in item:
      print(f"Invalid pair format (expected addrA/addrB): {item}", file=sys.stderr)
      sys.exit(2)
    a, b = item.split("/", 1)
    out.append((a.strip().lower(), b.strip().lower()))
  return out

async def fetch_price_hours(session: Client, pool_id: str, first: int, since: int) -> List[dict]:
  results: List[dict] = []
  skip = 0
  while True:
    data = await session.execute(Q_PRICE_HOUR, variable_values={
      "pool": pool_id, "first": first, "skip": skip, "since": since
    })
    rows = data.get("poolPriceHours") or []
    results.extend(rows)
    if len(rows) < first:
      break
    skip += first
  return results

async def main():
  import argparse
  ap = argparse.ArgumentParser()
  ap.add_argument("--version", type=int, default=3)
  ap.add_argument("--page-size", type=int, default=500)
  ap.add_argument("--pairs-addrs", type=str, default=None,
                  help="Comma-separated list of address pairs like '0xA/0xB,0xC/0xD'")
  ap.add_argument("--limit-pools", type=int, default=None)
  args = ap.parse_args()

  load_dotenv()
  endpoint = os.getenv("GRAPH_ENDPOINT")
  if not endpoint:
    print("GRAPH_ENDPOINT is not set", file=sys.stderr)
    sys.exit(2)

  dsn = os.getenv("DATABASE_URL", "postgresql+asyncpg://localhost/uniswap_lp_analytics")
  engine = create_async_engine(dsn, future=True)

  pairs = parse_pairs_addrs(args.pairs_addrs)

  pool_ids: List[str] = []
  async with engine.begin() as conn:
    if pairs:
      for a, b in pairs:
        rows = (await conn.execute(SQL_SELECT_POOLS_BY_PAIR_ADDRS, {"version": args.version, "a": a, "b": b})).all()
        pool_ids.extend([r[0] for r in rows])
    else:
      rows = (await conn.execute(SQL_SELECT_POOLS_ALL, {"version": args.version})).all()
      pool_ids = [r[0] for r in rows]

  # de-duplicate preserving order
  seen = set()
  pool_ids = [pid for pid in pool_ids if not (pid in seen or seen.add(pid))]
  if args.limit_pools:
    pool_ids = pool_ids[: args.limit_pools]

  if not pool_ids:
    print("No pools matched selection.", file=sys.stderr)
    sys.exit(1)

  transport = HTTPXAsyncTransport(url=endpoint, timeout=60.0)
  total_rows = 0
  async with Client(transport=transport, fetch_schema_from_transport=False) as session:
    for i, pid in enumerate(pool_ids, 1):
      async with engine.begin() as conn:
        last_hour = (await conn.execute(SQL_DB_LAST_HOUR, {"pool_id": pid})).scalar_one()
      rows = await fetch_price_hours(session, pid, args.page_size, last_hour)
      if rows:
        async with engine.begin() as conn:
          for r in rows:
            await conn.execute(SQL_UPSERT, {
              "pool_id": pid,
              "hour_start_unix": int(r["hourStartUnix"]),
              "sqrt_price_x96": str(r["sqrtPriceX96"]),
              "price0": str(r["price0"]),
              "price1": str(r["price1"]),
              "liquidity": str(r["liquidity"]),
              "updated_at": int(r["updatedAt"]),
            })
        print(f"[{i}/{len(pool_ids)}] {pid} -> inserted/updated {len(rows)} rows")
        total_rows += len(rows)
      else:
        print(f"[{i}/{len(pool_ids)}] {pid} -> up to date")
  print(f"Done. Upserted rows: {total_rows}")

if __name__ == "__main__":
  asyncio.run(main())
