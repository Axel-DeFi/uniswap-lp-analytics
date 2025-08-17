#!/usr/bin/env python3
# Quick listing of pools from our unified subgraph (V3+V4) filtered by token whitelist.
# Uses version range (gte/lte) for robust filtering.

import os, json, re, argparse, asyncio, sys
from pathlib import Path
from typing import List, Dict, Any, Set, Optional
from dotenv import load_dotenv
from gql import Client, gql
from gql.transport.httpx import HTTPXAsyncTransport

ROOT = Path("/Users/axel/Dev/open-source/uniswap-lp-analytics")
CONFIG_PATH = ROOT / "config" / "tokens.json"
OUTPUT_DIR = ROOT / "backend" / "ingestion" / "output"

def load_jsonc(path: Path) -> dict:
    text = path.read_text(encoding="utf-8")
    lines = []
    for line in text.splitlines():
        m = re.search(r'(^|\s)//', line)
        if m:
            line = line[:m.start()].rstrip()
        lines.append(line)
    return json.loads("\n".join(lines))

def normalize_addr(addr: str) -> str:
    a = addr.strip().lower()
    if not a.startswith("0x") or len(a) != 42:
        raise ValueError(f"Invalid address: {addr}")
    return a

def chunked(lst: List[str], n: int) -> List[List[str]]:
    return [lst[i:i+n] for i in range(0, len(lst), n)]

def make_client(url: str, timeout_s: float) -> Client:
    transport = HTTPXAsyncTransport(url=url, timeout=timeout_s)
    return Client(transport=transport, fetch_schema_from_transport=False, execute_timeout=timeout_s + 30)

# Use version range filters; Graph Node supports *_gte / *_lte for Int.
Q_POOLS_T0 = gql("""
query PoolsT0($first:Int!, $skip:Int!, $tokens:[String!], $vmin:Int!, $vmax:Int!) {
  pools(
    first:$first
    skip:$skip
    where:{ token0_in: $tokens, version_gte: $vmin, version_lte: $vmax }
    orderBy: createdAtTimestamp
    orderDirection: desc
  ) {
    id
    version
    token0 { id symbol decimals }
    token1 { id symbol decimals }
    feeTierBps
    tickSpacing
    createdAtTimestamp
  }
}
""")

Q_POOLS_T1 = gql("""
query PoolsT1($first:Int!, $skip:Int!, $tokens:[String!], $vmin:Int!, $vmax:Int!) {
  pools(
    first:$first
    skip:$skip
    where:{ token1_in: $tokens, version_gte: $vmin, version_lte: $vmax }
    orderBy: createdAtTimestamp
    orderDirection: desc
  ) {
    id
    version
    token0 { id symbol decimals }
    token1 { id symbol decimals }
    feeTierBps
    tickSpacing
    createdAtTimestamp
  }
}
""")

async def fetch_side(session: Client, query, tokens: List[str], vmin: int, vmax: int, page_size: int, max_total: Optional[int]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for batch in chunked(tokens, 30):
        skip = 0
        while True:
            vars = {"first": page_size, "skip": skip, "tokens": batch, "vmin": vmin, "vmax": vmax}
            data = await session.execute(query, variable_values=vars)
            rows = data.get("pools", [])
            if not rows:
                break
            out.extend(rows)
            if max_total is not None and len(out) >= max_total:
                return out[:max_total]
            break
        if max_total is not None and len(out) >= max_total:
            return out[:max_total]
    return out

def uniq_by_id(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: Set[str] = set()
    res: List[Dict[str, Any]] = []
    for r in rows:
        pid = r["id"]
        if pid not in seen:
            seen.add(pid)
            res.append(r)
    return res

async def main():
    p = argparse.ArgumentParser()
    p.add_argument("--chain-id", type=int, default=1)
    p.add_argument("--version", type=str, default="all", choices=["3", "4", "all"])
    p.add_argument("--page-size", type=int, default=50)
    p.add_argument("--timeout", type=float, default=60.0)
    p.add_argument("--limit", type=int, default=10)
    args = p.parse_args()

    load_dotenv(ROOT / ".env", override=True)
    endpoint = os.environ.get("GRAPH_ENDPOINT")
    if not endpoint:
        print("ERROR: GRAPH_ENDPOINT is not set in .env", file=sys.stderr)
        sys.exit(2)

    cfg = load_jsonc(CONFIG_PATH)
    chains = cfg.get("chains", {})
    chain_key = str(args.chain_id)
    if chain_key not in chains:
        print(f"ERROR: chain {args.chain_id} not found in config {CONFIG_PATH}", file=sys.stderr)
        sys.exit(2)
    network = chains[chain_key].get("network", str(args.chain_id))
    tokens = [normalize_addr(t) for t in chains[chain_key].get("tokens", [])]
    if not tokens:
        print(f"[chain {args.chain_id} / {network}] whitelist empty -> nothing to fetch.")
        return

    if args.version == "3":
        vmin, vmax, label = 3, 3, "3"
    elif args.version == "4":
        vmin, vmax, label = 4, 4, "4"
    else:
        vmin, vmax, label = 3, 4, "all"

    client = make_client(endpoint, args.timeout)
    async with client as session:
        pools0 = await fetch_side(session, Q_POOLS_T0, tokens, vmin, vmax, args.page_size, args.limit)
        remain = max(0, args.limit - len(pools0)) if args.limit is not None else None
        pools1 = [] if remain == 0 else await fetch_side(session, Q_POOLS_T1, tokens, vmin, vmax, args.page_size, remain)
        merged = uniq_by_id(pools0 + pools1)
        if args.limit is not None and len(merged) > args.limit:
            merged = merged[:args.limit]
        merged.sort(key=lambda r: (r["version"], r["id"]))

    print(f"[chain {args.chain_id} / {network}] version={label} tokens={len(tokens)}")
    print(f"pools matched (limited): {len(merged)}")
    for r in merged[:20]:
        t0 = r['token0']['symbol'] or r['token0']['id'][:6]
        t1 = r['token1']['symbol'] or r['token1']['id'][:6]
        print(f" - v{r['version']} {r['id']} :: {t0}/{t1} feeTierBps={r['feeTierBps']} tickSpacing={r['tickSpacing']} createdAt={r['createdAtTimestamp']}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / f"pools.{network}.v{label}.json"
    with out_path.open("w", encoding="utf-8") as f:
        json.dump({"chainId": args.chain_id, "network": network, "version": label, "tokens": tokens, "pools": merged}, f, indent=2)
    print(f"Saved: {out_path}")

if __name__ == "__main__":
    asyncio.run(main())
