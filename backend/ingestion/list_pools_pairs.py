import os, json, asyncio, argparse
from typing import List, Tuple, Dict, Any, Set
from gql import Client, gql
from gql.transport.httpx import HTTPXAsyncTransport
from dotenv import load_dotenv

# Query only the exact pair, orientation A/B
Q_PAIR = gql("""
query PoolsByPair($first: Int!, $skip: Int!, $version: Int!, $a: Bytes!, $b: Bytes!) {
  pools(
    first: $first
    skip: $skip
    orderBy: createdAtTimestamp
    orderDirection: asc
    where: { version: $version, token0: $a, token1: $b }
  ) {
    id
    createdAtTimestamp
    feeTierBps
    tickSpacing
    token0 { id symbol decimals }
    token1 { id symbol decimals }
  }
}
""")

def parse_pairs_addrs(pairs_str: str) -> List[Tuple[str, str]]:
    pairs: List[Tuple[str, str]] = []
    for raw in pairs_str.split(","):
        raw = raw.strip()
        if not raw:
            continue
        if "/" not in raw:
            raise ValueError(f"Pair '{raw}' must be 'addrA/addrB'")
        a, b = raw.split("/", 1)
        pairs.append((a.lower(), b.lower()))
    return pairs

async def fetch_pair(session: Client, version: int, a: str, b: str, page_size: int = 200) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    skip = 0
    while True:
        data = await session.execute(
            Q_PAIR,
            variable_values={"first": page_size, "skip": skip, "version": version, "a": a, "b": b},
        )
        items = data.get("pools") or []
        out.extend(items)
        if len(items) < page_size:
            break
        skip += page_size
    return out

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", type=int, choices=[3,4], required=True)
    parser.add_argument("--pairs-addrs", type=str, required=True,
                        help="CSV of pairs as 'addrA/addrB,addrX/addrY'. Both orientations will be queried.")
    parser.add_argument("--page-size", type=int, default=200)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--out", type=str, default="")
    args = parser.parse_args()

    load_dotenv()
    endpoint = os.getenv("GRAPH_ENDPOINT")
    if not endpoint:
        raise SystemExit("GRAPH_ENDPOINT is not set")

    transport = HTTPXAsyncTransport(url=endpoint, timeout=args.timeout)
    async with Client(transport=transport, fetch_schema_from_transport=False,) as session:
        pairs = parse_pairs_addrs(args.pairs_addrs)

        seen: Set[str] = set()
        pools: List[Dict[str, Any]] = []

        for (a, b) in pairs:
            # orientation A/B
            items_ab = await fetch_pair(session, args.version, a, b, args.page_size)
            # orientation B/A
            items_ba = await fetch_pair(session, args.version, b, a, args.page_size)

            for item in items_ab + items_ba:
                pid = item["id"].lower()
                if pid in seen:
                    continue
                seen.add(pid)
                pools.append(item)

        out_path = args.out or f"backend/ingestion/output/pools.filtered.v{args.version}.json"
        with open(out_path, "w") as f:
            json.dump({"pools": pools}, f, indent=2)

        print(f"[version={args.version}] pairs={len(pairs)} -> pools matched: {len(pools)}")
        for p in pools:
            t0 = p["token0"]; t1 = p["token1"]
            print(f" - v{args.version} {p['id']} :: {t0.get('symbol')}/{t1.get('symbol')} feeTierBps={p['feeTierBps']} tickSpacing={p['tickSpacing']} createdAt={p['createdAtTimestamp']}")
        print(f"Saved: {out_path}")

if __name__ == "__main__":
    asyncio.run(main())
