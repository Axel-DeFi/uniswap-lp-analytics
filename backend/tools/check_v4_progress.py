#!/usr/bin/env python3
# Prints unified subgraph sync progress towards V4 startBlock and shows sample V4 pools if any.

import os, json, re, pathlib, urllib.request, sys
from dotenv import load_dotenv

ROOT = pathlib.Path("/Users/axel/Dev/open-source/uniswap-lp-analytics")
ENV_PATH = ROOT / ".env"
SUBGRAPH_YAML = ROOT / "subgraphs" / "unified" / "subgraph.yaml"

def read_v4_startblock() -> int:
    try:
        text = SUBGRAPH_YAML.read_text(encoding="utf-8")
        i = text.find("name: PoolManager")
        if i >= 0:
            m = re.search(r"startBlock:\s*(\d+)", text[i:])
            if m:
                return int(m.group(1))
    except Exception:
        pass
    return 21688329  # fallback

def post_query(url: str, query: str) -> dict:
    data = json.dumps({"query": query}).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))

def main():
    load_dotenv(ENV_PATH, override=True)
    endpoint = os.environ.get("GRAPH_ENDPOINT")
    if not endpoint:
        print("GRAPH_ENDPOINT is not set in .env", file=sys.stderr)
        sys.exit(2)

    target = read_v4_startblock()
    q = """
    {
      _meta { block { number } hasIndexingErrors }
      pools(first: 3, where: {version: 4}, orderBy: createdAtTimestamp, orderDirection: desc) {
        id createdAtTimestamp
        token0 { id symbol }
        token1 { id symbol }
        feeTierBps tickSpacing
      }
    }
    """.strip()

    data = post_query(endpoint, q)
    meta = (data.get("data") or {}).get("_meta") or {}
    cur = ((meta.get("block") or {}).get("number"))
    pools = (data.get("data") or {}).get("pools") or []
    if cur is None:
        print("No _meta.block.number in response"); sys.exit(2)

    remaining = max(0, target - cur)
    pct = min(100.0, (cur / target) * 100.0)
    print(f"Endpoint:              {endpoint}")
    print(f"Subgraph synced block: {cur}")
    print(f"V4 startBlock:         {target}")
    print(f"Remaining to start:    {remaining}")
    print(f"Progress to V4 start:  {pct:.2f}%")
    print(f"V4 pools returned:     {len(pools)}")
    for r in pools:
        t0 = (r.get('token0') or {}).get('symbol') or ((r.get('token0') or {}).get('id') or '')[:6]
        t1 = (r.get('token1') or {}).get('symbol') or ((r.get('token1') or {}).get('id') or '')[:6]
        print(f" - {r['id']} :: {t0}/{t1} feeTierBps={r['feeTierBps']} tickSpacing={r['tickSpacing']} createdAt={r['createdAtTimestamp']}")
    print("Done.")
if __name__ == "__main__":
    main()
