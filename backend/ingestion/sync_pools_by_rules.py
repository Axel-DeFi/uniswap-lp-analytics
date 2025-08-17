import os, sys, json, argparse, pathlib
from typing import Dict, Any, List, Set
import httpx
import yaml

QUERY = """
query PoolsByTokens($first:Int!, $skip:Int!, $tokens:[Bytes!], $versions:[Int!]) {
  pools(
    first: $first
    skip: $skip
    where: {
      version_in: $versions
      token0_in: $tokens
      token1_in: $tokens
    }
    orderBy: createdAtTimestamp
    orderDirection: asc
  ) {
    id
    version
    createdAtTimestamp
    feeTierBps
    tickSpacing
    token0 { id symbol decimals }
    token1 { id symbol decimals }
  }
}
"""

def load_rules(path: pathlib.Path, chain_id: str) -> Dict[str, Any]:
    if not path.exists():
        print(f"Rules file not found: {path}", file=sys.stderr)
        sys.exit(2)
    data = yaml.safe_load(path.read_text()) or {}
    ruleset = data.get(chain_id)
    if not ruleset:
        print(f"No rules for chain {chain_id} in {path}", file=sys.stderr)
        sys.exit(2)
    return ruleset

def fetch_pools(client: httpx.Client, endpoint: str, versions: List[int], tokens: List[str], page_size: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    skip = 0
    while True:
        variables = {
            "first": page_size,
            "skip": skip,
            "tokens": [t.lower() for t in tokens],
            "versions": versions,
        }
        r = client.post(endpoint, json={"query": QUERY, "variables": variables}, timeout=60)
        r.raise_for_status()
        payload = r.json()
        if "errors" in payload:
            raise RuntimeError(payload["errors"])
        batch = payload.get("data", {}).get("pools", []) or []
        out.extend(batch)
        if len(batch) < page_size:
            break
        skip += page_size
    return out

def pair_ok(t0: str, t1: str, groupA: Set[str], groupB: Set[str]) -> bool:
    t0 = t0.lower(); t1 = t1.lower()
    return (t0 in groupA and t1 in groupB) or (t0 in groupB and t1 in groupA)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--chain-id", type=int, default=1)
    ap.add_argument("--version", choices=["3", "4", "all"], default="all")
    ap.add_argument("--page-size", type=int, default=1000)
    ap.add_argument("--rules", default="backend/config/pair_rules.yaml")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    endpoint = os.environ.get("GRAPH_ENDPOINT")
    if not endpoint:
        print("GRAPH_ENDPOINT is not set", file=sys.stderr)
        sys.exit(2)

    rules = load_rules(pathlib.Path(args.rules), str(args.chain_id))
    allowed_versions = rules.get("versions") or []
    if args.version != "all":
        forced = int(args.version)
        versions = [forced] if forced in allowed_versions or not allowed_versions else [forced]
    else:
        versions = allowed_versions or [3, 4]

    all_pools: Dict[str, Dict[str, Any]] = {}

    with httpx.Client() as client:
        for rule in (rules.get("rules") or []):
            groupA = set([a.lower() for a in (rule.get("groupA") or [])])
            groupB = set([b.lower() for b in (rule.get("groupB") or [])])
            if not groupA or not groupB:
                continue
            fee_tiers_cfg = rule.get("fee_tiers_bps") or []
            fee_whitelist: Set[int] = set(int(x) for x in fee_tiers_cfg) if fee_tiers_cfg else set()

            token_universe = sorted(list(groupA.union(groupB)))
            fetched = fetch_pools(client, endpoint, versions, token_universe, args.page_size)

            for p in fetched:
                t0 = p["token0"]["id"]
                t1 = p["token1"]["id"]
                if not pair_ok(t0, t1, groupA, groupB):
                    continue
                if fee_whitelist and int(p["feeTierBps"]) not in fee_whitelist:
                    continue
                all_pools[p["id"]] = {
                    "id": p["id"],
                    "version": int(p["version"]),
                    "createdAtTimestamp": int(p["createdAtTimestamp"]),
                    "feeTierBps": int(p["feeTierBps"]),
                    "tickSpacing": int(p["tickSpacing"]),
                    "token0": {
                        "id": p["token0"]["id"],
                        "symbol": p["token0"].get("symbol"),
                        "decimals": int(p["token0"].get("decimals") or 18),
                    },
                    "token1": {
                        "id": p["token1"]["id"],
                        "symbol": p["token1"].get("symbol"),
                        "decimals": int(p["token1"].get("decimals") or 18),
                    },
                }

    pools = sorted(all_pools.values(), key=lambda r: (r["version"], r["createdAtTimestamp"]))
    print(f"[chain {args.chain_id}] versions={versions} -> matched pools: {len(pools)}")
    for r in pools[:20]:
        t0 = r["token0"]["symbol"] or r["token0"]["id"][:6]
        t1 = r["token1"]["symbol"] or r["token1"]["id"][:6]
        print(f" - v{r['version']} {r['id']} :: {t0}/{t1} feeTierBps={r['feeTierBps']} tickSpacing={r['tickSpacing']} createdAt={r['createdAtTimestamp']}")

    out_path = args.out or f"backend/ingestion/output/pools.rules.chain{args.chain_id}.json"
    pathlib.Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    pathlib.Path(out_path).write_text(json.dumps({"pools": pools}, indent=2))
    print(f"Saved: {out_path}")

if __name__ == "__main__":
    main()
