#!/usr/bin/env python3
import os, sys, json, httpx
from pathlib import Path

REPO="/Users/axel/Dev/open-source/uniswap-lp-analytics"
CFG_PATH=f"{REPO}/backend/config/subgraphs.json"
NET_OUT=f"{REPO}/subgraphs/unified/networks.json"

net = sys.argv[1] if len(sys.argv)>1 else "optimism"
rpc = os.getenv("OP_RPC_URL","https://mainnet.optimism.io")

cfg = json.loads(Path(CFG_PATH).read_text())
if net not in cfg:
    raise SystemExit(f"Network not found in subgraphs.json: {net}")

meta = cfg[net]
ds   = meta.get("dataSources",{})

addresses = {}
for key in ("v3Factory","v4PoolManager"):
    if key in ds and ds[key].get("address"):
        addresses[key] = ds[key]["address"]

if not addresses:
    raise SystemExit("No addresses found in subgraphs.json for selected network")

cli = httpx.Client(timeout=60)

def rpc_call(method, params):
    r = cli.post(rpc, json={"jsonrpc":"2.0","id":1,"method":method,"params":params})
    r.raise_for_status()
    j = r.json()
    if "error" in j:
        raise RuntimeError(j["error"])
    return j["result"]

latest = int(rpc_call("eth_blockNumber", []), 16)

def has_logs(addr, frm, to):
    if frm < 0: frm = 0
    if to < frm: to = frm
    res = rpc_call("eth_getLogs", [{"address":addr, "fromBlock":hex(frm), "toBlock":hex(to)}])
    return len(res) > 0

def earliest_log_block(addr):
    lo, hi = 0, latest
    found = False
    while lo <= hi:
        mid = (lo + hi) // 2
        if has_logs(addr, mid, hi):
            found = True
            hi = mid - 1
        else:
            lo = mid + 1
    return lo if found else None

start_blocks = {}
for name, addr in addresses.items():
    b = earliest_log_block(addr)
    if b is None:
        # на крайний случай — старт с 0, лучше потом уточнить
        b = 0
    start_blocks[name] = b

networks = {}
if Path(NET_OUT).exists():
    try:
        networks = json.loads(Path(NET_OUT).read_text())
    except Exception:
        networks = {}

net_key = meta.get("graphNetworkName", meta.get("networkName", net))
networks.setdefault(net_key, {})

# Имена датасорсов должны совпадать с subgraph.yaml
if "v3Factory" in ds:
    networks[net_key]["UniswapV3Factory"] = {
        "address": ds["v3Factory"]["address"],
        "startBlock": start_blocks.get("v3Factory", 0)
    }
if "v4PoolManager" in ds:
    networks[net_key]["PoolManager"] = {
        "address": ds["v4PoolManager"]["address"],
        "startBlock": start_blocks.get("v4PoolManager", 0)
    }

Path(NET_OUT).write_text(json.dumps(networks, indent=2))
print(json.dumps({"network": net_key, "startBlocks": start_blocks}, indent=2))
