#!/usr/bin/env bash
set -euo pipefail

REPO="/Users/axel/Dev/open-source/uniswap-lp-analytics"
source "$REPO/.venv/bin/activate"

VERSION_LABEL="$(grep -E '^VERSION_LABEL=' "$REPO/.env" | cut -d= -f2)"

cd "$REPO/subgraphs/unified"
npx @graphprotocol/graph-cli codegen
npx @graphprotocol/graph-cli build --network mainnet  --network-file "$REPO/subgraphs/unified/networks.json"
npx @graphprotocol/graph-cli build --network optimism --network-file "$REPO/subgraphs/unified/networks.json"

"$REPO/backend/scripts/deploy_subgraph.py" mainnet
"$REPO/backend/scripts/deploy_subgraph.py" optimism

"$REPO/.venv/bin/python" - <<PY
from pathlib import Path
import re
p=Path("$REPO/.env")
s=p.read_text()
ver="${VERSION_LABEL}"
def bump(line):
    m=re.match(r'^(GRAPH_ENDPOINT_(?:MAINNET|OPTIMISM)=)(.+)$', line)
    if not m: return line
    pre,url=m.groups()
    if "/" in url:
        base="/".join(url.split("/")[:-1])
        return pre+base+"/"+ver
    return pre+url
out=[]
for l in s.splitlines():
    if l.startswith("GRAPH_ENDPOINT_MAINNET=") or l.startswith("GRAPH_ENDPOINT_OPTIMISM="):
        out.append(bump(l))
    else:
        out.append(l)
p.write_text("\n".join(out)+"\n")
print("OK")
PY
