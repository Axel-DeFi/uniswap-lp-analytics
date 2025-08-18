#!/usr/bin/env bash
set -euo pipefail
export REPO="/Users/axel/Dev/open-source/uniswap-lp-analytics"
source "$REPO/.venv/bin/activate"
cd "$REPO/subgraphs/unified"
npx @graphprotocol/graph-cli codegen
npx @graphprotocol/graph-cli build --network mainnet   --network-file "$REPO/subgraphs/unified/networks.json"
npx @graphprotocol/graph-cli build --network optimism  --network-file "$REPO/subgraphs/unified/networks.json"
"$REPO/backend/scripts/deploy_subgraph.py" mainnet
"$REPO/backend/scripts/deploy_subgraph.py" optimism
