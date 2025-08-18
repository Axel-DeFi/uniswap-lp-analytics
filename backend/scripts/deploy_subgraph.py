#!/usr/bin/env python3
import os, json, subprocess, sys, shlex
from pathlib import Path

repo="/Users/axel/Dev/open-source/uniswap-lp-analytics"

env_lines=Path(f"{repo}/.env").read_text().splitlines()
env=dict(l.split("=",1) for l in env_lines if l and not l.strip().startswith("#") and "=" in l)

cfg=json.loads(Path(f"{repo}/backend/config/subgraphs.json").read_text())

args=[a for a in sys.argv[1:] if a]
dry=("--dry-run" in args) or (os.getenv("DRY_RUN") in ("1","true","True"))
nets=[a for a in args if not a.startswith("-")]
net=nets[0] if nets else "mainnet"

if net not in cfg:
    raise SystemExit(f"Unknown network: {net}")

meta=cfg[net]
cli_net=meta.get("graphCliNetwork", net)
slug=meta["slug"]
version=env.get("VERSION_LABEL","0.1.7")

default_map={"mainnet":"GRAPH_DEPLOY_KEY_MAINNET","optimism":"GRAPH_DEPLOY_KEY_OPTIMISM"}
deploy_env_var=default_map.get(net, f"GRAPH_DEPLOY_KEY_{net.upper()}")
deploy_key=env.get(deploy_env_var,"")
if not deploy_key:
    raise SystemExit(f"Missing deploy key in .env: {deploy_env_var}")

cmd=f"npx @graphprotocol/graph-cli deploy {shlex.quote(slug)} subgraph.yaml --node https://api.studio.thegraph.com/deploy/ --deploy-key {shlex.quote(deploy_key)} --network {shlex.quote(cli_net)} --network-file {repo}/subgraphs/unified/networks.json --version-label {shlex.quote(version)}"

if dry:
    print(cmd)
    raise SystemExit(0)

subprocess.run(cmd, cwd=f"{repo}/subgraphs/unified", shell=True, check=True)
