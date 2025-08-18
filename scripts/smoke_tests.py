#!/usr/bin/env python3
import os, json, sys, subprocess, shlex
from pathlib import Path

REPO="/Users/axel/Dev/open-source/uniswap-lp-analytics"
ENV_PATH=f"{REPO}/.env"
CFG_PATH=f"{REPO}/backend/config/subgraphs.json"
NET_PATH=f"{REPO}/subgraphs/unified/networks.json"

def load_env(p):
    lines=Path(p).read_text().splitlines()
    kv={}
    for l in lines:
        if l and not l.strip().startswith("#") and "=" in l:
            k,v=l.split("=",1); kv[k]=v
    return kv

def load_json(p):
    return json.loads(Path(p).read_text()) if Path(p).exists() else {}

env=load_env(ENV_PATH)
cfg=load_json(CFG_PATH)
nets_json=load_json(NET_PATH)

def run_http_query(url, query):
    try:
        import httpx
        r=httpx.post(url, json={"query":query}, timeout=60.0)
        return True, r.status_code, r.json()
    except Exception as e:
        return False, None, {"error": str(e)}

def psql_run(sql):
    dburl = env.get("DATABASE_URL","")
    if not dburl:
        return False, "DATABASE_URL is empty"
    psql_url = dburl.replace("+asyncpg","")
    cmd = ["psql", psql_url, "-At", "-c", sql]
    try:
        out = subprocess.run(cmd, check=True, capture_output=True, text=True)
        return True, out.stdout.strip()
    except subprocess.CalledProcessError as e:
        return False, e.stderr.strip() or str(e)

def status(name, ok, detail):
    return {"test": name, "status": "PASS" if ok else "FAIL", "detail": detail}

def warn(name, detail):
    return {"test": name, "status": "WARN", "detail": detail}

results=[]

# T1: Конфиги согласованы
net_keys = list(cfg.keys())
if not net_keys:
    results.append(status("config_presence", False, "subgraphs.json has no networks"))
else:
    results.append(status("config_presence", True, f"subgraphs.json networks: {', '.join(net_keys)}"))

if Path(NET_PATH).exists():
    results.append(status("networks_json_presence", True, "networks.json present"))
else:
    results.append(status("networks_json_presence", False, "networks.json missing"))

# T2/T3/T4: Эндпоинты, схема, пробные запросы по всем сетям
for net in net_keys:
    meta = cfg.get(net, {})
    cli_net = meta.get("graphCliNetwork", net)
    env_key = f"GRAPH_ENDPOINT_{net.upper()}"
    url = env.get(env_key, "")
    if not url or not url.startswith("http"):
        results.append(status(f"{net}:endpoint", False, f"{env_key} missing or invalid"))
        continue
    ok, code, data = run_http_query(url, "{ _meta { hasIndexingErrors block { number } deployment } }")
    if not ok or "data" not in data:
        results.append(status(f"{net}:_meta", False, f"HTTP {code} {data.get('message') or data.get('error')}"))
        continue
    meta_data = data["data"]["_meta"]
    if meta_data.get("hasIndexingErrors"):
        results.append(warn(f"{net}:_meta", f"errors=true block={meta_data.get('block',{}).get('number')}"))
    else:
        results.append(status(f"{net}:_meta", True, f"block={meta_data.get('block',{}).get('number')}"))

    ok_h, _, h = run_http_query(url, '{ __type(name:"PoolHourData"){ name fields{ name } } }')
    ok_d, _, d = run_http_query(url, '{ __type(name:"PoolDayData"){ name fields{ name } } }')
    want_h = {"id","pool","periodStartUnix","feesUSD","tvlUSD"}
    want_d = {"id","pool","date","feesUSD","tvlUSD"}
    have_h = set([f["name"] for f in (h.get("data",{}).get("__type",{}) or {}).get("fields",[])])
    have_d = set([f["name"] for f in (d.get("data",{}).get("__type",{}) or {}).get("fields",[])])
    miss_h = sorted(list(want_h - have_h))
    miss_d = sorted(list(want_d - have_d))
    results.append(status(f"{net}:schema_PoolHourData", len(miss_h)==0, f"missing={miss_h}"))
    results.append(status(f"{net}:schema_PoolDayData", len(miss_d)==0, f"missing={miss_d}"))

    qh = "query { poolHourDatas(first:1, orderBy: periodStartUnix, orderDirection: desc){ id periodStartUnix feesUSD tvlUSD } }"
    qd = "query { poolDayDatas(first:1, orderBy: date, orderDirection: desc){ id date feesUSD tvlUSD } }"
    ok_qh, _, dh = run_http_query(url, qh)
    ok_qd, _, dd = run_http_query(url, qd)
    if ok_qh and "data" in dh:
        arr = dh["data"].get("poolHourDatas",[])
        results.append(status(f"{net}:query_poolHourDatas", True, f"rows={len(arr)}"))
    else:
        results.append(status(f"{net}:query_poolHourDatas", False, f"{dh}"))
    if ok_qd and "data" in dd:
        arr = dd["data"].get("poolDayDatas",[])
        results.append(status(f"{net}:query_poolDayDatas", True, f"rows={len(arr)}"))
    else:
        results.append(status(f"{net}:query_poolDayDatas", False, f"{dd}"))

# T5: DB connect
ok_db, db_msg = psql_run("select 1")
results.append(status("db_connect", ok_db, db_msg))

# T6: Наличие ключевых вьюх/матвьюх
views = [
  "v_pool_day_fees_usd_partial",
  "v_pool_hour_fees_usd_partial",
  "v_pool_latest_tvl",
  "v_pool_tvl_at_anchor",
  "v_pool_tvl_avg_7d","v_pool_tvl_avg_30d","v_pool_tvl_avg_90d","v_pool_tvl_avg_180d","v_pool_tvl_avg_365d",
  "v_pool_tvl_unified",
  "v_apr_candidates"
]
matviews = [
  "mv_top_fees_7d","mv_top_fees_30d","mv_top_fees_90d","mv_top_fees_180d","mv_top_fees_365d",
  "mv_fee_apr_7d","mv_fee_apr_30d","mv_fee_apr_90d","mv_fee_apr_180d","mv_fee_apr_365d"
]

missing=[]
for v in views:
    ok, out = psql_run(f"select 1 from pg_views where viewname='{v}'")
    if not ok or out.strip()!="1":
        missing.append(v)
for m in matviews:
    ok, out = psql_run(f"select 1 from pg_matviews where matviewname='{m}'")
    if not ok or out.strip()!="1":
        missing.append(m)
results.append(status("db_struct_presence", len(missing)==0, f"missing={missing}"))

# T7: Лёгкие метрики
ok1, c1 = psql_run("select count(*) from mv_top_fees_30d")
ok2, c2 = psql_run("select sum(case when fee_apr is null then 1 else 0 end), count(*) from mv_fee_apr_30d")
metrics = {"mv_top_fees_30d": c1 if ok1 else f"ERR:{c1}", "mv_fee_apr_30d_null_total": c2 if ok2 else f"ERR:{c2}"}
results.append(status("analytics_metrics", ok1 and ok2, json.dumps(metrics)))

any_fail = any(r["status"]=="FAIL" for r in results)

print(json.dumps({"summary": results}, indent=2))
sys.exit(1 if any_fail else 0)
