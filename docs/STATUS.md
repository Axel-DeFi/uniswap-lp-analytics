# Uniswap LP Analytics — STATUS

## Data source
- The Graph Studio subgraph: unified (v3 + v4). Endpoint kept in `.env` as GRAPH_V3_ENDPOINT.

## DB schema
- Tables: tokens, pools, pool_day_data, pool_hour_data, pool_price_hour
- Views/MatViews: v_pools_by_pair, v_pool_hour_fees_usd, mv_pool_day_fees_usd

## Config
- backend/config/tokens.yaml: chain "1" — WETH, USDC, USDT, WBTC
- backend/config/pair_rules.yaml: ETH/WBTC vs USDC/USDT, versions [3,4]

## Ingestion scripts
- list_pools_unified.py, list_pools_pairs.py
- load_pools_to_db.py
- backfill_pool_agg.py
- backfill_price_hour.py
- sync_pools_by_rules.py, backfill_price_hour_by_rules.py

## API (optional)
- uvicorn backend.api.app:app on 127.0.0.1:8000
- endpoints: /health, /pools, /pools/top_fees, /export/top_fees.csv, /export/top_volume.csv, /sync/status

## Known status
- v4 not fully visible until subgraph sync crosses v4 start block.
- v3 data loaded and aggregated; fees/volume queries OK.

## Next
1) Wait until v4 is visible via subgraph and run the same ingestion.
2) Add incremental hourly sync for PoolPriceHour by rules.
3) Improve USD metrics and TVL for APY analytics.
4) Add tests for pair normalization and SQL aggregations.
