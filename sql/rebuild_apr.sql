mkdir -p /Users/axel/Dev/open-source/uniswap-lp-analytics/sql
pbpaste > /Users/axel/Dev/open-source/uniswap-lp-analytics/sql/rebuild_apr.sql
LC_ALL=C grep -nP '[^\x00-\x7F]' /Users/axel/Dev/open-source/uniswap-lp-analytics/sql/rebuild_apr.sql || echo "ASCII OK"
psql "$(grep -E '^DATABASE_URL=' /Users/axel/Dev/open-source/uniswap-lp-analytics/.env | cut -d= -f2 | sed 's/+asyncpg//')" -v ON_ERROR_STOP=1 -f /Users/axel/Dev/open-source/uniswap-lp-analytics/sql/rebuild_apr.sql
