#!/bin/bash
# Incremental review scrape, all four sources, one-by-one. Each is incremental
# (stops at existing reviews). Detached via setsid+nohup so it survives disconnect.
export DB_PASS="${DB_PASS:?set DB_PASS}"
export CONCURRENCY=200
cd /data/tripadvisor || exit 1

run() {  # $1 = label, $2 = logfile, rest = flags
  local label="$1"; local logf="$2"; shift 2
  echo "[$(date '+%F %T')] START $label" >> /tmp/rev_chain.log
  python3 -u review_scraper_vps.py "$@" > "$logf" 2>&1
  echo "[$(date '+%F %T')] END $label (exit $?)" >> /tmp/rev_chain.log
}

echo "[$(date '+%F %T')] === review chain begin ===" >> /tmp/rev_chain.log
run restaurants  /tmp/rev_restaurants.log
run hotels       /tmp/rev_hotels.log       --hotels
run attractions  /tmp/rev_attractions.log  --attractions
run products     /tmp/rev_products.log     --attraction-products
echo "[$(date '+%F %T')] === review chain ALL DONE ===" >> /tmp/rev_chain.log
