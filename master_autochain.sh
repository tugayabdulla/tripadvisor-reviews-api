#!/bin/bash
# Auto-chain the full review-scrape sweep with the fast/no-bloat method.
# The 3 restaurant shards are already running. This:
#   1. waits for those shards to finish
#   2. scrapes restaurant giants (>=10k reviews) at conc 50 (big-location full-walks stall at high conc)
#   3. runs hotels -> attractions -> products, each via run_restaurants_fast.sh
#      (3 sharded workers + a conc-50 giants pass per source), one source at a time.
cd /data/tripadvisor || exit 1
export DB_PASS="${DB_PASS:?set DB_PASS}"
log(){ echo "[$(date '+%F %T')] $*" >> /tmp/rev_autochain.log; }

log "autochain start; waiting for restaurant shards"
# [r] bracket avoids pgrep matching its own command line; --shards only matches shard workers
while pgrep -f '[r]eview_scraper_fast.py --shards' >/dev/null 2>&1; do sleep 60; done
log "restaurant shards done -> restaurant giants (conc 50)"
CONCURRENCY=50 DB_POOL_MAX=30 python3 -u review_scraper_fast.py --min-reviews 10000 \
    > /tmp/rev_rest_giants.log 2>&1
log "restaurant giants done"

SRC=--hotels              LOGT=hotel bash run_restaurants_fast.sh; log "hotels done"
SRC=--attractions         LOGT=attr  bash run_restaurants_fast.sh; log "attractions done"
SRC=--attraction-products LOGT=prod  bash run_restaurants_fast.sh; log "products done"
log "=== AUTOCHAIN ALL SOURCES DONE ==="
