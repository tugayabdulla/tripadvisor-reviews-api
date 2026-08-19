#!/bin/bash
# Fast, no-bloat review scrape.
#  - N parallel shards by (location_id % N): one Python process is GIL-capped ~1 core,
#    so we run several to use the box's cores.
#  - empty locations get NO DB write (review_scraper_fast.py drops _mark_scraped) => no
#    table bloat => no PostgreSQL restarts needed.
#  - giants (>= MAXREV reviews) are excluded from the shards and scraped in one final pass,
#    so a handful of monster locations can't stall a shard.
set -u
export DB_PASS="${DB_PASS:?set DB_PASS}"
export CONCURRENCY="${CONCURRENCY:-120}"   # per shard; N*CONCURRENCY total conns to TripAdvisor
export DB_POOL_MAX="${DB_POOL_MAX:-30}"
cd /data/tripadvisor || exit 1
SHARDS="${SHARDS:-3}"
MAXREV="${MAXREV:-10000}"
SRC="${SRC:-}"   # ""=restaurants; or "--hotels" / "--attractions" / "--attraction-products"
LOGT="${LOGT:-rest}"

echo "[$(date '+%F %T')] === fast scrape begin: src='${SRC:-restaurants}' shards=$SHARDS conc=$CONCURRENCY maxrev=$MAXREV ===" >> /tmp/rev_fast.log

pids=()
for k in $(seq 0 $((SHARDS-1))); do
  python3 -u review_scraper_fast.py $SRC --shards "$SHARDS" --shard "$k" --max-reviews "$MAXREV" \
    > "/tmp/rev_${LOGT}_shard${k}.log" 2>&1 &
  pids+=($!)
done
echo "[$(date '+%F %T')] launched ${SHARDS} shards: ${pids[*]}" >> /tmp/rev_fast.log
for p in "${pids[@]}"; do wait "$p"; done
echo "[$(date '+%F %T')] shards done; scraping giants (>= $MAXREV reviews) single-pass" >> /tmp/rev_fast.log

# Giants are big-review full-walks: high concurrency stalls the event loop -> use 50.
CONCURRENCY=50 python3 -u review_scraper_fast.py $SRC --min-reviews "$MAXREV" > "/tmp/rev_${LOGT}_giants.log" 2>&1
echo "[$(date '+%F %T')] === fast scrape ALL DONE (${LOGT}) ===" >> /tmp/rev_fast.log
