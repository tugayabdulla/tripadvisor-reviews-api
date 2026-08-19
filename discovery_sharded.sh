#!/bin/bash
# Sharded discovery sweep: N parallel shards per source (city_id % N), one source at a time.
# Already-scraped cities (fresh city_scraped_at) are skipped, so this safely resumes restaurants.
export DB_PASS="${DB_PASS:?set DB_PASS}"
export CONCURRENCY="${CONCURRENCY:-50}"   # per shard
cd /data/tripadvisor || exit 1
SHARDS="${SHARDS:-3}"
log(){ echo "[$(date '+%F %T')] $*" >> /tmp/disc_autochain.log; }

run_source(){  # $1=label  $2=script  $3..=extra flags
  local label="$1" script="$2"; shift 2
  log "$label start (${SHARDS} shards)"
  local pids=()
  for k in $(seq 0 $((SHARDS-1))); do
    python3 -u "$script" "$@" --shards "$SHARDS" --shard "$k" > "/tmp/disc_${label}_s${k}.log" 2>&1 &
    pids+=($!)
  done
  for p in "${pids[@]}"; do wait "$p"; done
  log "$label done"
}

log "=== sharded discovery sweep begin (shards=$SHARDS conc=$CONCURRENCY) ==="
run_source restaurants restaurant_scraper.py
run_source hotels      hotel_scraper.py
run_source attractions attraction_scraper_vps.py
run_source products    attraction_product_scraper_vps.py

log "VACUUM ANALYZE source tables (reclaim re-discovery churn; no API impact)"
PGPASSWORD="$DB_PASS" psql -h 127.0.0.1 -U tripadvisor -d tripadvisor \
  -c "VACUUM (ANALYZE) locations;" -c "VACUUM (ANALYZE) hotels;" \
  -c "VACUUM (ANALYZE) attractions;" -c "VACUUM (ANALYZE) attraction_products;" \
  >> /tmp/disc_autochain.log 2>&1
log "=== DISCOVERY ALL DONE ==="
