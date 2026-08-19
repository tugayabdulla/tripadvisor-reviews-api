#!/bin/bash
# Run enrichment + reviews on the freshly-discovered locations only.
#   Phase 1 ENRICH: enrich_api_worker.py selects WHERE enriched_at IS NULL = new locs (+ old dead tail).
#   Phase 2 REVIEWS: review_scraper_fast.py --older-than 99999 => only latest_review_date IS NULL = new locs
#                    (NOT a re-sweep of all 9M). Sharded 3-wide + a conc-50 giants pass per source.
export DB_PASS="${DB_PASS:?set DB_PASS}"
cd /data/tripadvisor || exit 1
SHARDS=3; MAXREV=10000
log(){ echo "[$(date '+%F %T')] $*" >> /tmp/newlocs.log; }

log "=== PHASE 1: ENRICHMENT on new locs (enriched_at IS NULL) ==="
for src in hotels restaurants attractions products; do
  log "enrich $src start"
  SOURCE=$src CONC=60 CHUNK=4000 python3 -u enrich_api_worker.py > "/tmp/newenrich_${src}.log" 2>&1
  log "enrich $src done"
done

log "=== PHASE 2: REVIEWS on new locs (--older-than 99999 = NULL only), sharded ==="
run_reviews(){  # $1=label  $2=source-flag ("" for restaurants)
  local label="$1" srcflag="$2"
  log "reviews $label start (${SHARDS} shards)"
  local pids=()
  for k in $(seq 0 $((SHARDS-1))); do
    CONCURRENCY=50 python3 -u review_scraper_fast.py $srcflag --older-than 99999 \
      --shards $SHARDS --shard $k --max-reviews $MAXREV > "/tmp/newrev_${label}_s${k}.log" 2>&1 &
    pids+=($!)
  done
  for p in "${pids[@]}"; do wait "$p"; done
  # new giants get a full scrape at low concurrency (big-location full-walks stall at high conc)
  CONCURRENCY=50 python3 -u review_scraper_fast.py $srcflag --older-than 99999 \
    --min-reviews $MAXREV > "/tmp/newrev_${label}_giants.log" 2>&1
  log "reviews $label done"
}
run_reviews restaurants ""
run_reviews hotels      "--hotels"
run_reviews attractions "--attractions"
run_reviews products    "--attraction-products"
log "=== NEW-LOCS PIPELINE ALL DONE ==="
