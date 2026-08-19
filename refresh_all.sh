#!/bin/bash
# Full incremental review refresh across all 4 sources, one at a time.
# Each source: 3 shards (by location_id, giants excluded via --max-reviews) + a giants pass.
# Scrapers are incremental (stop at latest_review_date), so unchanged locations hit the fast probe path.
cd /data/tripadvisor || exit 1
export DB_PASS="${DB_PASS:?set DB_PASS}"
log(){ echo "[$(date '+%F %T')] $*" >> /tmp/rev_autochain.log; }
disk(){ df -h / | tail -1 | tr -s ' ' | cut -d' ' -f5; }

log "=== REFRESH ALL begin (incremental, opt scrapers) | disk $(disk) ==="
                                          LOGT=rest  bash run_restaurants_fast.sh; log "restaurants done | disk $(disk)"
SRC=--hotels              LOGT=hotel               bash run_restaurants_fast.sh; log "hotels done"
SRC=--attractions         LOGT=attr                bash run_restaurants_fast.sh; log "attractions done"
SRC=--attraction-products LOGT=prod                bash run_restaurants_fast.sh; log "products done | disk $(disk)"
log "=== REFRESH ALL DONE ==="
