#!/bin/bash
# Full discovery re-sweep across all due cities, one source at a time, then VACUUM.
# Uses the (now fixed) discovery scrapers: restaurant trailing-comma fixed, rollback-on-error
# added everywhere, attraction cards now UPSERT (no more list-only drops).
export DB_PASS="${DB_PASS:?set DB_PASS}"
export CONCURRENCY="${CONCURRENCY:-50}"
cd /data/tripadvisor || exit 1
log(){ echo "[$(date '+%F %T')] $*" >> /tmp/disc_autochain.log; }

log "=== discovery sweep begin (conc=$CONCURRENCY) ==="
log "restaurants start"
python3 -u restaurant_scraper.py            > /tmp/disc_restaurants.log 2>&1; log "restaurants done"
log "hotels start"
python3 -u hotel_scraper.py                 > /tmp/disc_hotels.log      2>&1; log "hotels done"
log "attractions start"
python3 -u attraction_scraper_vps.py        > /tmp/disc_attractions.log 2>&1; log "attractions done"
log "products start"
python3 -u attraction_product_scraper_vps.py > /tmp/disc_products.log   2>&1; log "products done"

log "VACUUM ANALYZE source tables (reclaim re-discovery churn; no API impact)"
PGPASSWORD="$DB_PASS" psql -h 127.0.0.1 -U tripadvisor -d tripadvisor \
  -c "VACUUM (ANALYZE) locations;" -c "VACUUM (ANALYZE) hotels;" \
  -c "VACUUM (ANALYZE) attractions;" -c "VACUUM (ANALYZE) attraction_products;" \
  >> /tmp/disc_autochain.log 2>&1
log "=== DISCOVERY ALL DONE ==="
