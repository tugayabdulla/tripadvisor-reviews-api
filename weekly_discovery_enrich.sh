#!/bin/bash
# Weekly: discover new venues, then enrich + review ONLY the new ones.
# (Daily refresh_all.sh handles re-scraping existing locations for new reviews.)
set -u
cd /data/tripadvisor || exit 1
log(){ echo "[$(date '+%F %T')] $*"; }
log "=== WEEKLY discovery+enrich BEGIN ==="
log "--- phase 1: discovery sweep (find new venues) ---"
bash /data/tripadvisor/discovery_sharded.sh
log "--- phase 2: enrich + reviews on new locations only ---"
bash /data/tripadvisor/new_locs_pipeline.sh
log "=== WEEKLY discovery+enrich END ==="
