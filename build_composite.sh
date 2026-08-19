#!/bin/bash
# Build composite (location_id, published_date) index on a partitioned review table, NO downtime.
# Pattern: invalid parent index ON ONLY -> CREATE INDEX CONCURRENTLY per partition -> ATTACH.
# Idempotent (IF NOT EXISTS + attach-if-needed). Usage: build_composite.sh <parent_table>
set -u
T="$1"
IDX="idx_${T}_loc_pubdate"
PSQL="env PGPASSWORD=$DB_PASS psql -h 127.0.0.1 -U tripadvisor -d tripadvisor -t -A"
ts(){ date '+%F %T'; }
LOG=/data/tripadvisor/cron/logs/composite_${T}.log
exec >>"$LOG" 2>&1
echo "[$(ts)] === composite build on $T BEGIN ==="

USE=$(df --output=pcent / | tail -1 | tr -dc '0-9')
echo "[$(ts)] disk ${USE}%"
if [ "${USE:-100}" -ge 89 ]; then echo "[$(ts)] ABORT: disk ${USE}% too high to build safely"; exit 1; fi

# 1) invalid parent index (instant)
$PSQL -c "CREATE INDEX IF NOT EXISTS $IDX ON ONLY $T (location_id, published_date)"
# 2) per-partition CONCURRENTLY + ATTACH
parts=$($PSQL -c "SELECT inhrelid::regclass::text FROM pg_inherits WHERE inhparent='$T'::regclass ORDER BY 1")
total=$(echo "$parts" | grep -c .)
n=0
for p in $parts; do
  pidx="${p}_loc_pubdate"
  $PSQL -c "CREATE INDEX CONCURRENTLY IF NOT EXISTS $pidx ON $p (location_id, published_date)" >/dev/null 2>&1
  $PSQL -c "ALTER INDEX $IDX ATTACH PARTITION $pidx" >/dev/null 2>&1
  n=$((n+1))
  [ $((n % 32)) -eq 0 ] && echo "[$(ts)] $T: $n/$total partitions"
done
valid=$($PSQL -c "SELECT indisvalid FROM pg_index WHERE indexrelid='$IDX'::regclass")
sz=$($PSQL -c "SELECT pg_size_pretty(pg_total_relation_size('$IDX'::regclass))")
echo "[$(ts)] === $T DONE: $n/$total partitions, parent indisvalid=$valid, size=$sz ==="
