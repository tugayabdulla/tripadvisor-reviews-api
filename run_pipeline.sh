#!/bin/bash
# Hardened cron wrapper for TA scraper pipelines (production: behind the published API).
# Usage: run_pipeline.sh <name> <command...>
#  - flock -n: only ONE pipeline runs at a time; SKIP (don't pile up) if busy
#  - disk guard: ABORT before starting if disk is high (a runaway scrape filling the
#    disk would crash the customer-facing PG/API -> stale-but-up beats fresh-but-down)
#  - persistent dated logs + last_success heartbeat (so stale data is DETECTABLE)
set -u
NAME="${1:?usage: run_pipeline.sh <name> <command...>}"; shift
BASE=/data/tripadvisor/cron
LOGDIR="$BASE/logs"
LOCK="$BASE/scraper.lock"
DISK_ABORT=90
mkdir -p "$LOGDIR"
ts(){ date '+%F %T'; }
LOG="$LOGDIR/${NAME}-$(date '+%Y%m%d-%H%M%S').log"
exec >>"$LOG" 2>&1

echo "[$(ts)] === $NAME START ==="
# 1) serialize across all pipelines
exec 9>"$LOCK"
if ! flock -n 9; then
  echo "[$(ts)] SKIP: another scraper pipeline holds the lock — exiting cleanly"
  exit 0
fi
# 2) disk guard (protect the live API DB)
USE=$(df --output=pcent / | tail -1 | tr -dc '0-9')
echo "[$(ts)] disk used=${USE}% (abort >= ${DISK_ABORT}%)"
if [ "${USE:-100}" -ge "$DISK_ABORT" ]; then
  echo "[$(ts)] ABORT: disk ${USE}% >= ${DISK_ABORT}% — refusing to run to protect the API DB. Free space / expand disk."
  exit 1
fi
# 3) run
echo "[$(ts)] running: $*"
"$@"; rc=$?
echo "[$(ts)] === $NAME END rc=$rc ==="
# 4) heartbeat on success + prune old logs
[ "$rc" -eq 0 ] && echo "$(ts)" > "$BASE/last_success_${NAME}"
find "$LOGDIR" -name "${NAME}-*.log" -mtime +14 -delete 2>/dev/null
exit $rc
