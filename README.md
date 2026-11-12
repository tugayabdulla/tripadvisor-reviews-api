# TripAdvisor Data Pipeline + Reviews API

A production scraping pipeline that collects TripAdvisor reviews, hotels, restaurants and
attractions at scale, stores them in a partitioned PostgreSQL database, and serves them
through a FastAPI service fronted by RapidAPI.

## Scale (November 2026)

| | |
|---|---|
| Reviews collected | **218,409,593** |
| Rows in database | **539,554,019** |
| Database size | **504 GB** |
| Review table | **256 hash partitions** on `location_id` (PostgreSQL 16) |
| Locations | 4,683,358 |
| Hotels | 2,469,497 |
| Attractions | 2,170,178 |
| Reviewer profiles | 981,094 |

Running continuously since mid-2026. The dataset grows every day — it is not a one-off dump.

## Pipeline

Nine scrapers (~3,000 lines of Python) feed the database, orchestrated by a chain of shell
runners under cron:

| Scraper | Target |
|---|---|
| `review_scraper_vps.py` / `review_scraper_fast.py` | reviews, two engines (throughput vs. resilience) |
| `hotel_scraper.py` | hotel listings + attributes |
| `restaurant_scraper.py` | restaurant listings + attributes |
| `attraction_scraper_vps.py` | attractions |
| `attraction_product_scraper_vps.py` | bookable attraction products |
| `city_scraper_vps.py` | geography / location discovery |
| `giant_scraper.py` | high-volume composite runs |
| `enrich_api_worker.py` | detail enrichment worker |

**Schedule in production:**

- `refresh_all.sh` — daily review refresh
- `weekly_discovery_enrich.sh` — weekly discovery of new locations, then enrichment
- `run_review_chain.sh` / `master_autochain.sh` — chained review passes that resume where they stopped
- `discovery_sharded.sh` — sharded discovery so passes run in parallel without overlap
- `new_locs_pipeline.sh` — onboarding path for newly discovered locations
- `build_composite.sh` — composite table builds for query performance
- plus an hourly watchdog and a half-hourly health check that restart stalled runs

Everything is append-and-resume: a pass that dies partway through is picked up rather than
restarted, which is what makes a dataset this size maintainable by one person.

## Engineering notes

- **Partitioning.** Reviews are hash-partitioned 256 ways on `location_id`. Review lookups
  for a location hit exactly one partition instead of scanning a 218M-row table.
- **Composite tables** (`build_composite.sh`) and purpose-built indexes
  (`migrations/001_search_indexes.sql`) keep search responsive at this row count.
- **Two review engines.** A fast path for bulk collection and a more defensive path for
  sources that rate-limit or break; the chain falls back between them.
- **Self-healing.** The watchdog and health check detect stalled or crashed passes and
  restart them without manual intervention, so the daily refresh survives unattended.

## API

FastAPI service exposing the collected data, fronted by RapidAPI.

### Endpoints

- `GET /health` — liveness + DB ping
- `GET /reviews/{location_id}` — paginated, filterable reviews. Params: `source` (restaurant|hotel|attraction|product), `limit` (1-100), `offset`, `sort` (date|rating), `language`, `min_rating`, `max_rating`, `trip_type`
- `GET /location/search` — params: `query` (name substring), `country` (restaurants only), `type` (source), `limit` (1-50), `offset`
- `GET /location/{location_id}` — basic metadata. Param: `source`

All data routes require the `X-RapidAPI-Proxy-Secret` header (set `RAPIDAPI_PROXY_SECRET` in `.env`).

### API V1 limitations

- The API serves data already collected by the pipeline; it does not scrape on demand per request.
- `/location/search` supports name + (restaurant) country; `city` filtering is not in V1 (source schemas lack a uniform city column).
- Enrichment detail fields (email, hours, amenities) are not yet populated — deferred.

## Local development

1. `python3 -m venv .venv && .venv/bin/pip install -r requirements.txt`
2. Open an SSH tunnel to the DB: `ssh -fN -L 5433:127.0.0.1:5432 root@your-vps-host`
3. `cp .env.example .env`; set `DB_PORT=5433`, `DB_PASS=...`, `RAPIDAPI_PROXY_SECRET=devsecret`
4. Run tests: `PYTHONPATH=. .venv/bin/pytest tests/ -v`
5. Dev server: `PYTHONPATH=. .venv/bin/uvicorn app.main:app --reload`

## Deploy (VPS)

1. Copy `api/` to `/data/tripadvisor/api`
2. `python3 -m venv .venv && .venv/bin/pip install -r requirements.txt`
3. `cp .env.example .env`; fill `DB_PASS` + `RAPIDAPI_PROXY_SECRET` (keep `DB_HOST=127.0.0.1 DB_PORT=5432`)
4. Apply DB migration once: `psql ... -f migrations/001_search_indexes.sql` (already applied on the current VPS)
5. `cp deploy/reviews-api.service /etc/systemd/system/ && systemctl enable --now reviews-api`
6. `cp deploy/nginx-reviews-api.conf /etc/nginx/sites-enabled/ && nginx -t && systemctl reload nginx`
7. `curl localhost/health` → `{"status":"ok","db":"ok"}`
8. Add TLS (certbot) + connect the public URL in the RapidAPI dashboard.
