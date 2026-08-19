# TripAdvisor Reviews API

FastAPI service exposing pre-scraped TripAdvisor reviews + location search, fronted by RapidAPI.

## Endpoints
- `GET /health` — liveness + DB ping
- `GET /reviews/{location_id}` — paginated, filterable reviews. Params: `source` (restaurant|hotel|attraction|product), `limit` (1-100), `offset`, `sort` (date|rating), `language`, `min_rating`, `max_rating`, `trip_type`
- `GET /location/search` — params: `query` (name substring), `country` (restaurants only), `type` (source), `limit` (1-50), `offset`
- `GET /location/{location_id}` — basic metadata. Param: `source`

All data routes require the `X-RapidAPI-Proxy-Secret` header (set `RAPIDAPI_PROXY_SECRET` in `.env`).

## Notes / V1 limitations
- Pre-scraped data only (no on-demand scraping).
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
