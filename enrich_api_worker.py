"""Fast enrichment via TripAdvisor internal location API (no proxy, no DataDome, no browser).
~122 req/s at conc 50, 0 throttling. Source-parameterized.

Env: DB_PASS (req), SOURCE=hotels|restaurants|attractions|products, CONC=80, CHUNK=4000,
     LIMIT=100000000, REQ_TIMEOUT=25 (hard per-request cancel), SKIP_AFTER=3 (park a row
     after N transient-fail rounds; it stays NULL and is retried on the next run)
"""
import asyncio, aiohttp, os, sys, time, json
import psycopg2
from psycopg2.extras import execute_batch

KEY = "ce957ab2-0385-40f2-a32d-ed80296ff67f"
HEADERS = {"X-TripAdvisor-API-Key": KEY, "Accept-Encoding": "gzip, deflate", "accept": "*/*"}
URL = "https://api.tripadvisor.com/api/internal/1.14/location/{}"

DB_PASS = os.environ["DB_PASS"]
SOURCE = os.environ.get("SOURCE", "hotels")
CONC = int(os.environ.get("CONC", "80"))
CHUNK = int(os.environ.get("CHUNK", "4000"))
LIMIT = int(os.environ.get("LIMIT", "100000000"))
REQ_TIMEOUT = int(os.environ.get("REQ_TIMEOUT", "25"))
SKIP_AFTER = int(os.environ.get("SKIP_AFTER", "3"))
TABLE = {"hotels": "hotels", "restaurants": "locations", "attractions": "attractions", "products": "attraction_products"}[SOURCE]

def names(lst): return [x.get("name") for x in (lst or []) if x.get("name")]
def I(x):
    try: return int(x)
    except (TypeError, ValueError): return None
def F(x):
    try: return float(x)
    except (TypeError, ValueError): return None
def RATING(x):
    # TripAdvisor returns -1 for "no rating"; store NULL, not -1
    v = F(x)
    return None if v == -1 else v

def parse_hotel(d):
    a = d.get("address_obj") or {}; rh = d.get("rating_histogram") or {}
    return {
        "phone": d.get("phone"), "official_url": d.get("website"), "description": d.get("description"),
        "address": d.get("address"), "street1": a.get("street1"), "street2": a.get("street2"),
        "postal_code": a.get("postalcode"),
        "star_rating": F(d.get("hotel_class")), "accommodation_type": d.get("subcategory_type_label"),
        "rating": RATING(d.get("rating")),
        "rating_1_star": I(rh.get("count_1")), "rating_2_star": I(rh.get("count_2")),
        "rating_3_star": I(rh.get("count_3")), "rating_4_star": I(rh.get("count_4")),
        "rating_5_star": I(rh.get("count_5")),
        "ranking_position": I(d.get("ranking_position")), "ranking_total": I(d.get("ranking_denominator")),
        "photo_count": I(d.get("photo_count")), "property_amenities": names(d.get("amenities")),
    }

def parse_rest(d):
    a = d.get("address_obj") or {}; rh = d.get("rating_histogram") or {}
    return {
        "email": d.get("email"), "telephone": d.get("phone"), "website": d.get("website"),
        "menu_url": d.get("menu_web_url"), "description": d.get("description"), "address": d.get("address"),
        "street2": a.get("street2"), "postal_code": a.get("postalcode"),
        "opening_hours": json.dumps(d["hours"]) if d.get("hours") else None,
        "cuisines": names(d.get("cuisine")), "establishment_types": names(d.get("establishment_types")),
        "meal_types": names(d.get("meal_types")), "diets": names(d.get("dietary_restrictions")),
        "top_reasons": names(d.get("dishes")), "price_range": d.get("price"), "rating": RATING(d.get("rating")),
        "rating_1_star": I(rh.get("count_1")), "rating_2_star": I(rh.get("count_2")),
        "rating_3_star": I(rh.get("count_3")), "rating_4_star": I(rh.get("count_4")),
        "rating_5_star": I(rh.get("count_5")),
        "ranking_position": I(d.get("ranking_position")), "ranking_total": I(d.get("ranking_denominator")),
    }

HOTEL_COLS = ["phone", "official_url", "description", "address", "street1", "street2", "postal_code",
    "star_rating", "accommodation_type", "rating", "rating_1_star", "rating_2_star", "rating_3_star",
    "rating_4_star", "rating_5_star", "ranking_position", "ranking_total", "photo_count", "property_amenities"]
REST_COLS = ["email", "telephone", "website", "menu_url", "description", "address", "street2", "postal_code",
    "opening_hours", "cuisines", "establishment_types", "meal_types", "diets", "top_reasons", "price_range",
    "rating", "rating_1_star", "rating_2_star", "rating_3_star", "rating_4_star", "rating_5_star",
    "ranking_position", "ranking_total"]

ATTR_COLS = ["address", "street1", "street2", "postal_code", "phone", "website", "email", "hours",
    "description", "rating", "rating_1_star", "rating_2_star", "rating_3_star", "rating_4_star",
    "rating_5_star", "ranking_position", "ranking_total", "photo_count"]
PROD_COLS = ["description", "rating", "rating_1_star", "rating_2_star", "rating_3_star", "rating_4_star",
    "rating_5_star", "ranking_position", "ranking_total", "photo_count"]

def parse_attraction(d):
    a = d.get("address_obj") or {}; rh = d.get("rating_histogram") or {}
    return {
        "address": d.get("address"), "street1": a.get("street1"), "street2": a.get("street2"),
        "postal_code": a.get("postalcode"), "phone": d.get("phone"), "website": d.get("website"),
        "email": d.get("email"),
        "hours": json.dumps(d["hours"]) if d.get("hours") else None,
        "description": d.get("description"), "rating": RATING(d.get("rating")),
        "rating_1_star": I(rh.get("count_1")), "rating_2_star": I(rh.get("count_2")),
        "rating_3_star": I(rh.get("count_3")), "rating_4_star": I(rh.get("count_4")),
        "rating_5_star": I(rh.get("count_5")),
        "ranking_position": I(d.get("ranking_position")), "ranking_total": I(d.get("ranking_denominator")),
        "photo_count": I(d.get("photo_count")),
    }

def parse_product(d):
    rh = d.get("rating_histogram") or {}
    return {
        "description": d.get("description"), "rating": RATING(d.get("rating")),
        "rating_1_star": I(rh.get("count_1")), "rating_2_star": I(rh.get("count_2")),
        "rating_3_star": I(rh.get("count_3")), "rating_4_star": I(rh.get("count_4")),
        "rating_5_star": I(rh.get("count_5")),
        "ranking_position": I(d.get("ranking_position")), "ranking_total": I(d.get("ranking_denominator")),
        "photo_count": I(d.get("photo_count")),
    }

PARSE, COLS = {"hotels": (parse_hotel, HOTEL_COLS), "restaurants": (parse_rest, REST_COLS),
               "attractions": (parse_attraction, ATTR_COLS), "products": (parse_product, PROD_COLS)}[SOURCE]
SET = ", ".join(f"{c}=%s" for c in COLS) + ", enriched_at=%s"
UPDATE_SQL = f"UPDATE {TABLE} SET {SET} WHERE location_id=%s"

def db():
    return psycopg2.connect(host="127.0.0.1", dbname="tripadvisor", user="tripadvisor", password=DB_PASS)

def row_values(lid, data):
    return [data.get(c) for c in COLS] + [int(time.time()), lid]

_RETRY = object()   # transient response -> back off and retry within this fetch

async def _request(session, lid):
    async with session.get(URL.format(lid), timeout=aiohttp.ClientTimeout(total=20)) as r:
        if r.status == 404:
            return {}                  # gone -> mark enriched, leave fields null
        if r.status != 200:
            return _RETRY              # 429 / 5xx / other transient
        d = await r.json()
        if not isinstance(d, dict) or "location_id" not in d:
            return {}
        return PARSE(d)

async def fetch(session, sem, lid):
    # asyncio.wait_for is a hard outer guard: even if aiohttp's own timeout fails to fire
    # on a wedged connection, the request is cancelled so one bad id can't freeze the chunk.
    async with sem:
        for attempt in range(3):
            try:
                res = await asyncio.wait_for(_request(session, lid), timeout=REQ_TIMEOUT)
            except Exception:
                if attempt == 2:
                    return lid, None      # transient -> caller counts the round, may skip
                await asyncio.sleep(1); continue
            if res is _RETRY:
                await asyncio.sleep(min(2 ** attempt, 4)); continue
            return lid, res
        return lid, None

def get_chunk(conn, skip):
    # exclude ids parked this run so a poison row can't be re-selected forever
    cur = conn.cursor()
    cur.execute(f"""SELECT location_id FROM {TABLE}
        WHERE enriched_at IS NULL AND NOT (location_id = ANY(%s::int[]))
        ORDER BY review_count DESC NULLS LAST LIMIT %s""", (list(skip), CHUNK))
    rows = [r[0] for r in cur.fetchall()]; cur.close()
    return rows

async def main():
    conn = db()
    conn.autocommit = True   # no long-lived read txn -> can't hold back autovacuum (bloat)
    sem = asyncio.Semaphore(CONC)
    done = 0; start = time.time(); last = start; last_done = 0
    attempts = {}            # lid -> transient-fail rounds this run
    skip = set()             # lids parked after SKIP_AFTER rounds (stay NULL, retried next run)
    connector = aiohttp.TCPConnector(limit=CONC + 10, enable_cleanup_closed=True, ttl_dns_cache=300)
    async with aiohttp.ClientSession(headers=HEADERS, connector=connector) as session:
        while done < LIMIT:
            ids = get_chunk(conn, skip)
            if not ids:
                break        # nothing left but parked rows -> clean exit
            results = await asyncio.gather(*[fetch(session, sem, lid) for lid in ids],
                                           return_exceptions=True)
            rows = []
            for res in results:
                if not isinstance(res, tuple):
                    continue                      # unexpected exception from a fetch
                lid, data = res
                if data is not None:
                    rows.append(row_values(lid, data))
                else:
                    attempts[lid] = attempts.get(lid, 0) + 1
                    if attempts[lid] >= SKIP_AFTER:
                        skip.add(lid)             # park it; next run retries (enriched_at still NULL)
            if rows:
                cur = conn.cursor()
                execute_batch(cur, UPDATE_SQL, rows, page_size=500)
                cur.close()
            done += len(rows)
            now = time.time()
            rate = (done - last_done) / max(now - last, 0.001)
            print(f"[{done}] {rate:.0f}/s  (chunk {len(ids)}, wrote {len(rows)}, skipped {len(skip)})", flush=True)
            last = now; last_done = done
    elapsed = time.time() - start
    print(f"DONE {SOURCE}: enriched {done} in {elapsed:.0f}s = {done/max(elapsed,1):.0f}/s, "
          f"skipped {len(skip)} (retry next run)", flush=True)
    conn.close()

asyncio.run(main())
