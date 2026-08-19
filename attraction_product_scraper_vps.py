#!/usr/bin/env python3
"""AttractionProduct (tours/experiences) discovery scraper. Single-pass list cards (no map pins)."""
import os, sys, re, time, json, logging, asyncio, uuid, aiohttp, psycopg2
import orjson
from psycopg2 import pool as pg_pool
from psycopg2.extras import execute_values
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%H:%M:%S')
log = logging.getLogger(__name__)

DB_HOST = os.environ.get('DB_HOST', '127.0.0.1')
DB_NAME = os.environ.get('DB_NAME', 'tripadvisor')
DB_USER = os.environ.get('DB_USER', 'tripadvisor')
DB_PASS = os.environ.get('DB_PASS', '')
CONCURRENCY = int(os.environ.get('CONCURRENCY', '50'))

GRAPHQL_URL = 'https://www.tripadvisor.com/data/graphql/ids'
QUERY_ID = '0a9ecd5c0d013d56'
PAGE_SIZE = 30

HEADERS = {'accept':'*/*','accept-encoding':'gzip, deflate','content-type':'application/json',
           'origin':'https://www.tripadvisor.com','sec-fetch-dest':'empty',
           'sec-fetch-mode':'same-origin','sec-fetch-site':'same-origin'}

_db_pool = None
def get_pool():
    global _db_pool
    if _db_pool is None:
        _db_pool = pg_pool.ThreadedConnectionPool(minconn=2, maxconn=8,
            host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
    return _db_pool
def get_conn(): return get_pool().getconn()
def put_conn(c): get_pool().putconn(c)

stats_429 = [0]

def make_payload(geo_id, page_num):
    params = {'geoId': int(geo_id), 'filters': [], 'contentType': 'attraction_product',
              'webVariant': 'AttractionProductsFusion'}
    if page_num > 0:
        params['pagee'] = str(page_num * PAGE_SIZE)
    return [{'variables': {
        'request': {'tracking': {'screenName':'AttractionProductsFusion','pageviewUid':str(uuid.uuid4())},
                    'routeParameters': params, 'updateToken': None},
        'commerce': {'attractionCommerce': {'pax': [{'ageBand':'ADULT','count':2}]}},
        'tracking': {'screenName':'AttractionProductsFusion','pageviewUid':str(uuid.uuid4())},
        'sessionId': uuid.uuid4().hex.upper(), 'unitLength':'MILES', 'currency':'USD',
        'currentGeoPoint': None, 'mapSurface': False, 'debug': False, 'polling': False},
        'extensions': {'preRegisteredQueryId': QUERY_ID}}]

async def fetch_page(session, geo_id, page_num):
    payload = make_payload(geo_id, page_num)
    for attempt in range(3):
        try:
            async with session.post(GRAPHQL_URL, json=payload, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status == 429:
                    stats_429[0] += 1; await asyncio.sleep(2**attempt*3); continue
                if resp.status >= 500:
                    await asyncio.sleep(2**attempt*3); continue
                data = await resp.json(loads=orjson.loads)
                return data[0]['data']['Result'][0]
        except Exception:
            if attempt == 2: return None
            await asyncio.sleep(2**attempt*3)
    return None

def text_of(v):
    if isinstance(v, dict): return v.get('text', '')
    return v or ''

def parse_card(section, city_id, country_id):
    card = section.get('singleFlexCardContent')
    if not card: return None
    url = (card.get('cardLink') or {}).get('webRoute', {}).get('webLinkUrl', '')
    if 'AttractionProductReview' not in url: return None
    m = re.search(r'-d(\d+)', url)
    if not m: return None
    bubble = card.get('bubbleRating') or {}
    fc = card.get('flexCardCommerce') or {}
    tk = {}
    try: tk = json.loads(card.get('trackingKey', '{}'))
    except Exception: pass
    pcode = re.search(r'attraction_product_(\w+)', card.get('stableDiffingType', '') or '')
    photo = ((card.get('cardPhoto') or {}).get('sizes') or {}).get('urlTemplate')
    langs = card.get('languageDetails') or []
    lang_txt = None
    if langs and isinstance(langs[0], dict):
        lt = langs[0].get('text')
        lang_txt = lt.get('text') if isinstance(lt, dict) else lt
    return {
        'location_id': int(m.group(1)), 'city_id': city_id, 'country_id': country_id,
        'name': text_of(card.get('cardTitle')), 'url': url,
        'rating': bubble.get('rating'), 'review_count': bubble.get('reviewCount'),
        'price': text_of(fc.get('price')) or None,
        'price_numeric': tk.get('prc'),
        'currency': tk.get('cur'),
        'strikethrough_price': text_of(fc.get('strikeThroughPrice')) or None,
        'price_suffix': text_of(fc.get('priceSuffixText')) or None,
        'duration': text_of(card.get('duration')) or None,
        'operator': text_of(card.get('operatorName')) or None,
        'product_code': pcode.group(1) if pcode else None,
        'thumbnail': photo,
        'languages': lang_txt,
        'categories': text_of(card.get('primaryInfo')) or None,
        'description': text_of(card.get('descriptiveText')) or None,
    }

def _do_write(products, city_id, scrape_started_at):
    conn = get_conn()
    try:
        if products:
            cur = conn.cursor()
            def flat(v):
                return v.get('text', str(v)) if isinstance(v, dict) else v
            seen = set(); rows = []
            for p in products:
                if p['location_id'] in seen: continue
                seen.add(p['location_id'])
                rows.append((p['location_id'], p.get('country_id'), p.get('city_id'),
                    flat(p.get('name')), flat(p.get('url')), p.get('rating'), p.get('review_count'),
                    flat(p.get('price')), p.get('price_numeric'), flat(p.get('currency')),
                    flat(p.get('strikethrough_price')), flat(p.get('price_suffix')),
                    flat(p.get('duration')), flat(p.get('operator')), flat(p.get('product_code')),
                    flat(p.get('thumbnail')), flat(p.get('languages')),
                    flat(p.get('categories')), flat(p.get('description'))))
            execute_values(cur, """
                INSERT INTO attraction_products (location_id, country_id, city_id, name, url,
                    rating, review_count, price, price_numeric, currency, strikethrough_price,
                    price_suffix, duration, operator, product_code, thumbnail, languages,
                    categories, description)
                VALUES %s
                ON CONFLICT (location_id) DO UPDATE SET
                    name=EXCLUDED.name, url=EXCLUDED.url, rating=EXCLUDED.rating,
                    review_count=EXCLUDED.review_count, price=EXCLUDED.price,
                    price_numeric=EXCLUDED.price_numeric, currency=EXCLUDED.currency,
                    strikethrough_price=EXCLUDED.strikethrough_price, price_suffix=EXCLUDED.price_suffix,
                    duration=EXCLUDED.duration, operator=EXCLUDED.operator,
                    product_code=EXCLUDED.product_code, thumbnail=EXCLUDED.thumbnail,
                    languages=EXCLUDED.languages, categories=EXCLUDED.categories,
                    description=EXCLUDED.description
            """, rows)
            conn.commit(); cur.close()
            n = len(rows)
        else:
            n = 0
        cur = conn.cursor()
        cur.execute("UPDATE cities SET products_scraped=%s, product_scraped_at=%s WHERE city_id=%s",
                    (n, scrape_started_at, city_id))
        conn.commit(); cur.close()
    except Exception:
        conn.rollback(); raise
    finally:
        put_conn(conn)

async def process_city(session, sem, city_id, country_id, db_executor, stats):
    try:
        started = int(datetime.now(timezone.utc).timestamp())
        all_products = []
        async with sem:
            page = 0
            total_results = None
            empty_streak = 0
            while True:
                res = await fetch_page(session, city_id, page)
                if res is None: break
                if total_results is None:
                    total_results = res.get('totalResults') or 0
                cards = []
                for sec in res.get('sections', []):
                    if sec.get('__typename') == 'WebPresentation_SingleFlexCardSection':
                        c = parse_card(sec, city_id, country_id)
                        if c: cards.append(c)
                if cards:
                    all_products.extend(cards)
                    empty_streak = 0
                else:
                    empty_streak += 1
                    if empty_streak >= 2: break  # two consecutive empty pages -> done
                page += 1
                # stop once we've paged past the reported total (with a small buffer)
                if total_results and page * PAGE_SIZE > total_results + PAGE_SIZE:
                    break
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(db_executor, _do_write, all_products, city_id, started)
        stats['products'] += len(all_products); stats['done'] += 1
    except Exception as e:
        log.error(f"City {city_id}: {e}"); stats['done'] += 1

async def run(cities):
    sem = asyncio.Semaphore(CONCURRENCY)
    stats = {'done':0, 'products':0}; total = len(cities); start = time.time()
    db_executor = ThreadPoolExecutor(max_workers=4)
    log.info(f"Starting: {total:,} cities, concurrency={CONCURRENCY}")

    async def reporter():
        pd, pt = 0, start
        while stats['done'] < total:
            await asyncio.sleep(10)
            now = time.time(); dt = now-pt; dd = stats['done']-pd
            rate = dd/dt if dt>0 else 0; rem = total-stats['done']
            eta = int(rem/rate) if rate>0 else 0; h,mm = divmod(eta//60,60)
            log.info(f"[{stats['done']:,}/{total:,}] {stats['products']:,} products | {rate:.0f}/s | ETA {h}h{mm:02d}m | 429s: {stats_429[0]}")
            pd, pt = stats['done'], now

    connector = aiohttp.TCPConnector(limit=CONCURRENCY, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        rep = asyncio.create_task(reporter()); pending = set()
        for cid, coid in cities:
            t = asyncio.create_task(process_city(session, sem, cid, coid, db_executor, stats))
            pending.add(t); t.add_done_callback(pending.discard)
            if len(pending) >= CONCURRENCY*2:
                await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
        if pending: await asyncio.gather(*pending)
        rep.cancel()
    db_executor.shutdown(wait=True)
    log.info(f"DONE: {stats['done']:,} cities, {stats['products']:,} products in {time.time()-start:.0f}s | 429s: {stats_429[0]}")

def main():
    argv = sys.argv[1:]
    limit = int(argv[argv.index('--limit')+1]) if '--limit' in argv else None
    shards = int(argv[argv.index('--shards')+1]) if '--shards' in argv else None
    shard = int(argv[argv.index('--shard')+1]) if '--shard' in argv else None
    skip = set()
    for flag in ('--limit', '--shards', '--shard'):
        if flag in argv:
            i = argv.index(flag); skip.add(i); skip.add(i+1)
    country_ids = [int(a) for i,a in enumerate(argv) if i not in skip and a.isdigit()]
    shard_sql = " AND (city_id %% %s) = %s" if shards is not None and shard is not None else ""
    shard_params = [shards, shard] if shards is not None and shard is not None else []
    stale = int(datetime.now(timezone.utc).timestamp()) - (7*86400)
    conn = get_conn(); cur = conn.cursor()
    if country_ids:
        ph = ','.join(['%s']*len(country_ids))
        cur.execute(f"""SELECT city_id, country_id FROM cities WHERE country_id IN ({ph})
            AND (products_scraped IS NULL OR products_scraped=0 OR product_scraped_at IS NULL OR product_scraped_at < %s){shard_sql}
            ORDER BY country_id {f'LIMIT {limit}' if limit else ''}""", country_ids+[stale]+shard_params)
    else:
        cur.execute(f"""SELECT city_id, country_id FROM cities
            WHERE (products_scraped IS NULL OR products_scraped=0 OR product_scraped_at IS NULL OR product_scraped_at < %s){shard_sql}
            ORDER BY country_id {f'LIMIT {limit}' if limit else ''}""", [stale]+shard_params)
    cities = cur.fetchall(); cur.close(); put_conn(conn)
    if not cities:
        log.info("No unprocessed cities"); return
    asyncio.run(run(cities))

if __name__ == '__main__':
    main()
