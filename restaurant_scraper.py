#!/usr/bin/env python3
"""Restaurant discovery scraper for VPS using aiohttp."""

import json
import orjson
import os
import sys
import time
import logging
import asyncio
import aiohttp
import psycopg2
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
QUERY_ID = '0aafc4cb11a3a073'
PAGE_SIZE = 30

HEADERS = {
    'accept': '*/*',
    'accept-encoding': 'gzip, deflate',
    'content-type': 'application/json',
    'origin': 'https://www.tripadvisor.com',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'same-origin',
    'sec-fetch-site': 'same-origin',
}

_db_pool = None

def get_pool():
    global _db_pool
    if _db_pool is None:
        _db_pool = pg_pool.ThreadedConnectionPool(
            minconn=2, maxconn=8,
            host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS
        )
    return _db_pool

def get_conn():
    return get_pool().getconn()

def put_conn(conn):
    get_pool().putconn(conn)


stats_429 = [0]


def parse_restaurant(restaurant, city_id, country_id):
    location_id = restaurant.get('locationId')
    if not location_id:
        return None, None

    parent_geo_id = None
    try:
        parent_geo_id = restaurant['taLocation']['hierarchy']['parentGeoId']
    except (KeyError, TypeError):
        pass

    item = {
        'location_id': location_id,
        'name': restaurant.get('name', ''),
        'city_id': city_id,
        'country_id': country_id,
        'detail_page_url': restaurant.get('detailPageRoute', {}).get('url', ''),
    }

    if 'location' in restaurant and restaurant['location'].get('geoPoint'):
        item['latitude'] = restaurant['location']['geoPoint'].get('latitude')
        item['longitude'] = restaurant['location']['geoPoint'].get('longitude')

    contact = (restaurant.get('taLocation') or {}).get('contact') or {}
    if contact:
        item['telephone'] = contact.get('telephone', '')
        street = contact.get('streetAddress') or {}
        if street:
            item['address'] = street.get('fullAddress', '')
            item['postal_code'] = street.get('postalCode', '')
            item['country'] = street.get('country', '')

    cuisines = (restaurant.get('cuisines') or {}).get('items', [])
    if cuisines:
        item['cuisines'] = [c.get('tag', {}).get('localizedName', '') for c in cuisines]

    etypes = (restaurant.get('establishmentTypes') or {}).get('items', [])
    if etypes:
        item['establishment_types'] = [e.get('tag', {}).get('localizedName', '') for e in etypes]

    price_types = (restaurant.get('priceTypes') or {}).get('items', [])
    if price_types:
        item['price_range'] = price_types[0].get('secondaryName', '')

    open_hours = restaurant.get('openHours') or {}
    if open_hours:
        item['open_status'] = open_hours.get('openStatusText', '')

    review_summary = restaurant.get('reviewSummary') or {}
    if review_summary:
        item['review_count'] = review_summary.get('count', 0)
        _r = review_summary.get('rating')
        item['rating'] = None if _r == -1 else _r  # TA returns -1 for "no rating" -> store NULL

    item['has_menu_provider'] = (restaurant.get('menu') or {}).get('hasProvider', False)

    return item, parent_geo_id


def _do_write(locations, city_id, country_id, discovered_cities, scrape_started_at):
    conn = get_conn()
    try:
        if locations:
            cur = conn.cursor()
            rows = []
            for loc in locations:
                rows.append((
                    loc['location_id'], loc.get('country_id'), loc.get('city_id'),
                    loc.get('name'), loc.get('address'), loc.get('postal_code'),
                    loc.get('country'), loc.get('latitude'), loc.get('longitude'),
                    loc.get('telephone'), loc.get('cuisines', []),
                    loc.get('establishment_types', []), loc.get('price_range'),
                    loc.get('rating'), loc.get('review_count'),
                    loc.get('detail_page_url'), loc.get('open_status'),
                    loc.get('has_menu_provider'),
                ))
            execute_values(cur, """
                INSERT INTO locations (location_id, country_id, city_id, name, address, postal_code,
                    country, latitude, longitude, telephone, cuisines, establishment_types,
                    price_range, rating, review_count, detail_page_url, open_status,
                    has_menu_provider)
                VALUES %s
                ON CONFLICT (location_id) DO UPDATE SET
                    name = EXCLUDED.name, address = EXCLUDED.address,
                    postal_code = EXCLUDED.postal_code, country = EXCLUDED.country,
                    latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude,
                    telephone = EXCLUDED.telephone, cuisines = EXCLUDED.cuisines,
                    establishment_types = EXCLUDED.establishment_types,
                    price_range = EXCLUDED.price_range, rating = EXCLUDED.rating,
                    review_count = EXCLUDED.review_count, open_status = EXCLUDED.open_status,
                    has_menu_provider = EXCLUDED.has_menu_provider
            """, rows)
            conn.commit()
            cur.close()

        for disc_city in discovered_cities:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO cities (city_id, country_id, name) VALUES (%s, %s, '') ON CONFLICT DO NOTHING",
                (disc_city, int(country_id))
            )
            conn.commit()
            cur.close()

        cur = conn.cursor()
        cur.execute(
            "UPDATE cities SET restaurants_scraped = %s, city_scraped_at = %s WHERE city_id = %s",
            (len(locations), scrape_started_at, city_id)
        )
        conn.commit()
        cur.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        put_conn(conn)


async def fetch_page(session, city_id, offset):
    params = {'geoId': int(city_id), 'broadened': False}
    if offset > 0:
        params['offset'] = str(offset)

    payload = [{
        'variables': {
            'limit': PAGE_SIZE, 'racRequest': None,
            'route': {'page': 'FindRestaurants', 'params': params},
            'additionalSelections': [], 'insertSponsoredListings': False,
            'avoidLSS': False, 'locale': 'en-US',
        },
        'extensions': {'preRegisteredQueryId': QUERY_ID},
    }]

    for attempt in range(3):
        try:
            async with session.post(GRAPHQL_URL, json=payload, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status == 429:
                    stats_429[0] += 1
                    await asyncio.sleep(2 ** attempt * 3)
                    continue
                if resp.status >= 500:
                    await asyncio.sleep(2 ** attempt * 3)
                    continue
                data = await resp.json(loads=orjson.loads)
                response_data = data[0]['data']['response']
                if 'geos' in response_data:
                    return {'broad_geo': response_data['geos']}
                return response_data.get('restaurants', [])
        except Exception:
            if attempt == 2:
                return None
            await asyncio.sleep(2 ** attempt * 3)
    return None


async def process_city(session, sem, city_id, country_id, db_executor, stats, city_queue):
    try:
        scrape_started_at = int(datetime.now(timezone.utc).timestamp())
        all_locations = []
        discovered_cities = set()
        offset = 0

        async with sem:
            while True:
                restaurants = await fetch_page(session, city_id, offset)

                if restaurants is None:
                    break

                if isinstance(restaurants, dict) and 'broad_geo' in restaurants:
                    for geo in restaurants['broad_geo']:
                        await city_queue.put((geo['id'], country_id))
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(db_executor, _do_write, [], city_id, country_id, set(), scrape_started_at)
                    stats['done'] += 1
                    return

                if not restaurants:
                    break

                for r in restaurants:
                    loc, parent_geo = parse_restaurant(r, city_id, country_id)
                    if loc:
                        all_locations.append(loc)
                        if parent_geo and parent_geo != city_id:
                            discovered_cities.add(parent_geo)

                if len(restaurants) < PAGE_SIZE:
                    break
                offset += PAGE_SIZE

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(db_executor, _do_write, all_locations, city_id, country_id, discovered_cities, scrape_started_at)

        stats['restaurants'] += len(all_locations)
        stats['done'] += 1
    except Exception as e:
        log.error(f"City {city_id}: {e}")
        stats['done'] += 1


async def run(cities):
    sem = asyncio.Semaphore(CONCURRENCY)
    stats = {'done': 0, 'restaurants': 0}
    total = len(cities)
    start = time.time()
    city_queue = asyncio.Queue()

    db_executor = ThreadPoolExecutor(max_workers=4)

    log.info(f"Starting: {total:,} cities, concurrency={CONCURRENCY}")

    async def progress_reporter():
        while stats['done'] < total:
            await asyncio.sleep(10)
            elapsed = time.time() - start
            rate = int(stats['done'] / elapsed) if elapsed > 0 else 0
            remaining = total - stats['done']
            eta = int(remaining / rate) if rate > 0 else 0
            log.info(f"[{stats['done']:,}/{total:,}] {stats['restaurants']:,} restaurants | {rate}/s | ETA {eta}s | 429s: {stats_429[0]}")

    connector = aiohttp.TCPConnector(limit=CONCURRENCY, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        reporter = asyncio.create_task(progress_reporter())
        pending = set()

        for city_id, country_id in cities:
            task = asyncio.create_task(
                process_city(session, sem, city_id, country_id, db_executor, stats, city_queue)
            )
            pending.add(task)
            task.add_done_callback(pending.discard)

            if len(pending) >= CONCURRENCY * 2:
                await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)

        # Process any BroadGeo child cities that were discovered
        while not city_queue.empty():
            city_id, country_id = await city_queue.get()
            task = asyncio.create_task(
                process_city(session, sem, city_id, country_id, db_executor, stats, city_queue)
            )
            pending.add(task)
            task.add_done_callback(pending.discard)
            total += 1

        if pending:
            await asyncio.gather(*pending)

        reporter.cancel()

    db_executor.shutdown(wait=True)

    elapsed = time.time() - start
    log.info(f"DONE: {stats['done']:,} cities, {stats['restaurants']:,} restaurants in {elapsed:.0f}s | 429s: {stats_429[0]}")


def main():
    argv = sys.argv[1:]
    limit = int(argv[argv.index('--limit') + 1]) if '--limit' in argv else None
    shards = int(argv[argv.index('--shards') + 1]) if '--shards' in argv else None
    shard = int(argv[argv.index('--shard') + 1]) if '--shard' in argv else None
    skip = set()
    for flag in ('--limit', '--shards', '--shard'):
        if flag in argv:
            i = argv.index(flag); skip.add(i); skip.add(i + 1)
    country_ids = [int(a) for i, a in enumerate(argv) if i not in skip and a.isdigit()]
    shard_sql = " AND (city_id %% %s) = %s" if shards is not None and shard is not None else ""
    shard_params = [shards, shard] if shards is not None and shard is not None else []

    conn = get_conn()
    cur = conn.cursor()

    stale_threshold = int(datetime.now(timezone.utc).timestamp()) - (7 * 86400)

    if country_ids:
        placeholders = ','.join(['%s'] * len(country_ids))
        cur.execute(f"""
            SELECT city_id, country_id FROM cities
            WHERE country_id IN ({placeholders})
            AND (restaurants_scraped IS NULL OR restaurants_scraped = 0
                 OR city_scraped_at IS NULL OR city_scraped_at < %s){shard_sql}
            ORDER BY country_id
            {f'LIMIT {limit}' if limit else ''}
        """, country_ids + [stale_threshold] + shard_params)
    else:
        cur.execute(f"""
            SELECT city_id, country_id FROM cities
            WHERE (restaurants_scraped IS NULL OR restaurants_scraped = 0
                  OR city_scraped_at IS NULL OR city_scraped_at < %s){shard_sql}
            ORDER BY country_id
            {f'LIMIT {limit}' if limit else ''}
        """, [stale_threshold] + shard_params)

    cities = cur.fetchall()
    cur.close()
    put_conn(conn)

    if not cities:
        log.info("No unprocessed cities found")
        return

    asyncio.run(run(cities))


if __name__ == '__main__':
    main()
