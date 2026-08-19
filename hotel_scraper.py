#!/usr/bin/env python3
"""Hotel discovery scraper for VPS using aiohttp."""

import json
import orjson
import os
import sys
import time
import logging
import asyncio
import uuid
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
QUERY_ID = '63e3822a61019f55'
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


def parse_hotel(result, city_id, country_id):
    location_id = result.get('locationId')
    if not location_id:
        return None

    loc = result.get('location') or {}
    review_summary = loc.get('reviewSummary') or {}
    loc_v2 = loc.get('locationV2') or {}
    names = loc_v2.get('names') or {}
    geocode = loc_v2.get('geocode') or {}
    contact = loc_v2.get('contact') or {}
    street = contact.get('streetAddress') or {}
    hierarchy = loc_v2.get('hierarchy') or {}
    accom_type = loc_v2.get('accommodationType') or {}
    price_range = loc_v2.get('hotelPriceRangesUSD') or loc_v2.get('hotelPriceRanges') or {}

    return {
        'location_id': location_id,
        'city_id': city_id,
        'country_id': country_id,
        'name': names.get('name', ''),
        'url': loc.get('url', ''),
        'latitude': geocode.get('latitude'),
        'longitude': geocode.get('longitude'),
        'address': street.get('fullAddress', ''),
        'telephone': contact.get('telephone', ''),
        'postal_code': street.get('postalCode', ''),
        'country': street.get('country', ''),
        'city': street.get('city', ''),
        'rating': review_summary.get('rating'),
        'review_count': review_summary.get('count', 0),
        'accommodation_type': accom_type.get('name', ''),
        'price_min': price_range.get('minimum'),
        'price_max': price_range.get('maximum'),
        'parent_geo_id': hierarchy.get('parentGeoId'),
    }


def _do_write(hotels, city_id, scrape_started_at):
    conn = get_conn()
    try:
        if hotels:
            cur = conn.cursor()
            rows = [(
                h['location_id'], h.get('country_id'), h.get('city_id'),
                h.get('name'), h.get('url'), h.get('address'), h.get('telephone'),
                h.get('latitude'), h.get('longitude'), h.get('postal_code'),
                h.get('rating'), h.get('review_count'),
                h.get('accommodation_type'), h.get('price_min'), h.get('price_max'),
                h.get('city'), h.get('parent_geo_id'),
            ) for h in hotels]
            execute_values(cur, """
                INSERT INTO hotels (location_id, country_id, city_id,
                    name, url, address, telephone,
                    latitude, longitude, postal_code,
                    rating, review_count,
                    accommodation_type, price_range_min, price_range_max,
                    city, parent_geo_id)
                VALUES %s
                ON CONFLICT (location_id) DO UPDATE SET
                    name = COALESCE(NULLIF(EXCLUDED.name, ''), hotels.name), url = EXCLUDED.url,
                    address = EXCLUDED.address, telephone = EXCLUDED.telephone,
                    latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude,
                    postal_code = EXCLUDED.postal_code,
                    rating = EXCLUDED.rating, review_count = EXCLUDED.review_count,
                    accommodation_type = EXCLUDED.accommodation_type,
                    price_range_min = EXCLUDED.price_range_min,
                    price_range_max = EXCLUDED.price_range_max,
                    city = COALESCE(EXCLUDED.city, hotels.city),
                    parent_geo_id = COALESCE(EXCLUDED.parent_geo_id, hotels.parent_geo_id)
            """, rows)
            conn.commit()
            cur.close()

        cur = conn.cursor()
        cur.execute(
            "UPDATE cities SET hotels_scraped = %s, hotel_scraped_at = %s WHERE city_id = %s",
            (len(hotels), scrape_started_at, city_id)
        )
        conn.commit()
        cur.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        put_conn(conn)


async def fetch_page(session, geo_id, offset):
    payload = [{
        'variables': {
            'geoId': int(geo_id),
            'blenderId': None, 'boundingBox': None, 'centerAndRadius': None,
            'travelInfo': None, 'currency': 'USD', 'pricingMode': None,
            'filters': {'selectTravelersChoiceWinner': False, 'selectTravelersChoiceBOTBWinner': False},
            'offset': offset, 'limit': PAGE_SIZE, 'sort': 'POPULARITY',
            'clientType': 'DESKTOP', 'loadMapSpecificData': False, 'viewType': 'LIST',
            'productId': 'Hotels',
            'pageviewId': str(uuid.uuid4()),
            'sessionId': uuid.uuid4().hex.upper(),
            'route': {'page': 'HotelsFusion', 'params': {'geoId': int(geo_id), 'contentType': 'hotel', 'webVariant': 'HotelsFusion'}},
            'userEngagedFilters': False, 'loadPoiThumbnail': False,
            'loadLocationSEOData': False, 'loadLocationInfoData': False,
            'loadNearbyPointOfInterestPlaceType': False,
            'metaMarketingQueryString': '', 'loadReviewSubratingAvgs': False,
            'isSplitMapView': False, 'showContextualisedThumbnail': False,
            'polling': False, 'tertiaryOffers': False, 'includePhotoSizes': False,
            'requestNumber': offset // PAGE_SIZE,
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
                return data[0].get('data', {}).get('list', {}).get('results', [])
        except Exception:
            if attempt == 2:
                return None
            await asyncio.sleep(2 ** attempt * 3)
    return None


async def process_city(session, sem, city_id, country_id, db_executor, stats):
    try:
        scrape_started_at = int(datetime.now(timezone.utc).timestamp())
        all_hotels = []
        offset = 0

        async with sem:
            while True:
                results = await fetch_page(session, city_id, offset)

                if results is None or not results:
                    break

                for r in results:
                    parsed = parse_hotel(r, city_id, country_id)
                    if parsed:
                        all_hotels.append(parsed)

                if len(results) < PAGE_SIZE:
                    break
                offset += PAGE_SIZE

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(db_executor, _do_write, all_hotels, city_id, scrape_started_at)

        stats['hotels'] += len(all_hotels)
        stats['done'] += 1
    except Exception as e:
        log.error(f"City {city_id}: {e}")
        stats['done'] += 1


async def run(cities):
    sem = asyncio.Semaphore(CONCURRENCY)
    stats = {'done': 0, 'hotels': 0}
    total = len(cities)
    start = time.time()

    db_executor = ThreadPoolExecutor(max_workers=4)

    log.info(f"Starting: {total:,} cities, concurrency={CONCURRENCY}")

    async def progress_reporter():
        while stats['done'] < total:
            await asyncio.sleep(10)
            elapsed = time.time() - start
            rate = int(stats['done'] / elapsed) if elapsed > 0 else 0
            remaining = total - stats['done']
            eta = int(remaining / rate) if rate > 0 else 0
            log.info(f"[{stats['done']:,}/{total:,}] {stats['hotels']:,} hotels | {rate}/s | ETA {eta}s | 429s: {stats_429[0]}")

    connector = aiohttp.TCPConnector(limit=CONCURRENCY, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        reporter = asyncio.create_task(progress_reporter())
        pending = set()

        for city_id, country_id in cities:
            task = asyncio.create_task(
                process_city(session, sem, city_id, country_id, db_executor, stats)
            )
            pending.add(task)
            task.add_done_callback(pending.discard)

            if len(pending) >= CONCURRENCY * 2:
                await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)

        if pending:
            await asyncio.gather(*pending)

        reporter.cancel()

    db_executor.shutdown(wait=True)

    elapsed = time.time() - start
    log.info(f"DONE: {stats['done']:,} cities, {stats['hotels']:,} hotels in {elapsed:.0f}s | 429s: {stats_429[0]}")


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

    stale_threshold = int(datetime.now(timezone.utc).timestamp()) - (7 * 86400)

    conn = get_conn()
    cur = conn.cursor()

    if country_ids:
        placeholders = ','.join(['%s'] * len(country_ids))
        cur.execute(f"""
            SELECT city_id, country_id FROM cities
            WHERE country_id IN ({placeholders})
            AND (hotels_scraped IS NULL OR hotels_scraped = 0
                 OR hotel_scraped_at IS NULL OR hotel_scraped_at < %s){shard_sql}
            ORDER BY country_id
            {f'LIMIT {limit}' if limit else ''}
        """, country_ids + [stale_threshold] + shard_params)
    else:
        cur.execute(f"""
            SELECT city_id, country_id FROM cities
            WHERE (hotels_scraped IS NULL OR hotels_scraped = 0
                  OR hotel_scraped_at IS NULL OR hotel_scraped_at < %s){shard_sql}
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
