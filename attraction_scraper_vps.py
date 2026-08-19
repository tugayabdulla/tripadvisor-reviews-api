#!/usr/bin/env python3
"""Attraction discovery scraper — two-pass: map pins (coordinates) + list cards (rich data)."""

import os
import sys
import re
import time
import logging
import asyncio
import orjson
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
QUERY_ID = '0a9ecd5c0d013d56'
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


def make_payload(geo_id, page_num, map_surface):
    params = {
        'geoId': int(geo_id),
        'filters': [{'id': 'allAttractions', 'value': ['true']}],
        'contentType': 'attraction',
        'webVariant': 'AttractionsFusion',
    }
    if page_num > 0:
        params['pagee'] = str(page_num * PAGE_SIZE)
    return [{
        'variables': {
            'request': {
                'tracking': {'screenName': 'AttractionsFusion', 'pageviewUid': str(uuid.uuid4())},
                'routeParameters': params,
                'updateToken': None,
            },
            'commerce': {'attractionCommerce': {'pax': [{'ageBand': 'ADULT', 'count': 2}]}},
            'tracking': {'screenName': 'AttractionsFusion', 'pageviewUid': str(uuid.uuid4())},
            'sessionId': uuid.uuid4().hex.upper(),
            'unitLength': 'MILES',
            'currency': 'USD',
            'currentGeoPoint': None,
            'mapSurface': map_surface,
            'debug': False,
            'polling': False,
        },
        'extensions': {'preRegisteredQueryId': QUERY_ID},
    }]


async def fetch_page(session, geo_id, page_num, map_surface):
    payload = make_payload(geo_id, page_num, map_surface)
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
                return data[0]['data']['Result'][0]
        except Exception:
            if attempt == 2:
                return None
            await asyncio.sleep(2 ** attempt * 3)
    return None


def parse_pin(pin, city_id, country_id):
    url = (pin.get('cardLink') or {}).get('webRoute', {}).get('webLinkUrl', '')
    m = re.search(r'-d(\d+)-', url)
    if not m:
        return None
    name_obj = pin.get('locationName')
    name = name_obj.get('text', '') if isinstance(name_obj, dict) else (name_obj or '')
    geo = pin.get('geoPoint') or {}
    return {
        'location_id': int(m.group(1)),
        'city_id': city_id,
        'country_id': country_id,
        'name': name,
        'url': url,
        'latitude': geo.get('latitude'),
        'longitude': geo.get('longitude'),
    }


def parse_card(section):
    card = section.get('singleFlexCardContent')
    if not card:
        return None
    url = (card.get('cardLink') or {}).get('webRoute', {}).get('webLinkUrl', '')
    m = re.search(r'-d(\d+)-', url)
    if not m:
        return None
    def text_or(val):
        if isinstance(val, dict):
            return val.get('text', '')
        return val or ''

    bubble = card.get('bubbleRating') or {}
    photo = card.get('cardPhoto') or {}
    sizes = photo.get('sizes') or {}
    ordinal = card.get('ordinalPrefix', '')
    rank_match = re.match(r'(\d+)', ordinal) if ordinal else None
    return {
        'location_id': int(m.group(1)),
        'name': text_or(card.get('cardTitle')),
        'url': url,
        'rating': bubble.get('rating'),
        'review_count': bubble.get('reviewCount'),
        'categories': text_or(card.get('primaryInfo')),
        'description': text_or(card.get('descriptiveText')),
        'open_status': text_or(card.get('openStatus')) or None,
        'ranking_position': int(rank_match.group(1)) if rank_match else None,
        'thumbnail': sizes.get('urlTemplate'),
        'merchandising_text': text_or(card.get('merchandisingText')) or None,
    }


def _do_write_pins(attractions, city_id, country_id, scrape_started_at):
    conn = get_conn()
    try:
        if attractions:
            cur = conn.cursor()
            seen = set()
            rows = []
            for a in attractions:
                if a['location_id'] not in seen:
                    seen.add(a['location_id'])
                    rows.append((
                        a['location_id'], a.get('country_id'), a.get('city_id'),
                        a.get('name'), a.get('url'),
                        a.get('latitude'), a.get('longitude'),
                    ))
            execute_values(cur, """
                INSERT INTO attractions (location_id, country_id, city_id, name, url, latitude, longitude)
                VALUES %s
                ON CONFLICT (location_id) DO UPDATE SET
                    name = EXCLUDED.name, url = EXCLUDED.url,
                    latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude
            """, rows)
            conn.commit()
            cur.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        put_conn(conn)


def _do_write_cards(cards, city_id, country_id, scrape_started_at):
    conn = get_conn()
    try:
        if cards:
            cur = conn.cursor()
            def flat(v):  # some fields occasionally arrive as {'text': ...} dicts; psycopg2 can't adapt those
                return v.get('text', str(v)) if isinstance(v, dict) else v
            seen = set()
            rows = []
            for c in cards:
                if c['location_id'] not in seen:
                    seen.add(c['location_id'])
                    rows.append(tuple(flat(v) for v in (
                        c['location_id'], country_id, city_id,
                        c.get('name'), c.get('url'),
                        c.get('rating'), c.get('review_count'), c.get('categories'),
                        c.get('description'), c.get('open_status'),
                        c.get('ranking_position'), c.get('thumbnail'),
                        c.get('merchandising_text'),
                    )))
            # UPSERT so list-only attractions (no map pin) still get a row, instead of being
            # silently dropped by an UPDATE that matches nothing. COALESCE preserves existing
            # data (e.g. lat/long from the pins pass) when a card omits a field.
            execute_values(cur, """
                INSERT INTO attractions (location_id, country_id, city_id, name, url,
                    rating, review_count, categories, description, open_status,
                    ranking_position, thumbnail, merchandising_text)
                VALUES %s
                ON CONFLICT (location_id) DO UPDATE SET
                    name = COALESCE(EXCLUDED.name, attractions.name),
                    url = COALESCE(EXCLUDED.url, attractions.url),
                    rating = COALESCE(EXCLUDED.rating, attractions.rating),
                    review_count = COALESCE(EXCLUDED.review_count, attractions.review_count),
                    categories = COALESCE(EXCLUDED.categories, attractions.categories),
                    description = COALESCE(EXCLUDED.description, attractions.description),
                    open_status = COALESCE(EXCLUDED.open_status, attractions.open_status),
                    -- ranking_position intentionally NOT updated here: enrichment owns the
                    -- (position,total) pair from one source. Discovery's card rank is a different
                    -- scale and overwriting it alone produced position > total. New rows still get
                    -- the discovery value via the INSERT column list above.
                    thumbnail = COALESCE(EXCLUDED.thumbnail, attractions.thumbnail),
                    merchandising_text = COALESCE(EXCLUDED.merchandising_text, attractions.merchandising_text)
            """, rows)
            conn.commit()
            cur.close()

        cur = conn.cursor()
        cur.execute(
            "UPDATE cities SET attractions_scraped = %s, attraction_scraped_at = %s WHERE city_id = %s",
            (len(cards), scrape_started_at, city_id)
        )
        conn.commit()
        cur.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        put_conn(conn)


async def process_city(session, sem, city_id, country_id, db_executor, stats):
    try:
        scrape_started_at = int(datetime.now(timezone.utc).timestamp())

        async with sem:
            # Pass 1: map pins (coordinates)
            all_pins = []
            page_num = 0
            while True:
                result = await fetch_page(session, city_id, page_num, True)
                if result is None:
                    break
                pins = (result.get('mapSections') or [{}])[0].get('pins', [])
                if not pins:
                    break
                for pin in pins:
                    parsed = parse_pin(pin, city_id, country_id)
                    if parsed:
                        all_pins.append(parsed)
                if len(pins) < PAGE_SIZE:
                    break
                page_num += 1

            # Write pins
            if all_pins:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(db_executor, _do_write_pins, all_pins, city_id, country_id, scrape_started_at)

            # Pass 2: list cards (rich data)
            all_cards = []
            page_num = 0
            while True:
                result = await fetch_page(session, city_id, page_num, False)
                if result is None:
                    break
                sections = result.get('sections', [])
                page_cards = []
                for sec in sections:
                    if sec.get('__typename') == 'WebPresentation_SingleFlexCardSection':
                        card = parse_card(sec)
                        if card:
                            page_cards.append(card)
                if not page_cards:
                    break
                all_cards.extend(page_cards)
                if len(page_cards) < PAGE_SIZE:
                    break
                page_num += 1

            # Write cards
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(db_executor, _do_write_cards, all_cards, city_id, country_id, scrape_started_at)

        stats['attractions'] += max(len(all_pins), len(all_cards))
        stats['done'] += 1
    except Exception as e:
        log.error(f"City {city_id}: {e}")
        stats['done'] += 1


async def run(cities):
    sem = asyncio.Semaphore(CONCURRENCY)
    stats = {'done': 0, 'attractions': 0}
    total = len(cities)
    start = time.time()
    db_executor = ThreadPoolExecutor(max_workers=4)

    log.info(f"Starting: {total:,} cities, concurrency={CONCURRENCY} (two-pass: pins + cards)")

    async def progress_reporter():
        prev_done, prev_time = 0, start
        while stats['done'] < total:
            await asyncio.sleep(10)
            now = time.time()
            dt = now - prev_time
            dd = stats['done'] - prev_done
            rate = dd / dt if dt > 0 else 0
            remaining = total - stats['done']
            eta_s = int(remaining / rate) if rate > 0 else 0
            eta_h, eta_m = divmod(eta_s // 60, 60)
            log.info(f"[{stats['done']:,}/{total:,}] {stats['attractions']:,} attractions | {rate:.0f}/s | ETA {eta_h}h{eta_m:02d}m | 429s: {stats_429[0]}")
            prev_done, prev_time = stats['done'], now

    connector = aiohttp.TCPConnector(limit=CONCURRENCY, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        reporter = asyncio.create_task(progress_reporter())
        pending = set()
        for city_id, country_id in cities:
            task = asyncio.create_task(process_city(session, sem, city_id, country_id, db_executor, stats))
            pending.add(task)
            task.add_done_callback(pending.discard)
            if len(pending) >= CONCURRENCY * 2:
                await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
        if pending:
            await asyncio.gather(*pending)
        reporter.cancel()

    db_executor.shutdown(wait=True)
    elapsed = time.time() - start
    log.info(f"DONE: {stats['done']:,} cities, {stats['attractions']:,} attractions in {elapsed:.0f}s | 429s: {stats_429[0]}")


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
            AND (attractions_scraped IS NULL OR attractions_scraped = 0
                 OR attraction_scraped_at IS NULL OR attraction_scraped_at < %s){shard_sql}
            ORDER BY country_id
            {f'LIMIT {limit}' if limit else ''}
        """, country_ids + [stale_threshold] + shard_params)
    else:
        cur.execute(f"""
            SELECT city_id, country_id FROM cities
            WHERE (attractions_scraped IS NULL OR attractions_scraped = 0
                  OR attraction_scraped_at IS NULL OR attraction_scraped_at < %s){shard_sql}
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
