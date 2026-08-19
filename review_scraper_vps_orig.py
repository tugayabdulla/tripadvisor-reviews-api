#!/usr/bin/env python3
"""Review scraper for VPS using aiohttp. Always incremental — stops at existing reviews."""

import json
import os
import sys
import time
import logging
import asyncio
import orjson
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass
import aiohttp
import psycopg2
from psycopg2 import pool as pg_pool
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%H:%M:%S')
log = logging.getLogger(__name__)

DB_HOST = os.environ.get('DB_HOST', '127.0.0.1')
DB_NAME = os.environ.get('DB_NAME', 'tripadvisor')
DB_USER = os.environ.get('DB_USER', 'tripadvisor')
DB_PASS = os.environ.get('DB_PASS', '')
CONCURRENCY = int(os.environ.get('CONCURRENCY', '200'))

# Table config — set by --hotels or --attractions flag
SOURCE_TABLE = 'locations'
REVIEW_TABLE = 'reviews'

GRAPHQL_URL = 'https://www.tripadvisor.com/data/graphql/ids'
REVIEW_QUERY_ID = 'ef1a9f94012220d3'
PAGE_SIZE = 20
MAX_OFFSET = 10000000

HEADERS = {
    'accept': '*/*',
    'accept-encoding': 'gzip, deflate',
    'content-type': 'application/json',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'same-origin',
    'sec-fetch-site': 'same-origin',
}

COLUMNS = ["review_id", "location_id", "rating", "title", "text",
    "original_title", "original_text", "language", "original_language",
    "translation_type", "published_date", "user_id", "user_name",
    "user_contributions", "trip_type", "stay_date", "additional_ratings",
    "helpful_votes", "has_mgmt_response", "mgmt_response_text", "mgmt_response_date",
    "photo_urls", "user_avatar", "user_hometown",
    "mgmt_response_author", "mgmt_response_connection",
    "username", "is_verified", "publish_platform", "review_tip"]


_db_pool = None

def get_pool():
    global _db_pool
    if _db_pool is None:
        _db_pool = pg_pool.ThreadedConnectionPool(
            minconn=10, maxconn=100,
            host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS
        )
    return _db_pool


def get_conn():
    return get_pool().getconn()


def put_conn(conn):
    get_pool().putconn(conn)


def clean(val):
    if val is None:
        return "\\N"
    return str(val).replace("\\", "\\\\").replace("\t", " ").replace("\n", " ").replace("\r", "")


def extract_review(r, location_id):
    published_date = r.get('publishedDate', '')
    try:
        timestamp = int(datetime.strptime(published_date, '%Y-%m-%d').timestamp())
    except:
        timestamp = None

    user_profile = r.get('userProfile') or {}
    contribution_counts = user_profile.get('contributionCounts') or {}
    trip_info = r.get('tripInfo')
    mgmt = r.get('mgmtResponse')

    photos = r.get('photos') or []
    photo_urls = []
    for p in photos:
        tpl = (p.get('photo') or {}).get('photoSizeDynamic', {}).get('urlTemplate')
        if tpl:
            photo_urls.append(tpl)

    avatar_data = (user_profile.get('avatar') or {}).get('data') or {}
    user_avatar = (avatar_data.get('photoSizeDynamic') or {}).get('urlTemplate')

    hometown_obj = (user_profile.get('hometown') or {}).get('location') or {}
    user_hometown = (hometown_obj.get('additionalNames') or {}).get('long')

    mgmt_profile = (mgmt.get('userProfile') or {}) if mgmt else {}

    return {
        'review_id': str(r.get('id', '')),
        'location_id': location_id,
        'rating': r.get('rating'),
        'title': r.get('title', ''),
        'text': r.get('text', ''),
        'original_title': r.get('_originalTitle', ''),
        'original_text': r.get('_originalText', ''),
        'language': r.get('language'),
        'original_language': r.get('originalLanguage'),
        'translation_type': r.get('translationType'),
        'published_date': timestamp,
        'user_id': user_profile.get('id'),
        'user_name': user_profile.get('displayName'),
        'user_contributions': contribution_counts.get('sumAllUgc'),
        'trip_type': trip_info.get('tripType') if trip_info else None,
        'stay_date': trip_info.get('stayDate') if trip_info else None,
        'additional_ratings': json.dumps(r.get('additionalRatings')) if r.get('additionalRatings') else None,
        'helpful_votes': r.get('helpfulVotes', 0),
        'has_mgmt_response': mgmt is not None,
        'mgmt_response_text': mgmt.get('text') if mgmt else None,
        'mgmt_response_date': mgmt.get('publishedDate') if mgmt else None,
        'photo_urls': photo_urls if photo_urls else None,
        'user_avatar': user_avatar,
        'user_hometown': user_hometown,
        'mgmt_response_author': mgmt_profile.get('displayName'),
        'mgmt_response_connection': mgmt.get('connectionToSubject') if mgmt else None,
        'username': user_profile.get('username'),
        'is_verified': user_profile.get('isVerified', False),
        'publish_platform': r.get('publishPlatform'),
        'review_tip': ((r.get('reviewTip') or {}).get('responseData') or {}).get('tips', [{}])[0].get('body') if ((r.get('reviewTip') or {}).get('responseData') or {}).get('tips') else None,
    }


def _do_insert(reviews, location_id, api_total_count, complete=True):
    conn = get_conn()
    try:
        cur = conn.cursor()
        from psycopg2.extras import execute_values
        rows = []
        for r in reviews:
            rows.append((
                r['review_id'], r['location_id'], r['rating'],
                r['title'], r['text'],
                r['original_title'], r['original_text'],
                r['language'], r['original_language'],
                r['translation_type'], r['published_date'],
                r['user_id'], r['user_name'],
                r['user_contributions'], r['trip_type'],
                r['stay_date'], r['additional_ratings'],
                r['helpful_votes'], r['has_mgmt_response'],
                r['mgmt_response_text'], r['mgmt_response_date'],
                r['photo_urls'], r['user_avatar'], r['user_hometown'],
                r['mgmt_response_author'], r['mgmt_response_connection'],
                r['username'], r['is_verified'], r['publish_platform'], r['review_tip'],
            ))
        execute_values(cur, f"""
            INSERT INTO {REVIEW_TABLE} (review_id, location_id, rating, title, text,
                original_title, original_text, language, original_language,
                translation_type, published_date, user_id, user_name,
                user_contributions, trip_type, stay_date, additional_ratings,
                helpful_votes, has_mgmt_response, mgmt_response_text, mgmt_response_date,
                photo_urls, user_avatar, user_hometown,
                mgmt_response_author, mgmt_response_connection,
                username, is_verified, publish_platform, review_tip)
            VALUES %s
            ON CONFLICT (review_id, location_id) DO NOTHING
        """, rows)

        if complete:
            cur.execute(f"""
                UPDATE {SOURCE_TABLE} SET review_count = %s, latest_review_date = %s
                WHERE location_id = %s
            """, (api_total_count, int(datetime.now(timezone.utc).timestamp()), location_id))
        else:
            # bailed/incomplete -> keep partial reviews but leave latest_review_date so it retries
            cur.execute(f"UPDATE {SOURCE_TABLE} SET review_count = %s WHERE location_id = %s",
                        (api_total_count, location_id))
        conn.commit()
        cur.close()
    finally:
        put_conn(conn)


def _mark_scraped(location_id):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(f"UPDATE {SOURCE_TABLE} SET latest_review_date = %s WHERE location_id = %s",
                    (int(datetime.now(timezone.utc).timestamp()), location_id))
        conn.commit()
        cur.close()
    finally:
        put_conn(conn)


async def fetch_page(session, location_id, offset, do_translate, limit=PAGE_SIZE):
    payload = [{
        'variables': {
            'locationId': location_id,
            'filters': [],
            'limit': limit,
            'offset': offset,
            'sortBy': 'DATE',
            'language': 'en',
            'doMachineTranslation': do_translate,
        },
        'extensions': {'preRegisteredQueryId': REVIEW_QUERY_ID},
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
                return await resp.json()
        except Exception:
            if attempt == 2:
                return None
            await asyncio.sleep(2 ** attempt * 3)
    return None


async def fetch_all_reviews(session, location_id, stop_before_date=None):
    reviews = []
    seen_ids = set()
    offset = 0
    api_total_count = 0
    consecutive_errors = 0

    # Quick probe: fetch limit=1 to check if newest review is newer than our last check
    if stop_before_date:
        probe = await fetch_page(session, location_id, 0, True, limit=1)
        if probe is not None:
            try:
                p_wrapper = probe[0]['data']['ReviewsProxy_getReviewListPageForLocation'][0]
                api_total_count = p_wrapper.get('totalCount', 0)
                p_reviews = p_wrapper.get('reviews', [])
                if not p_reviews:
                    return [], api_total_count, True
                newest_date = int(datetime.strptime(p_reviews[0].get('publishedDate', ''), '%Y-%m-%d').timestamp())
                if newest_date < stop_before_date:
                    return [], api_total_count, True
            except (KeyError, IndexError, TypeError, ValueError):
                pass

    has_reviews = False
    complete = False  # True only on a clean end; stays False on error-bail / MAX_OFFSET

    while offset < MAX_OFFSET:
        if has_reviews:
            translated_data, original_data = await asyncio.gather(
                fetch_page(session, location_id, offset, True),
                fetch_page(session, location_id, offset, False),
            )
        else:
            translated_data = await fetch_page(session, location_id, offset, True)
            original_data = None

        if translated_data is None:
            consecutive_errors += 1
            if consecutive_errors >= 5:
                break
            await asyncio.sleep(2 * consecutive_errors)
            continue

        try:
            t_result = translated_data[0].get('data', {}).get('ReviewsProxy_getReviewListPageForLocation')
            if not t_result or not t_result[0]:
                consecutive_errors += 1
                if consecutive_errors >= 5:
                    break
                await asyncio.sleep(2 * consecutive_errors)
                continue
            t_wrapper = t_result[0]
            t_reviews = t_wrapper.get('reviews', [])
            api_total_count = t_wrapper.get('totalCount', 0)
        except (KeyError, IndexError, TypeError):
            consecutive_errors += 1
            if consecutive_errors >= 5:
                break
            await asyncio.sleep(2 * consecutive_errors)
            continue

        consecutive_errors = 0

        if not t_reviews:
            complete = True
            break

        if not has_reviews:
            has_reviews = True
            original_data = await fetch_page(session, location_id, offset, False)

        o_reviews = []
        if original_data:
            try:
                o_result = original_data[0].get('data', {}).get('ReviewsProxy_getReviewListPageForLocation')
                o_wrapper = o_result[0] if o_result and o_result[0] else {}
                o_reviews = o_wrapper.get('reviews', [])
            except (KeyError, IndexError, TypeError):
                pass

        original_by_id = {r['id']: r for r in o_reviews}
        hit_existing = False

        for review in t_reviews:
            rid = review.get('id')
            if not rid or rid in seen_ids:
                continue

            if stop_before_date:
                try:
                    pub = int(datetime.strptime(review.get('publishedDate', ''), '%Y-%m-%d').timestamp())
                    if pub < stop_before_date:
                        hit_existing = True
                        break
                except:
                    pass

            seen_ids.add(rid)

            original = original_by_id.get(rid)
            if original:
                review['_originalText'] = original.get('text', '')
                review['_originalTitle'] = original.get('title', '')
            else:
                review['_originalText'] = review.get('text', '')
                review['_originalTitle'] = review.get('title', '')

            try:
                reviews.append(extract_review(review, location_id))
            except Exception:
                pass

        if hit_existing:
            complete = True
            break

        if api_total_count and offset + PAGE_SIZE >= api_total_count:
            complete = True
            break

        offset += PAGE_SIZE

    return reviews, api_total_count, complete


# Global 429 counter
stats_429 = [0]


async def process_location(session, sem, location_id, db_executor, stats, dry_run, stop_before_date):
    try:
        async with sem:
            reviews, api_total_count, complete = await fetch_all_reviews(session, location_id, stop_before_date)

        if not dry_run:
            loop = asyncio.get_event_loop()
            if reviews:
                await loop.run_in_executor(db_executor, _do_insert, reviews, location_id, api_total_count, complete)
            elif complete:
                # genuinely no new reviews -> mark scraped. If the fetch BAILED (not complete),
                # do NOT mark -> avoids locking in a location that errored before returning reviews.
                await loop.run_in_executor(db_executor, _mark_scraped, location_id)

        stats['reviews'] += len(reviews)
        stats['done'] += 1
    except Exception as e:
        log.error(f"Location {location_id}: {e}")
        stats['done'] += 1


async def run(location_ids, stop_dates, dry_run=False):
    sem = asyncio.Semaphore(CONCURRENCY)
    stats = {'done': 0, 'reviews': 0}
    total = len(location_ids)
    start = time.time()

    db_executor = ThreadPoolExecutor(max_workers=100)

    mode = "DRY RUN" if dry_run else "LIVE"
    log.info(f"[{mode}] Starting: {total:,} locations, concurrency={CONCURRENCY}")

    async def progress_reporter():
        prev_done = 0
        prev_time = start
        while stats['done'] < total:
            await asyncio.sleep(10)
            now = time.time()
            dt = now - prev_time
            dd = stats['done'] - prev_done
            rate = dd / dt if dt > 0 else 0
            remaining = total - stats['done']
            eta_s = int(remaining / rate) if rate > 0 else 0
            eta_h, eta_m = divmod(eta_s // 60, 60)
            log.info(f"[{stats['done']:,}/{total:,}] {stats['reviews']:,} reviews | {rate:.0f}/s | ETA {eta_h}h{eta_m:02d}m | 429s: {stats_429[0]}")
            prev_done = stats['done']
            prev_time = now

    connector = aiohttp.TCPConnector(limit=CONCURRENCY, ttl_dns_cache=300, keepalive_timeout=30)
    async with aiohttp.ClientSession(connector=connector) as session:
        reporter = asyncio.create_task(progress_reporter())
        pending = set()

        for lid in location_ids:
            stop_date = stop_dates.get(lid)
            task = asyncio.create_task(
                process_location(session, sem, lid, db_executor, stats, dry_run, stop_date)
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
    log.info(f"DONE: {total:,} locations, {stats['reviews']:,} reviews in {elapsed:.0f}s | 429s: {stats_429[0]}")


def main():
    global SOURCE_TABLE, REVIEW_TABLE
    argv = sys.argv[1:]
    dry_run = '--dry-run' in argv
    if '--hotels' in argv:
        SOURCE_TABLE = 'hotels'
        REVIEW_TABLE = 'hotel_reviews'
    elif '--attraction-products' in argv:
        SOURCE_TABLE = 'attraction_products'
        REVIEW_TABLE = 'attraction_product_reviews'
    elif '--attractions' in argv:
        SOURCE_TABLE = 'attractions'
        REVIEW_TABLE = 'attraction_reviews'
    limit = int(argv[argv.index('--limit') + 1]) if '--limit' in argv else None
    min_reviews = int(argv[argv.index('--min-reviews') + 1]) if '--min-reviews' in argv else None
    max_reviews = int(argv[argv.index('--max-reviews') + 1]) if '--max-reviews' in argv else None
    skip = set()
    for flag in ('--dry-run', '--limit', '--min-reviews', '--max-reviews', '--hotels', '--attractions', '--attraction-products', '--older-than'):
        if flag in argv:
            idx = argv.index(flag)
            skip.add(idx)
            if flag not in ('--dry-run', '--hotels', '--attractions', '--attraction-products'):
                skip.add(idx + 1)
    country_ids = [int(a) for i, a in enumerate(argv) if i not in skip and a.isdigit()]

    older_than_days = int(argv[argv.index('--older-than') + 1]) if '--older-than' in argv else 1
    stale_threshold = int(datetime.now(timezone.utc).timestamp()) - (older_than_days * 86400)

    conn = get_conn()
    cur = conn.cursor()

    cur.execute(f"SELECT location_id, latest_review_date FROM {SOURCE_TABLE} WHERE latest_review_date IS NOT NULL")
    stop_dates = {row[0]: row[1] for row in cur.fetchall()}
    log.info(f"Loaded {len(stop_dates):,} stop dates from {SOURCE_TABLE}")

    if country_ids:
        placeholders = ','.join(['%s'] * len(country_ids))
        cur.execute(f"""
            SELECT location_id FROM {SOURCE_TABLE}
            WHERE country_id IN ({placeholders})
            AND (latest_review_date IS NULL OR latest_review_date < %s)
            ORDER BY review_count DESC NULLS LAST
            {f'LIMIT {limit}' if limit else ''}
        """, country_ids + [stale_threshold])
    else:
        review_filter = ""
        params = [stale_threshold]
        if min_reviews is not None:
            review_filter += " AND review_count >= %s"
            params.append(min_reviews)
        if max_reviews is not None:
            review_filter += " AND (review_count < %s OR review_count IS NULL)"
            params.append(max_reviews)
        cur.execute(f"""
            SELECT location_id FROM {SOURCE_TABLE}
            WHERE (latest_review_date IS NULL OR latest_review_date < %s)
            {review_filter}
            ORDER BY review_count DESC NULLS LAST
            {f'LIMIT {limit}' if limit else ''}
        """, params)

    location_ids = [row[0] for row in cur.fetchall()]
    cur.close()
    put_conn(conn)

    if not location_ids:
        log.info("No locations to scrape")
        return

    asyncio.run(run(location_ids, stop_dates, dry_run))


if __name__ == '__main__':
    main()
