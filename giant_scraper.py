#!/usr/bin/env python3
"""Aggressive parallel scraper for mega-landmarks with flaky review APIs.
Fetches all offsets in parallel; tracks failed offsets and retries them in rounds
until none remain. One location at a time."""
import asyncio, aiohttp, sys, os, time, logging
import orjson
sys.path.insert(0, '/data/tripadvisor')
import review_scraper_vps as R

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%H:%M:%S')
log = logging.getLogger(__name__)

R.SOURCE_TABLE = os.environ.get('SOURCE_TABLE', 'attractions')
R.REVIEW_TABLE = os.environ.get('REVIEW_TABLE', 'attraction_reviews')

GRAPHQL_URL = R.GRAPHQL_URL
HEADERS = R.HEADERS
QID = R.REVIEW_QUERY_ID
PAGE = 20
CONC = int(os.environ.get('CONCURRENCY', '60'))
PAGE_RETRY = int(os.environ.get('PAGE_RETRY', '8'))      # retries within one fetch call
BACKOFF_CAP = float(os.environ.get('BACKOFF_CAP', '2'))  # max sleep between retries
MAX_ROUNDS = int(os.environ.get('MAX_ROUNDS', '15'))     # retry rounds over failed offsets

async def fetch(session, lid, offset, translate):
    payload = [{'variables':{'locationId':lid,'filters':[],'limit':PAGE,'offset':offset,
                'sortBy':'DATE','language':'en','doMachineTranslation':translate},
                'extensions':{'preRegisteredQueryId':QID}}]
    for attempt in range(PAGE_RETRY):
        try:
            async with session.post(GRAPHQL_URL, json=payload, headers=HEADERS,
                                    timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status >= 500 or resp.status == 429:
                    await asyncio.sleep(min(2**attempt * 0.3, BACKOFF_CAP)); continue
                d = await resp.json(loads=orjson.loads)
                w = d[0].get('data', {}).get('ReviewsProxy_getReviewListPageForLocation')
                if w and w[0] is not None:
                    return w[0]
                await asyncio.sleep(min(2**attempt * 0.3, BACKOFF_CAP))
        except Exception:
            await asyncio.sleep(min(2**attempt * 0.3, BACKOFF_CAP))
    return None

async def fetch_offsets(session, lid, offsets, reviews, seen, label):
    """Fetch a set of offsets concurrently. Returns the set of offsets that failed."""
    sem = asyncio.Semaphore(CONC)
    failed = set()
    prog = {'n':0}
    total = len(offsets)

    async def do(offset):
        async with sem:
            tdata, odata = await asyncio.gather(
                fetch(session, lid, offset, True),
                fetch(session, lid, offset, False),
            )
        prog['n'] += 1
        if prog['n'] % 500 == 0:
            log.info(f"  {lid} [{label}]: {prog['n']}/{total} pages, {len(reviews)} reviews, {len(failed)} failed")
        if tdata is None:
            failed.add(offset)
            return
        t_reviews = tdata.get('reviews', [])
        o_by_id = {r['id']: r for r in (odata.get('reviews', []) if odata else [])}
        for rv in t_reviews:
            rid = rv.get('id')
            if not rid or rid in seen:
                continue
            seen.add(rid)
            o = o_by_id.get(rid)
            rv['_originalText'] = (o or rv).get('text','')
            rv['_originalTitle'] = (o or rv).get('title','')
            try:
                reviews[rid] = R.extract_review(rv, lid)
            except Exception:
                pass

    await asyncio.gather(*[do(o) for o in offsets])
    return failed

async def scrape_one(session, lid, total):
    offsets = list(range(0, total + PAGE, PAGE))
    reviews, seen = {}, set()
    t0 = time.time()

    failed = await fetch_offsets(session, lid, offsets, reviews, seen, "pass1")
    rnd = 0
    while failed and rnd < MAX_ROUNDS:
        rnd += 1
        log.info(f"  {lid}: retry round {rnd}, {len(failed)} failed offsets")
        failed = await fetch_offsets(session, lid, sorted(failed), reviews, seen, f"r{rnd}")

    fpath = f"/data/tripadvisor/giant_failed_{lid}.txt"
    if failed:
        with open(fpath, "w") as f:
            for o in sorted(failed):
                f.write(f"{o}\n")
        log.info(f"  {lid}: {len(failed)} offsets STILL failed after {MAX_ROUNDS} rounds -> saved to giant_failed_{lid}.txt")
    else:
        if os.path.exists(fpath):
            os.remove(fpath)   # fully recovered -> clear the failed flag
        log.info(f"  {lid}: all offsets succeeded, 0 lost")

    complete = (len(failed) == 0)   # incomplete -> R._do_insert won't lock latest_review_date, so it retries
    revs = list(reviews.values())
    log.info(f"  {lid}: collected {len(revs):,} reviews in {time.time()-t0:.0f}s")
    if revs:
        loop = asyncio.get_event_loop()
        from concurrent.futures import ThreadPoolExecutor
        ex = ThreadPoolExecutor(max_workers=4)
        await loop.run_in_executor(ex, R._do_insert, revs, lid, total, complete)
        ex.shutdown(wait=True)
    elif not failed:
        R._mark_scraped(lid)
    return len(revs), len(failed)

async def main():
    argv = sys.argv[1:]
    if '--retry-failed' in argv:
        import glob, re
        lids = []
        for p in glob.glob('/data/tripadvisor/giant_failed_*.txt'):
            m = re.search(r'giant_failed_(\d+)\.txt', p)
            if m:
                lids.append(int(m.group(1)))
        log.info(f"--retry-failed: re-ingesting {len(lids)} locations with saved failed offsets")
    else:
        lids = [int(x) for x in argv]
    if not lids:
        print("usage: giant_scraper.py <lid> [lid...]  |  giant_scraper.py --retry-failed"); return
    connector = aiohttp.TCPConnector(limit=CONC, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        for lid in lids:
            w = await fetch(session, lid, 0, True)
            if w is None:
                log.warning(f"{lid}: could not get totalCount — skipping"); continue
            total = w.get('totalCount', 0)
            log.info(f"=== {lid}: totalCount={total:,}, conc={CONC}, page_retry={PAGE_RETRY}, backoff_cap={BACKOFF_CAP}, max_rounds={MAX_ROUNDS} ===")
            n, lost = await scrape_one(session, lid, total)
            log.info(f"=== {lid}: DONE, inserted {n:,} reviews, {lost} offsets unrecovered ===")

if __name__ == '__main__':
    asyncio.run(main())
