#!/usr/bin/env python3
"""City discovery scraper — curl_cffi + cookie (sequential).

Replaces the Selenium/Docker city_scraper. Fetches the country Restaurants
listing pages (server-rendered HTML) via curl_cffi using a CloakBrowser-acquired
cookie through a proxy, paginates the `oa` offsets, parses city geo links with
lxml, and writes new cities to Postgres.

Verified equivalent to the Selenium scraper: Japan (g294232) returns the same
1758 cities in ~4 min vs ~52 min.

Usage:
    python city_scraper_vps.py                 # all countries, by total_restaurants desc
    python city_scraper_vps.py --missing-only  # only countries with 0 cities so far
    python city_scraper_vps.py 294232 187768   # specific country ids
    python city_scraper_vps.py --limit 50 --dry-run
"""

import os
import sys
import re
import time
import random
import logging
import psycopg2
from psycopg2.extras import execute_values
from curl_cffi import requests as curl_requests
from lxml import html as LH

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%H:%M:%S')
log = logging.getLogger(__name__)

WEBSHARE_URL = os.environ.get('WEBSHARE_URL', '')
PROXY_FILE = os.environ.get('PROXY_FILE', '/data/tripadvisor/proxies.txt')
DONE_FILE = os.environ.get('CITY_DONE_FILE', '/data/tripadvisor/city_vps_done.txt')

DB_HOST = os.environ.get('DB_HOST', '127.0.0.1')
DB_NAME = os.environ.get('DB_NAME', 'tripadvisor')
DB_USER = os.environ.get('DB_USER', 'tripadvisor')
DB_PASS = os.environ.get('DB_PASS', '')

PAGE_SIZE = 20
MAX_OFFSET = 200000          # safety cap (~10k pages) — no real country is this big
MAX_REACQUIRE = 6            # consecutive cookie re-acquires before giving up on a page


def get_conn():
    return psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)


# ── Proxies + cookie ──

def load_proxies():
    if WEBSHARE_URL:
        import requests
        resp = requests.get(WEBSHARE_URL, timeout=15)
        lines = [l.strip() for l in resp.text.strip().split('\n') if l.strip()]
    elif os.path.exists(PROXY_FILE):
        with open(PROXY_FILE) as f:
            lines = [l.strip() for l in f if l.strip()]
    else:
        raise SystemExit("No WEBSHARE_URL env and no proxy file — cannot continue")
    random.shuffle(lines)
    log.info(f"Loaded {len(lines)} proxies")
    return lines


def _proxy_parts(line):
    p = line.split(':')
    if len(p) == 4:
        ip, port, user, pw = p
        return f"http://{user}:{pw}@{ip}:{port}", {"server": f"http://{ip}:{port}", "username": user, "password": pw}, f"{ip}:{port}"
    if len(p) == 2:
        ip, port = p
        return f"http://{ip}:{port}", {"server": f"http://{ip}:{port}"}, f"{ip}:{port}"
    return None


def acquire_cookie(proxies):
    """Launch CloakBrowser through a proxy, return (cookies, proxy_url) or (None, None)."""
    from cloakbrowser import launch
    for line in random.sample(proxies, min(len(proxies), 10)):
        pp = _proxy_parts(line)
        if not pp:
            continue
        purl, pdict, tag = pp
        browser = None
        try:
            browser = launch(headless=True, proxy=pdict)
            page = browser.new_page()
            page.goto("https://www.tripadvisor.com/Restaurants", timeout=30000)
            time.sleep(5)
            cookies = {c['name']: c['value'] for c in page.context.cookies()}
            browser.close()
            if len(cookies) > 5:
                log.info(f"Cookie acquired via {tag} ({len(cookies)} cookies)")
                return cookies, purl
        except Exception:
            try:
                browser and browser.close()
            except Exception:
                pass
    return None, None


# ── Parsing ──

def parse_cities(htmltext, offset):
    doc = LH.fromstring(htmltext)
    if offset == 0:
        anchors = doc.xpath("//div[contains(concat(' ',normalize-space(@class),' '),' geo_name ')]/a")
    else:
        anchors = doc.xpath("//ul[contains(concat(' ',normalize-space(@class),' '),' geoList ')]//li//a")
    out = []
    for a in anchors:
        href = a.get('href') or ''
        m = re.search(r'-g(\d+)-', href)
        if m:
            out.append((int(m.group(1)), (a.text_content() or '').strip().removesuffix(' Restaurants')))
    return out


def is_blocked(htmltext, status):
    low = htmltext.lower()
    return status != 200 or any(m in low for m in ("captcha-delivery", "datadome", "access blocked", "geo.captcha"))


# ── DB ──

def get_existing_city_ids(country_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT city_id FROM cities WHERE country_id = %s", (country_id,))
    existing = {row[0] for row in cur.fetchall()}
    cur.close()
    conn.close()
    return existing


def write_cities(cities, existing, country_id):
    new = [(cid, country_id, name) for cid, name in cities.items() if cid not in existing]
    if not new:
        return 0
    conn = get_conn()
    cur = conn.cursor()
    execute_values(cur,
                   "INSERT INTO cities (city_id, country_id, name) VALUES %s ON CONFLICT DO NOTHING",
                   new)
    conn.commit()
    cur.close()
    conn.close()
    return len(new)


# ── Scrape one country ──

class CookieState:
    __slots__ = ("cookies", "proxy_url")

    def __init__(self):
        self.cookies = None
        self.proxy_url = None

    def ensure(self, proxies):
        if self.cookies is None:
            self.cookies, self.proxy_url = acquire_cookie(proxies)
        return self.cookies is not None

    def rotate(self, proxies):
        self.cookies, self.proxy_url = acquire_cookie(proxies)
        return self.cookies is not None


def scrape_country(country_id, state, proxies):
    """Return dict {city_id: name} for the country, or None on hard failure."""
    cities = {}
    offset = 0
    reacquire = 0

    while offset < MAX_OFFSET:
        url = f"https://www.tripadvisor.com/Restaurants-g{country_id}-oa{offset}-Country.html"
        try:
            r = curl_requests.get(url, impersonate="chrome",
                                  cookies=state.cookies,
                                  proxies={"https": state.proxy_url, "http": state.proxy_url},
                                  timeout=30)
        except Exception:
            if reacquire >= MAX_REACQUIRE or not state.rotate(proxies):
                return None
            reacquire += 1
            continue

        if is_blocked(r.text, r.status_code):
            if reacquire >= MAX_REACQUIRE or not state.rotate(proxies):
                return None
            reacquire += 1
            continue

        # keep cookies fresh
        for k, v in r.cookies.items():
            state.cookies[k] = v
        reacquire = 0

        page = parse_cities(r.text, offset)
        for cid, name in page:
            if cid != country_id and cid not in cities:
                cities[cid] = name

        if len(page) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    return cities


# ── Country selection ──

def select_countries(argv):
    ids = [int(a) for a in argv if a.isdigit()]
    limit = int(argv[argv.index('--limit') + 1]) if '--limit' in argv else None
    missing_only = '--missing-only' in argv

    conn = get_conn()
    cur = conn.cursor()
    if ids:
        cur.execute("SELECT country_id, name FROM countries WHERE country_id = ANY(%s)", (ids,))
        rows = cur.fetchall()
    elif missing_only:
        cur.execute("""
            SELECT c.country_id, c.name FROM countries c
            WHERE NOT EXISTS (SELECT 1 FROM cities ci WHERE ci.country_id = c.country_id)
            ORDER BY c.total_restaurants DESC NULLS LAST
        """)
        rows = cur.fetchall()
    else:
        cur.execute("SELECT country_id, name FROM countries ORDER BY total_restaurants DESC NULLS LAST")
        rows = cur.fetchall()
    cur.close()
    conn.close()

    if limit:
        rows = rows[:limit]
    return rows


def load_done():
    if os.path.exists(DONE_FILE):
        with open(DONE_FILE) as f:
            return {int(l.strip()) for l in f if l.strip().isdigit()}
    return set()


def mark_done(country_id):
    with open(DONE_FILE, 'a') as f:
        f.write(f"{country_id}\n")


def main():
    argv = sys.argv[1:]
    dry_run = '--dry-run' in argv

    proxies = load_proxies()
    countries = select_countries(argv)
    done = load_done()
    todo = [(cid, name) for cid, name in countries if cid not in done]

    log.info(f"{len(todo)} countries to process ({len(done)} already done){' [DRY RUN]' if dry_run else ''}")

    state = CookieState()
    if not state.ensure(proxies):
        raise SystemExit("Could not acquire an initial cookie")

    start = time.time()
    total_new = 0
    for i, (cid, name) in enumerate(todo, 1):
        t0 = time.time()
        cities = scrape_country(cid, state, proxies)
        if cities is None:
            log.warning(f"[{i}/{len(todo)}] {cid} ({name}): FAILED (blocked) — not marking done")
            continue

        if dry_run:
            log.info(f"[{i}/{len(todo)}] {cid} ({name}): {len(cities)} cities found in {time.time()-t0:.1f}s [dry-run, no write]")
            continue

        existing = get_existing_city_ids(cid)
        written = write_cities(cities, existing, cid)
        total_new += written
        mark_done(cid)
        log.info(f"[{i}/{len(todo)}] {cid} ({name}): {len(cities)} found, {written} new in {time.time()-t0:.1f}s")

    elapsed = time.time() - start
    log.info(f"DONE: {len(todo)} countries, {total_new} new cities, {elapsed:.0f}s")


if __name__ == '__main__':
    main()
