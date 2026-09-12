import os
import re
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import execute_batch
from curl_cffi import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def clean_number(text: str) -> Optional[float]:
    if not text:
        return None
    cleaned = text.replace(",", "").strip()
    match = re.search(r"([+-]?\d+(?:\.\d+)?)", cleaned)
    return float(match.group(1)) if match else None


EXCLUDED_PORT_TERMS = {
    "eua",
    "eu ets",
    "brent",
    "wti",
    "crude",
    "global",
    "market",
    "index",
    "average",
    "port",
    "region",
}

BASE_URL = "https://shipandbunker.com"

LISTING_ENDPOINTS = {
    "Global Benchmark": f"{BASE_URL}/prices",
    "Americas": f"{BASE_URL}/prices/am",
    "Asia-Pacific": f"{BASE_URL}/prices/apac",
    "EMEA": f"{BASE_URL}/prices/emea",
}

# Detail-page section headings we look for, mapped to our fuel_grade enum.
# "MGO" is deliberately skipped: on every port checked it duplicates "LSMGO"
# exactly (same series under two labels on this site) -- taking both would
# double-insert identical rows.
SECTION_TO_GRADE = {
    "Latest Prices, VLSFO": "VLSFO",
    "Latest Prices, LSMGO": "LSMGO",
    "Latest Prices, IFO380 (HSFO)": "IFO380",
}

CURRENT_YEAR = datetime.now(timezone.utc).year


# ---------------------------------------------------------------------------
# 1. Port discovery -- walk the 4 listing pages, collect every real port's own
#    detail-page URL. Most of these are subscription-gated (no free data);
#    that's fine, they just contribute 0 records below. Discovering all of
#    them (rather than hand-listing a fixed set) means any port Ship & Bunker
#    unlocks in the future is picked up automatically on the next run.
# ---------------------------------------------------------------------------


def discover_ports() -> List[Tuple[str, str, str]]:
    """Returns [(port_name, detail_url, section_name)], deduped by detail_url."""
    seen: Dict[str, Tuple[str, str, str]] = {}
    for section_name, url in LISTING_ENDPOINTS.items():
        try:
            resp = requests.get(url, impersonate="chrome120", timeout=20)
        except Exception as e:
            print(f"Error fetching listing page {section_name}: {e}")
            continue
        if resp.status_code != 200:
            print(f"HTTP {resp.status_code} fetching {section_name} ({url})")
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/prices/" not in href or href.rstrip("/").endswith(
                ("/am", "/apac", "/emea", "/prices")
            ):
                continue
            path = href.split("shipandbunker.com")[-1]
            # Detail-page hrefs have 3+ path segments after /prices/, e.g.
            # /prices/emea/me/ae-fjr-fujairah -- listing-page anchors to
            # region tabs or averages don't.
            if path.count("/") < 4:
                continue
            name = a.get_text(strip=True)
            # Anchor text sometimes has the day's price glued on (a colspan
            # artifact) or trails "Bunker Prices" from a page-title link --
            # strip both down to the port name.
            m = re.match(r"^[A-Za-z][A-Za-z\s/().'-]*", name)
            name = m.group(0).strip() if m else name
            name = re.sub(r"\s*Bunker Prices\s*$", "", name, flags=re.IGNORECASE).strip()
            if (
                not name
                or name.lower() in EXCLUDED_PORT_TERMS
                or "average" in name.lower()
                or path.startswith("/prices/av/")
                or path.startswith("/prices/ea/")  # EUA etc.
            ):
                continue
            full_url = (href if href.startswith("http") else f"{BASE_URL}{href}").split("#")[0]
            if full_url not in seen:
                seen[full_url] = (name, full_url, section_name)
        time.sleep(1.0)
    return list(seen.values())


# ---------------------------------------------------------------------------
# 2. Per-port page parsing -- each port's own page has a distinct "Latest
#    Prices, <GRADE>" table per fuel grade (Date | Price $/mt | Change | High
#    | Low | Spread), one row per trading day. No colspan-shared headers here
#    (that ambiguity only exists on the aggregate listing tables this
#    replaces), so there's no price/delta-column collision to guard against.
# ---------------------------------------------------------------------------


def parse_date_cell(date_cell: str) -> Optional[str]:
    # e.g. "F Sep 11" -> "2026-09-11"
    m = re.search(r"([A-Za-z]{3})\s+(\d{1,2})", date_cell)
    if not m:
        return None
    month_str, day_str = m.group(1), m.group(2)
    try:
        dt = datetime.strptime(f"{month_str} {day_str} {CURRENT_YEAR}", "%b %d %Y")
    except ValueError:
        return None
    return dt.strftime("%Y-%m-%d")


def table_after_heading(soup: BeautifulSoup, heading_text: str):
    for tag in soup.find_all(["h1", "h2", "h3", "h4", "h5", "strong", "b"]):
        if tag.get_text(strip=True) == heading_text:
            return tag.find_next("table")
    return None


def parse_port_page(html: str, port_name: str, section_name: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    records = []
    for heading, grade in SECTION_TO_GRADE.items():
        table = table_after_heading(soup, heading)
        if not table:
            continue
        rows = table.find_all("tr")
        for row in rows[1:]:  # skip header row
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue
            date_str = parse_date_cell(cells[0].get_text(strip=True))
            if not date_str:
                continue
            spot_price = clean_number(cells[1].get_text(strip=True))
            change_delta = clean_number(cells[2].get_text(strip=True)) if len(cells) > 2 else None
            if spot_price is None or spot_price <= 50:
                continue
            records.append(
                {
                    "date_str": date_str,
                    "section": section_name,
                    "port": port_name,
                    "fuel_grade": grade,
                    "currency": "USD",
                    "spot_price": spot_price,
                    "change_delta": change_delta,
                }
            )
    return records


# ---------------------------------------------------------------------------
# 3. Crawl + upsert
# ---------------------------------------------------------------------------


def fetch_all_data() -> List[Dict]:
    ports = discover_ports()
    print(f"Discovered {len(ports)} candidate ports.")
    all_records: List[Dict] = []
    for i, (name, url, section) in enumerate(ports):
        try:
            resp = requests.get(url, impersonate="chrome120", timeout=20)
            if resp.status_code == 200:
                recs = parse_port_page(resp.text, name, section)
                all_records.extend(recs)
                if recs:
                    print(f"[{i + 1}/{len(ports)}] {name}: {len(recs)} rows")
            else:
                print(f"[{i + 1}/{len(ports)}] {name}: HTTP {resp.status_code}")
        except Exception as e:
            print(f"[{i + 1}/{len(ports)}] {name}: error {e}")
        time.sleep(1.0)
    return all_records


def upsert_to_postgres(records: List[Dict]):
    if not records:
        print("No records to insert.")
        return

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise ValueError("DATABASE_URL environment variable is not set.")

    query = """
    INSERT INTO bunker_prices (crawl_timestamp, section, port, fuel_grade, currency, spot_price, change_delta)
    VALUES (%(crawl_timestamp)s, %(section)s, %(port)s, %(fuel_grade)s, %(currency)s, %(spot_price)s, %(change_delta)s)
    ON CONFLICT ON CONSTRAINT uq_bunker_snapshot
    DO UPDATE SET
        spot_price = EXCLUDED.spot_price,
        change_delta = EXCLUDED.change_delta,
        section = EXCLUDED.section,
        created_at = NOW();
    """

    params = []
    for r in records:
        # Anchored to the row's own real date (noon UTC), not "now" -- so
        # re-crawling the same trailing window on a later day updates the
        # SAME row in place instead of inserting a duplicate for that date.
        # A "now()" timestamp here was the root cause of a stale-vs-corrected
        # duplicate-row bug found 2026-09-12 (see bunker.ts on the FuelSense
        # side for the read-path defense against any remaining duplicates).
        crawl_timestamp = datetime.strptime(r["date_str"], "%Y-%m-%d").replace(
            hour=12, tzinfo=timezone.utc
        )
        params.append({**r, "crawl_timestamp": crawl_timestamp})

    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            execute_batch(cur, query, params, page_size=200)
        conn.commit()
        print(f"Successfully upserted {len(params)} records into PostgreSQL.")
    finally:
        conn.close()


if __name__ == "__main__":
    data = fetch_all_data()
    unique_data = {(d["date_str"], d["port"], d["fuel_grade"]): d for d in data}.values()
    upsert_to_postgres(list(unique_data))
