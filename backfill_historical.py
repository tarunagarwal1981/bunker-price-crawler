"""
One-off backfill: pulls each port's real ~2-week "Latest Prices" history from
its Ship & Bunker detail page (the only historical data the free tier exposes
without a subscription) and upserts it into bunker_prices.

Not part of the daily cron — run manually when a backfill is needed.
"""
import os
import re
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import execute_batch
from curl_cffi import requests
from bs4 import BeautifulSoup

from main import clean_number, EXCLUDED_PORT_TERMS

BASE_URL = "https://shipandbunker.com"

LISTING_ENDPOINTS = {
    "Global Benchmark": f"{BASE_URL}/prices",
    "Americas": f"{BASE_URL}/prices/am",
    "Asia-Pacific": f"{BASE_URL}/prices/apac",
    "EMEA": f"{BASE_URL}/prices/emea",
}

# Detail-page section headings we care about, mapped to our fuel_grade enum.
# "MGO" is skipped deliberately: on every port checked it duplicates "LSMGO"
# exactly (same site, same series under two labels) -- taking both would
# double-insert identical rows.
SECTION_TO_GRADE = {
    "Latest Prices, VLSFO": "VLSFO",
    "Latest Prices, LSMGO": "LSMGO",
    "Latest Prices, IFO380 (HSFO)": "IFO380",
}

CURRENT_YEAR = datetime.now(timezone.utc).year


def discover_ports() -> List[Tuple[str, str, str]]:
    """Returns [(port_name, detail_url, section_name)], deduped by detail_url."""
    seen: Dict[str, Tuple[str, str, str]] = {}
    for section_name, url in LISTING_ENDPOINTS.items():
        resp = requests.get(url, impersonate="chrome120", timeout=20)
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
            # Detail-page hrefs have 3+ path segments after /prices/, e.g.
            # /prices/emea/me/ae-fjr-fujairah
            path = href.split("shipandbunker.com")[-1]
            if path.count("/") < 4:
                continue
            name = a.get_text(strip=True)
            # Anchor text on listing pages sometimes has the price glued on
            # (colspan artifact) -- keep just the leading letters/spaces/punct.
            m = re.match(r"^[A-Za-z][A-Za-z\s/().'-]*", name)
            name = m.group(0).strip() if m else name
            if (
                not name
                or name.lower() in EXCLUDED_PORT_TERMS
                or "average" in name.lower()
                or path.startswith("/prices/av/")
                or path.startswith("/prices/ea/")  # EUA etc.
            ):
                continue
            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
            full_url = full_url.split("#")[0]
            name = re.sub(r"\s*Bunker Prices\s*$", "", name, flags=re.IGNORECASE).strip()
            if not name:
                continue
            if full_url not in seen:
                seen[full_url] = (name, full_url, section_name)
        time.sleep(1.0)
    return list(seen.values())


def parse_date(date_cell: str) -> Optional[str]:
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
            sib = tag.find_next("table")
            return sib
    return None


def parse_port_history(html: str, port_name: str, section_name: str) -> List[Dict]:
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
            date_text = cells[0].get_text(strip=True)
            date_str = parse_date(date_text)
            if not date_str:
                continue
            spot_price = clean_number(cells[1].get_text(strip=True))
            change_delta = (
                clean_number(cells[2].get_text(strip=True)) if len(cells) > 2 else None
            )
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


def upsert_backfill(records: List[Dict]):
    if not records:
        print("No backfill records to insert.")
        return
    db_url = os.environ["DATABASE_URL"]
    query = """
    INSERT INTO bunker_prices (crawl_timestamp, section, port, fuel_grade, currency, spot_price, change_delta)
    VALUES (%(crawl_timestamp)s, %(section)s, %(port)s, %(fuel_grade)s, %(currency)s, %(spot_price)s, %(change_delta)s)
    ON CONFLICT ON CONSTRAINT uq_bunker_snapshot
    DO NOTHING;
    """
    params = []
    for r in records:
        crawl_timestamp = datetime.strptime(r["date_str"], "%Y-%m-%d").replace(
            hour=12, tzinfo=timezone.utc
        )
        params.append({**r, "crawl_timestamp": crawl_timestamp})

    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            execute_batch(cur, query, params, page_size=200)
        conn.commit()
        print(f"Upserted {len(params)} historical records.")
    finally:
        conn.close()


if __name__ == "__main__":
    ports = discover_ports()
    print(f"Discovered {len(ports)} ports.")
    all_records: List[Dict] = []
    for i, (name, url, section) in enumerate(ports):
        try:
            resp = requests.get(url, impersonate="chrome120", timeout=20)
            if resp.status_code == 200:
                recs = parse_port_history(resp.text, name, section)
                all_records.extend(recs)
                print(f"[{i+1}/{len(ports)}] {name}: {len(recs)} rows")
            else:
                print(f"[{i+1}/{len(ports)}] {name}: HTTP {resp.status_code}")
        except Exception as e:
            print(f"[{i+1}/{len(ports)}] {name}: error {e}")
        time.sleep(1.0)

    unique = {
        (r["date_str"], r["port"], r["fuel_grade"]): r for r in all_records
    }.values()
    upsert_backfill(list(unique))
