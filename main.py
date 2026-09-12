import os
import re
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional
import psycopg2
from psycopg2.extras import execute_batch
from curl_cffi import requests
from bs4 import BeautifulSoup

def clean_number(text: str) -> Optional[float]:
    if not text:
        return None
    cleaned = text.replace(",", "").strip()
    match = re.search(r"([+-]?\d+(?:\.\d+)?)", cleaned)
    return float(match.group(1)) if match else None

def detect_fuel_grade(header_text: str) -> Optional[str]:
    h = header_text.upper()
    if "VLSFO" in h or "0.5%" in h:
        return "VLSFO"
    if "LSMGO" in h or "MGO" in h or "0.1%" in h:
        return "LSMGO"
    if "380" in h or "HSFO" in h or "IFO" in h:
        return "IFO380"
    if "MDO" in h:
        return "MDO"
    if "LNG" in h:
        return "LNG"
    return None

EXCLUDED_PORT_TERMS = {"eua", "eu ets", "brent", "wti", "crude", "global", "market", "index", "average", "port", "region"}

def parse_bunker_tables(html_content: str, section_name: str, crawl_timestamp: datetime) -> List[Dict]:
    soup = BeautifulSoup(html_content, "html.parser")
    records = []

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue

        header_cells = rows[0].find_all(["th", "td"])
        expanded_headers = []
        for cell in header_cells:
            colspan = int(cell.get("colspan", 1))
            expanded_headers.extend([cell.get_text(strip=True)] * colspan)

        header_text_joined = " ".join(expanded_headers).upper()
        if "HIGH" in header_text_joined and "LOW" in header_text_joined and "PRICE $/MT" in header_text_joined:
            continue

        grade_map = {}
        for idx, h in enumerate(expanded_headers):
            g = detect_fuel_grade(h)
            if g:
                grade_map[idx] = g

        if not grade_map:
            continue

        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue

            port_cell = cells[0]
            port_link = port_cell.find("a")
            port_name = port_link.get_text(strip=True) if port_link else port_cell.get_text(strip=True)

            if not port_name or port_name.lower() in EXCLUDED_PORT_TERMS or bool(re.match(r"^\$?[\d\.\,\+\-]+$", port_name)):
                continue

            for col_idx, grade in grade_map.items():
                if col_idx >= len(cells):
                    continue

                cell_elem = cells[col_idx]
                cell_text = cell_elem.get_text(" ", strip=True)

                if any(kw in cell_text.lower() for kw in ["subscribe", "n/a", "--", "null"]):
                    continue

                spot_price, change_delta = None, None
                if col_idx + 1 < len(cells) and (col_idx + 1) not in grade_map:
                    price_text = cell_elem.get_text(strip=True)
                    delta_text = cells[col_idx + 1].get_text(strip=True)
                    spot_price = clean_number(price_text)
                    change_delta = clean_number(delta_text)
                else:
                    nums = re.findall(r"([+-]?\d+(?:\.\d+)?)", cell_text.replace(",", ""))
                    if len(nums) == 1:
                        spot_price = float(nums[0])
                    elif len(nums) >= 2:
                        spot_price = float(nums[0])
                        change_delta = float(nums[1])

                if spot_price is not None and spot_price > 50:
                    records.append({
                        "crawl_timestamp": crawl_timestamp,
                        "section": section_name,
                        "port": port_name,
                        "fuel_grade": grade,
                        "currency": "USD",
                        "spot_price": spot_price,
                        "change_delta": change_delta
                    })
    return records

BASE_URL = "https://shipandbunker.com"

def fetch_all_data() -> List[Dict]:
    crawl_timestamp = datetime.now(timezone.utc)
    endpoints = {
        "Global Benchmark": f"{BASE_URL}/prices",
        "Americas": f"{BASE_URL}/prices/am",
        "Asia-Pacific": f"{BASE_URL}/prices/apac",
        "EMEA": f"{BASE_URL}/prices/emea",
    }
    all_records = []
    for name, url in endpoints.items():
        try:
            resp = requests.get(url, impersonate="chrome120", timeout=20)
            if resp.status_code == 200:
                all_records.extend(parse_bunker_tables(resp.text, name, crawl_timestamp))
        except Exception as e:
            print(f"Error fetching {name}: {e}")
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

    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            execute_batch(cur, query, records, page_size=100)
        conn.commit()
        print(f"Successfully upserted {len(records)} records into PostgreSQL.")
    finally:
        conn.close()

if __name__ == "__main__":
    data = fetch_all_data()
    unique_data = { (d["crawl_timestamp"], d["port"], d["fuel_grade"]): d for d in data }.values()
    upsert_to_postgres(list(unique_data))
