#!/usr/bin/env python3
"""
Production-ready, optimized fetch_data script for Michigan attractions.

Fixes included:
- Removed stray Markdown fences that caused SyntaxError.
- Resolved SSL certificate failures when loading DNR CSVs by fetching them
  with requests (verify=False) and passing the response into pandas.read_csv.
  (Suppresses InsecureRequestWarning.)
- Keeps optimized logic: compact category mapping, chunked Overpass fetching,
  defensive CSV parsing, and deterministic deduplication.

Security note: verify=False is used only for DNR CSV downloads to work around
broken/missing local CA chains (common on macOS dev setups). If you prefer,
install/update certifi and set REQUESTS_CA_BUNDLE or remove verify=False.
"""

from dataclasses import dataclass
from enum import Enum
import argparse
import hashlib
import io
import json
import os
import time
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from requests import Session

# suppress noisy InsecureRequestWarning when using verify=False
from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# Local helper (must exist in your project)
from michigan_cities import MICHIGAN_CITIES, list_available_cities

# ---- Configuration / small helpers ----

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
DB_FILE = "michigan_attractions_database.json"

@dataclass
class BoundingBox:
    min_lat: float; max_lat: float; min_lon: float; max_lon: float
    def to_overpass(self) -> str:
        return f"{self.min_lat},{self.min_lon},{self.max_lat},{self.max_lon}"

class MichiganRegion(Enum):
    UPPER_PENINSULA = "upper_peninsula"
    LOWER_PENINSULA = "lower_peninsula"
    ENTIRE_STATE = "entire_state"

MICHIGAN_BOUNDING_BOXES = {
    MichiganRegion.UPPER_PENINSULA: BoundingBox(45.0, 47.5, -90.5, -83.5),
    MichiganRegion.LOWER_PENINSULA: BoundingBox(41.7, 45.9, -87.0, -82.4),
    MichiganRegion.ENTIRE_STATE: BoundingBox(41.7, 47.5, -90.5, -82.4),
}

# Compact category→(key,value) mappings. Adjust as needed.
OSM_CATEGORY_MAP = {
    "Lighthouses": [("man_made","lighthouse"), ("tourism","lighthouse"), ("seamark:type","lighthouse")],
    "Parks & Nature": [("leisure","park"), ("natural","wood"), ("landuse","recreation_ground"), ("boundary","protected_area")],
    "Beaches & Lakeshores": [("natural","beach"), ("leisure","beach_resort"), ("tourism","beach")],
    "Waterfalls": [("natural","waterfall"), ("waterway","waterfall")],
    "Museums & Historic Sites": [("tourism","museum"), ("historic","monument")],
    "Public Art & Sculptures": [("tourism","artwork")],
    "Breweries & Wineries": [("craft","brewery"), ("industrial","brewery"), ("amenity","brewery")],
    "Hiking & Biking Trails": [("highway","path"), ("route","hiking"), ("route","bicycle"), ("leisure","track")],
    "Family Fun": [("tourism","theme_park"), ("tourism","zoo"), ("leisure","water_park")],
}

LOOKUP = { (k,v): cat for cat, pairs in OSM_CATEGORY_MAP.items() for k,v in pairs }
CATEGORY_TO_PAIRS = { cat: pairs for cat, pairs in OSM_CATEGORY_MAP.items() }

# ---- Overpass / OSM helpers ----

def make_unique_id(attr: Dict) -> str:
    """Deterministic unique id: trails dedupe by name+type+source; others include coords."""
    base = f"{attr.get('name')}_{attr.get('type','')}_{attr.get('source','')}"
    if attr.get("type") != "Hiking & Biking Trails":
        base += f"_{attr.get('latitude')}_{attr.get('longitude')}"
    return hashlib.md5(base.encode("utf-8")).hexdigest()

def detect_category(tags: Dict[str,str]) -> Optional[str]:
    """Prefer explicit OSM mappings then fall back to broad heuristics."""
    if not tags:
        return None
    # explicit pair check
    for (k,v), cat in LOOKUP.items():
        if tags.get(k) == v:
            return cat
    # heuristics & special cases
    if tags.get("tourism") in ("museum","gallery","attraction"):
        return "Museums & Historic Sites"
    if tags.get("natural") == "beach":
        return "Beaches & Lakeshores"
    if tags.get("natural") == "waterfall" or tags.get("waterway") == "waterfall":
        return "Waterfalls"
    name = (tags.get("name") or "").lower()
    if "zoo" in name or tags.get("tourism") == "zoo":
        return "Family Fun"
    # default fallbacks
    if any(k in tags for k in ("tourism","leisure","natural","historic","man_made","highway","route","waterway")):
        if tags.get("tourism") in ("theme_park","attraction"):
            return "Family Fun"
        return "Parks & Nature"
    return None

def build_overpass_query(bbox: str, category: Optional[str]=None, timeout: int=30) -> str:
    """Build Overpass query. If category provided, target its pairs, else broad scan."""
    if category and category in CATEGORY_TO_PAIRS:
        parts = []
        for k,v in CATEGORY_TO_PAIRS[category]:
            parts.append(f'node["{k}"="{v}"]({bbox});')
            parts.append(f'way["{k}"="{v}"]({bbox});')
            parts.append(f'relation["{k}"="{v}"]({bbox});')
        body = "\n".join(parts)
    else:
        body = f'nwr["tourism"]({bbox});\n nwr["leisure"]({bbox});\n nwr["natural"]({bbox});\n nwr["man_made"]({bbox});'
    return f"[out:json][timeout:{timeout}];(\n{body}\n);\nout center qt;"

def fetch_json_overpass(query: str, session: Session, retries: int=3, timeout: int=120) -> Optional[dict]:
    """Post Overpass query with simple retry and backoff."""
    for attempt in range(1, retries+1):
        try:
            r = session.post(OVERPASS_URL, data=query, timeout=timeout)
            r.raise_for_status()
            if not r.text.strip():
                raise ValueError("Empty response")
            return r.json()
        except Exception as exc:
            wait = 2 ** attempt
            if attempt < retries:
                print(f"Overpass attempt {attempt} failed: {exc}. Retrying in {wait}s...")
                time.sleep(wait)
            else:
                print(f"Overpass failed: {exc}")
    return None

def parse_overpass_elements(data: dict, filter_category: Optional[str]=None) -> List[dict]:
    """Extract attractions from Overpass JSON, applying category detection and optional filtering."""
    out = []
    if not data or "elements" not in data:
        return out
    for el in data["elements"]:
        tags = el.get("tags") or {}
        name = tags.get("name")
        if not name:
            continue
        lat = el.get("lat") or el.get("center", {}).get("lat")
        lon = el.get("lon") or el.get("center", {}).get("lon")
        if lat is None or lon is None:
            continue
        cat = detect_category(tags)
        if not cat:
            continue
        if filter_category and cat != filter_category:
            continue
        out.append({
            "name": name,
            "latitude": float(lat),
            "longitude": float(lon),
            "type": cat,
            "source": "OpenStreetMap",
            "tags": tags,
        })
    return out

# ---- Chunking (3x4 grid across the state) ----

def michigan_chunks() -> List[BoundingBox]:
    bb = MICHIGAN_BOUNDING_BOXES[MichiganRegion.ENTIRE_STATE]
    lat_steps = [bb.min_lat, bb.min_lat + (bb.max_lat - bb.min_lat)/3, bb.min_lat + 2*(bb.max_lat - bb.min_lat)/3, bb.max_lat]
    lon_steps = [bb.min_lon, bb.min_lon + (bb.max_lon - bb.min_lon)/4, bb.min_lon + 2*(bb.max_lon - bb.min_lon)/4, bb.min_lon + 3*(bb.max_lon - bb.min_lon)/4, bb.max_lon]
    chunks: List[BoundingBox] = []
    for i in range(3):
        for j in range(4):
            chunks.append(BoundingBox(lat_steps[i], lat_steps[i+1], lon_steps[j], lon_steps[j+1]))
    return chunks

def fetch_overpass_for_bbox_list(bboxes: List[BoundingBox], category: Optional[str], test_mode: bool=False) -> List[dict]:
    session = requests.Session()
    results: List[dict] = []
    if test_mode:
        bboxes = bboxes[:1]
        print("TEST MODE: only first chunk will be processed")
    for i,b in enumerate(bboxes, 1):
        bbox_str = b.to_overpass()
        query = build_overpass_query(bbox_str, category)
        print(f"Chunk {i}/{len(bboxes)} query bbox={bbox_str}...")
        data = fetch_json_overpass(query, session=session, retries=3)
        if data:
            parsed = parse_overpass_elements(data, filter_category=category)
            print(f"  => {len(parsed)} parsed attractions")
            results.extend(parsed)
        else:
            print(f"  => no data for chunk {i}")
        time.sleep(1)
    return results

# ---- DNR CSV fetch + parsing (robust and tolerant) ----

DNR_SOURCES = {
    "parks": ("https://gis-midnr.opendata.arcgis.com/datasets/midnr::michigan-state-park-boundaries-1.csv", "Parks & Nature"),
    "campgrounds": ("https://gis-midnr.opendata.arcgis.com/datasets/michigan-state-park-campgrounds-1.csv", "Family Fun"),
    "trails": ("https://gis-midnr.opendata.arcgis.com/datasets/dnr-trails.csv", "Hiking & Biking Trails"),
}

def load_csv_tolerant(url: str, timeout: int = 30) -> Optional[pd.DataFrame]:
    """
    Fetch CSV via requests to avoid system urllib cert validation issues, then load to pandas.
    Uses verify=False and suppresses warnings above; if you want strict validation,
    remove verify=False and configure your system certs (recommended for production).
    """
    try:
        r = requests.get(url, timeout=timeout, verify=False)
        r.raise_for_status()
        text = r.text
        return pd.read_csv(io.StringIO(text), on_bad_lines="skip", low_memory=False)
    except Exception as e:
        print(f"Failed to load CSV {url}: {e}")
        return None

def extract_geo_from_row(row: pd.Series, name_cols: List[str], lat_cols: List[str], lon_cols: List[str]) -> Optional[Tuple[str,float,float]]:
    name = next((row[c] for c in name_cols if c in row and pd.notna(row[c])), None)
    lat = next((row[c] for c in lat_cols if c in row and pd.notna(row[c])), None)
    lon = next((row[c] for c in lon_cols if c in row and pd.notna(row[c])), None)
    if name is None or lat is None or lon is None:
        return None
    try:
        return (str(name).strip(), float(lat), float(lon))
    except Exception:
        return None

def parse_dnr_source(df: pd.DataFrame, category: str) -> List[dict]:
    out: List[dict] = []
    if df is None or df.empty:
        return out
    name_cols = [c for c in ("FACILITY","Name","Trail_Name","NAME","name") if c in df.columns]
    lat_cols = [c for c in ("LATITUDE","Latitude","lat","Y") if c in df.columns]
    lon_cols = [c for c in ("LONGITUDE","Longitude","lon","X") if c in df.columns]
    if not (name_cols and lat_cols and lon_cols):
        print("DNR CSV missing obvious name/lat/lon columns; skipping")
        return out
    for _, row in df.iterrows():
        geo = extract_geo_from_row(row, name_cols, lat_cols, lon_cols)
        if not geo:
            continue
        name, lat, lon = geo
        tags: Dict[str, str] = {}
        for k in ("ACRES","Type","Length_Miles","Difficulty","Surface_Type"):
            if k in df.columns:
                tags[k.lower()] = str(row.get(k, "")) if pd.notna(row.get(k, "")) else ""
        out.append({
            "name": name,
            "latitude": lat,
            "longitude": lon,
            "type": category,
            "source": "Michigan DNR",
            "tags": tags
        })
    return out

# ---- Persistence / dedupe ----

def load_existing_ids(dbfile: str=DB_FILE) -> set:
    if not os.path.exists(dbfile):
        return set()
    try:
        with open(dbfile, "r", encoding="utf-8") as fh:
            data = json.load(fh)
            return { item["id"] for item in data if "id" in item }
    except Exception:
        return set()

def save_combined(new_items: List[dict], dbfile: str=DB_FILE):
    existing: List[dict] = []
    if os.path.exists(dbfile):
        try:
            with open(dbfile, "r", encoding="utf-8") as fh:
                existing = json.load(fh)
        except Exception:
            existing = []
    combined = existing + new_items
    with open(dbfile, "w", encoding="utf-8") as fh:
        json.dump(combined, fh, indent=2, ensure_ascii=False)
    return combined

# ---- CLI / main flow ----

def main():
    parser = argparse.ArgumentParser(description="Fetch Michigan attractions (optimized)")
    parser.add_argument("--region", choices=[r.value for r in MichiganRegion], help="Region to fetch")
    parser.add_argument("--location", choices=list(list_available_cities()) + ["all"], help="Specific city or 'all'")
    parser.add_argument("--category", choices=list(OSM_CATEGORY_MAP.keys()), help="Optional category filter")
    parser.add_argument("--no-images", action="store_true", help="Skip image enrichment (kept for compatibility)")
    args = parser.parse_args()

    if not args.region and not args.location:
        print("ERROR: supply --region or --location (or use --location all). Available locations are:")
        print(", ".join(list_available_cities()))
        return

    # determine bbox(s)
    if args.location:
        if args.location == "all":
            bboxes = michigan_chunks()
        else:
            loc = MICHIGAN_CITIES[args.location]
            lat, lon = loc["latitude"], loc["longitude"]
            bboxes = [BoundingBox(lat-0.2, lat+0.2, lon-0.2, lon+0.2)]
    else:
        region = MichiganRegion(args.region)
        bboxes = [MICHIGAN_BOUNDING_BOXES[region]]

    print(f"Fetching category={args.category or 'ANY'} for {len(bboxes)} bbox(es)...")
    existing_ids = load_existing_ids()
    print(f"Loaded {len(existing_ids)} existing attraction IDs")

    all_attractions: List[dict] = []
    # OSM
    test_mode = len(existing_ids) == 0 and len(bboxes) > 1
    if len(bboxes) > 1:
        all_attractions.extend(fetch_overpass_for_bbox_list(bboxes, args.category, test_mode=test_mode))
    else:
        session = requests.Session()
        q = build_overpass_query(bboxes[0].to_overpass(), args.category)
        data = fetch_json_overpass(q, session=session)
        if data:
            all_attractions.extend(parse_overpass_elements(data, filter_category=args.category))

    # DNR sources (use requests with verify=False to avoid local CA issues)
    for key, (url, cat) in DNR_SOURCES.items():
        df = load_csv_tolerant(url)
        if df is not None:
            parsed = parse_dnr_source(df, cat)
            if args.category:
                parsed = [p for p in parsed if p["type"] == args.category]
            print(f"DNR {key}: {len(parsed)} items")
            all_attractions.extend(parsed)

    # Deduplicate & filter out previously seen
    new_items: List[dict] = []
    seen = set()
    for a in all_attractions:
        a_id = make_unique_id(a)
        if a_id in existing_ids or a_id in seen:
            continue
        a["id"] = a_id
        new_items.append(a)
        seen.add(a_id)

    print(f"Found {len(new_items)} new attractions")
    combined = save_combined(new_items)
    print(f"SUCCESS: DB now contains {len(combined)} items (added {len(new_items)})")

if __name__ == "__main__":
    main()
