#!/usr/bin/env python3
"""
Analyze Spanish country mismatches for all feeds marked autoSpanishOverrides.

- Reads:
    automation/feeds.json
    automation/spain.kml          # Spain polygon(s) in KML
    site/<slug>/gtfs.zip          # built/downloaded GTFS per feed, if present
    (fallback) downloads GTFS from feeds.json URL or copies from localPath.

- For each feed where autoSpanishOverrides === true:
    * Ensure a GTFS zip exists (site/<slug>/gtfs.zip or freshly downloaded)
    * Read stops.txt
    * For each stop:
        - Determine if coordinates fall inside the Spain polygon(s)
        - Map stop_timezone -> timezone_country (ES/FR/PT/UNKNOWN)
        - If either:
            - timezone_country == "ES"  XOR  in_spain_polygon is True
          then record a mismatch.

- Writes:
    automation/spanish-country-mismatches.json

JSON shape:
{
  "generatedAt": "...",
  "feeds": {
    "<slug>": [
      {
        "stop_id": "...",
        "stop_name": "...",
        "stop_lat": 43.3511,
        "stop_lon": -1.7833,
        "stop_timezone": "Europe/Paris",
        "timezone_country": "FR",
        "geo_country": "ES",
        "in_spain_polygon": true,
        "mismatch_type": "NON_ES_TZ_INSIDE_SP",
        "reason": "Lat/Lon in Spain but timezone maps to non-ES country."
      },
      ...
    ]
  }
}
"""

from __future__ import annotations

import csv
import io
import json
import shutil
import urllib.request
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Tuple, Any

# ---- project paths ---------------------------------------------------------

HERE = Path(__file__).resolve().parent               # gtfs-mapper/automation
ROOT = HERE.parent                                   # gtfs-mapper/
FEEDS_JSON = ROOT / "automation" / "feeds.json"
SPAIN_KML = ROOT / "automation" / "spain.kml"
SITE_DIR = ROOT / "site"
OUT_JSON = HERE / "spanish-country-mismatches.json"  # automation/spanish-country-mismatches.json

# ---- timezone → country mapping --------------------------------------------

TZ_TO_COUNTRY = {
    "Europe/Madrid": "ES",
    "Atlantic/Canary": "ES",
    "Europe/Paris": "FR",
    "Europe/Lisbon": "PT",
    # extend if needed (e.g. "Europe/London": "GB", ...)
}


def tz_to_country(tz: str | None) -> str:
    if not tz:
        return "UNKNOWN"
    return TZ_TO_COUNTRY.get(tz.strip(), "UNKNOWN")


# ---- point-in-polygon (KML) -----------------------------------------------

def load_spain_polygons(kml_path: Path) -> List[List[Tuple[float, float]]]:
    """
    Parse Spain polygons from a KML file.

    Returns a list of polygons; each polygon is a list of (lon, lat) pairs.
    """
    if not kml_path.exists():
        raise SystemExit(f"[error] Spain KML not found at {kml_path}")

    tree = ET.parse(kml_path)
    root = tree.getroot()

    # KML namespace
    ns = {"kml": "http://www.opengis.net/kml/2.2"}

    polys: List[List[Tuple[float, float]]] = []

    # Grab all <coordinates> inside <Polygon> elements
    for coords_el in root.findall(".//kml:Polygon//kml:coordinates", ns):
        text = coords_el.text or ""
        parts = text.strip().split()
        pts: List[Tuple[float, float]] = []
        for p in parts:
            # lon,lat,alt
            bits = p.split(",")
            if len(bits) < 2:
                continue
            try:
                lon = float(bits[0])
                lat = float(bits[1])
            except ValueError:
                continue
            pts.append((lon, lat))
        if len(pts) >= 3:
            polys.append(pts)

    if not polys:
        raise SystemExit("[error] No polygons found inside spain.kml")

    return polys


def point_in_poly(lon: float, lat: float, poly: List[Tuple[float, float]]) -> bool:
    """
    Standard ray-casting point-in-polygon (lon,lat), treating polygon as closed.
    """
    inside = False
    n = len(poly)
    if n < 3:
        return False

    x = lon
    y = lat

    x0, y0 = poly[0]
    for i in range(1, n + 1):
        x1, y1 = poly[i % n]

        # Edge crosses horizontal ray?
        if ((y0 > y) != (y1 > y)) and (x <= max(x0, x1)):
            # Avoid division by zero for vertical segments
            denom = (y1 - y0) if (y1 != y0) else 1e-12
            x_intersect = x0 + (y - y0) * (x1 - x0) / denom
            if x_intersect >= x:
                inside = not inside

        x0, y0 = x1, y1

    return inside


def point_in_spain(lon: float, lat: float, polygons: List[List[Tuple[float, float]]]) -> bool:
    for poly in polygons:
        if point_in_poly(lon, lat, poly):
            return True
    return False


# ---- feeds + GTFS helpers --------------------------------------------------

def load_feeds_with_auto_flag() -> List[Dict[str, Any]]:
    """
    Parse automation/feeds.json and return normalized records with:
        {
          "slug": str,
          "autoSpanishOverrides": bool,
          "url": str,
          "localPath": str
        }
    """
    if not FEEDS_JSON.exists():
        raise SystemExit(f"[error] feeds.json not found at {FEEDS_JSON}")

    raw = json.loads(FEEDS_JSON.read_text(encoding="utf-8"))

    if isinstance(raw, list):
        feeds_raw = raw
    elif isinstance(raw, dict) and isinstance(raw.get("feeds"), list):
        feeds_raw = raw["feeds"]
    else:
        feeds_raw = []

    feeds: List[Dict[str, Any]] = []
    for x in feeds_raw:
        if isinstance(x, str):
            feeds.append(
                {
                    "slug": x,
                    "autoSpanishOverrides": False,
                    "url": "",
                    "localPath": "",
                }
            )
            continue
        if not isinstance(x, dict):
            continue
        slug = (x.get("slug") or "").strip()
        if not slug:
            continue
        feeds.append(
            {
                "slug": slug,
                "autoSpanishOverrides": bool(x.get("autoSpanishOverrides")),
                "url": (x.get("url") or "").strip(),
                "localPath": (x.get("localPath") or "").strip(),
            }
        )

    return feeds


def iter_stops_from_gtfs_zip(zip_path: Path):
    """
    Yield rows (dicts) from stops.txt inside the given GTFS zip.
    """
    if not zip_path.exists():
        return

    with zipfile.ZipFile(zip_path, "r") as z:
        try:
            with z.open("stops.txt") as f:
                text = io.TextIOWrapper(f, encoding="utf-8-sig", newline="")
                reader = csv.DictReader(text)
                for row in reader:
                    yield row
        except KeyError:
            # no stops.txt
            return


def ensure_gtfs_zip_for_feed(feed: Dict[str, Any]) -> Path | None:
    """
    Ensure we have site/<slug>/gtfs.zip for this feed.

    Order:
      1) If site/<slug>/gtfs.zip exists, use it (CI case).
      2) Else, if feed.url is http(s), download to site/<slug>/gtfs.zip.
      3) Else, if feed.localPath is set and file exists, copy it there.
      4) Else, return None.
    """
    slug = feed["slug"]
    zip_path = SITE_DIR / slug / "gtfs.zip"

    # 1) Already present (CI / local build)
    if zip_path.exists():
        return zip_path

    url = (feed.get("url") or "").strip()
    local_path = (feed.get("localPath") or "").strip()

    zip_path.parent.mkdir(parents=True, exist_ok=True)

    # 2) Try remote URL
    if url.startswith("http://") or url.startswith("https://"):
        print(f"[info] Downloading GTFS for '{slug}' from {url} ...")
        try:
            with urllib.request.urlopen(url) as resp, open(zip_path, "wb") as out:
                shutil.copyfileobj(resp, out)
            print(f"[info] Saved GTFS to {zip_path}")
            return zip_path
        except Exception as e:
            print(f"[warn] Failed to download GTFS for '{slug}' from {url}: {e}")

    # 3) Try localPath (relative to automation/ or repo root)
    if local_path:
        rel_candidate = HERE / local_path
        if not rel_candidate.exists():
            rel_candidate = ROOT / local_path

        if rel_candidate.exists():
            print(f"[info] Copying GTFS for '{slug}' from {rel_candidate} ...")
            try:
                shutil.copyfile(rel_candidate, zip_path)
                print(f"[info] Copied GTFS to {zip_path}")
                return zip_path
            except Exception as e:
                print(f"[warn] Failed to copy GTFS for '{slug}' from {rel_candidate}: {e}")
        else:
            print(f"[warn] localPath for '{slug}' does not exist: {rel_candidate}")

    # 4) Give up
    print(f"[warn] No GTFS source available for '{slug}' (no site zip, no valid url/localPath).")
    return None


# ---- main analysis ---------------------------------------------------------

def main() -> None:
    print("[info] Loading Spain polygons ...")
    polygons = load_spain_polygons(SPAIN_KML)
    print(f"[info] Loaded {len(polygons)} polygon(s) from {SPAIN_KML}")

    feeds = load_feeds_with_auto_flag()
    auto_feeds = [f for f in feeds if f["autoSpanishOverrides"]]
    print(f"[info] Feeds with autoSpanishOverrides=true: {[f['slug'] for f in auto_feeds]}")

    mismatches_by_slug: Dict[str, List[Dict[str, Any]]] = {}
    total_mismatches = 0

    for feed in auto_feeds:
        slug = feed["slug"]

        # Ensure we have a GTFS zip (either from site/ or freshly fetched)
        zip_path = ensure_gtfs_zip_for_feed(feed)
        if not zip_path or not zip_path.exists():
            print(f"[warn] GTFS zip not available for '{slug}'; skipping")
            continue

        print(f"[info] Analyzing Spanish country mismatches for feed '{slug}' using {zip_path} ...")

        mismatches: List[Dict[str, Any]] = []
        checked = 0

        for row in iter_stops_from_gtfs_zip(zip_path):
            stop_id = (row.get("stop_id") or "").strip()
            stop_name = (row.get("stop_name") or "").strip()

            try:
                lat = float(row.get("stop_lat") or "")
                lon = float(row.get("stop_lon") or "")
            except (TypeError, ValueError):
                continue

            tz_raw = (row.get("stop_timezone") or "").strip()
            tz_country = tz_to_country(tz_raw)

            in_spain = point_in_spain(lon, lat, polygons)
            checked += 1

            # We only care about Spanish context:
            #  - stops that are inside Spain polygon OR have Spanish timezone.
            is_spanish_candidate = in_spain or (tz_country == "ES")
            if not is_spanish_candidate:
                continue

            mismatch_type = None
            if tz_country == "ES" and not in_spain:
                mismatch_type = "TZ_ES_OUTSIDE_SP"
            elif in_spain and tz_country not in ("ES", "UNKNOWN"):
                mismatch_type = "NON_ES_TZ_INSIDE_SP"
            elif in_spain and tz_country == "UNKNOWN":
                mismatch_type = "UNKNOWN_TZ_INSIDE_SP"

            if mismatch_type:
                # Human-readable reason for the Admin UI
                if mismatch_type == "TZ_ES_OUTSIDE_SP":
                    reason = "Timezone country is ES but geometry is outside Spain polygon."
                elif mismatch_type == "NON_ES_TZ_INSIDE_SP":
                    reason = "Lat/Lon in Spain but timezone maps to non-ES country."
                elif mismatch_type == "UNKNOWN_TZ_INSIDE_SP":
                    reason = "Lat/Lon in Spain but timezone is missing or unmapped."
                else:
                    reason = ""

                mismatches.append(
                    {
                        "stop_id": stop_id,
                        "stop_name": stop_name,
                        # Admin expects these exact keys:
                        "stop_lat": lat,
                        "stop_lon": lon,
                        "stop_timezone": tz_raw or "",
                        "timezone_country": tz_country,
                        # For display / semantics:
                        "geo_country": "ES" if in_spain else "",
                        # Extra debug fields:
                        "in_spain_polygon": in_spain,
                        "mismatch_type": mismatch_type,
                        "reason": reason,
                    }
                )

        if mismatches:
            mismatches_by_slug[slug] = mismatches
            total_mismatches += len(mismatches)
            print(f"  -> {len(mismatches)} mismatches for {slug} (checked {checked} stops)")
        else:
            print(f"  -> no mismatches for {slug} (checked {checked} stops)")

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    out_obj = {
        "generatedAt": __import__("datetime").datetime.utcnow().isoformat() + "Z",
        "feeds": mismatches_by_slug,
    }
    OUT_JSON.write_text(json.dumps(out_obj, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"[ok] Wrote Spanish country mismatch report to {OUT_JSON}")
    print(f"[ok] Total mismatches: {total_mismatches}")


if __name__ == "__main__":
    main()