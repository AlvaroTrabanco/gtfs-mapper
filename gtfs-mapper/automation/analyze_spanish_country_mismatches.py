#!/usr/bin/env python3
"""
Multi-country border analyser for GTFS stops.

- Reads automation/feeds.json
- For each feed with autoBorderOverrides.enabled:
  - Reads site/<slug>/gtfs.zip
  - Looks at stops.txt (stop_lat, stop_lon, stop_timezone/stop_tz)
  - Uses autoBorderOverrides.timezonesByCountry + a base map
    to infer timezone-country.
  - Loads KML for each configured border country from automation/<CC>.kml
    (e.g. automation/ES.kml, automation/CH.kml)
  - If a stop is inside the country polygon(s):

    * If timezone is missing:
        -> add entry to "needsManualReview"
           (slug, stop_id, lat, lon, country, reason="inside_kml_no_timezone")

    * If timezone-country != polygon-country:
        -> add decisions["<slug>::<stop_id>"] = {
               "newCountry": polygonCountry,
               "oldCountry": tzCountry,
               "timezone": tz,
               "reason": "kml_tz_mismatch"
           }

Outputs:
  automation/spanish-country-decisions.json

This file is consumed by rebuild-gtfs.js as a generic "border decisions" JSON.
"""

import csv
import json
import os
import zipfile
from pathlib import Path
from xml.etree import ElementTree as ET

ROOT = Path(__file__).resolve().parent.parent  # gtfs-mapper/
AUTOMATION = ROOT / "automation"

FEEDS_JSON = AUTOMATION / "feeds.json"
DECISIONS_JSON = AUTOMATION / "spanish-country-decisions.json"  # path already used by JS

# Simple base timezone -> country mapping (fallback)
BASE_TZ_TO_COUNTRY = {
    "Europe/Madrid": "ES",
    "Atlantic/Canary": "ES",
    "Europe/Paris": "FR",
    "Europe/Lisbon": "PT",
    "Europe/Zurich": "CH",
    "Europe/Berlin": "DE",
    "Europe/Rome": "IT",
}


def load_feeds():
    with FEEDS_JSON.open("r", encoding="utf-8") as f:
        raw = json.load(f)
    return raw if isinstance(raw, list) else raw.get("feeds", [])


def build_tz_map_for_feed(feed):
    """
    Build timezone->country map for this feed from autoBorderOverrides.timezonesByCountry
    plus the global base map.
    """
    tz_map = dict(BASE_TZ_TO_COUNTRY)
    abo = feed.get("autoBorderOverrides") or {}
    by_country = abo.get("timezonesByCountry") or {}
    for country, tz_list in by_country.items():
        if not isinstance(tz_list, list):
            continue
        cc = str(country or "").strip().upper()
        for tz in tz_list:
            if not tz:
                continue
            tz_map[str(tz).strip()] = cc
    return tz_map


def load_kml_polygons_for_country(country_code):
    """
    Load polygons from automation/<CC>.kml (CC = country code, e.g. ES, CH).
    Returns a list of polygons, each polygon = list[(lon, lat), ...].

    We deliberately ignore XML namespaces and just look for any element
    whose tag ends with 'coordinates'.
    """
    cc = country_code.upper()
    kml_path = AUTOMATION / f"{cc}.kml"
    if not kml_path.exists():
        print(f"[KML] No KML file for country {cc}: {kml_path} (skipping)")
        return []

    print(f"[KML] Loading KML for {cc}: {kml_path}")
    tree = ET.parse(str(kml_path))
    root = tree.getroot()

    polygons = []

    # Iterate through all elements; pick those whose tag ends with 'coordinates'
    for coords_node in root.iter():
        tag = coords_node.tag
        # Handles tags like '{http://www.opengis.net/kml/2.2}coordinates'
        if not str(tag).lower().endswith("coordinates"):
            continue

        text = coords_node.text or ""
        coords = []
        for token in text.strip().split():
            # KML format: lon,lat[,alt]
            parts = token.split(",")
            if len(parts) < 2:
                continue
            try:
                lon = float(parts[0])
                lat = float(parts[1])
            except ValueError:
                continue
            coords.append((lon, lat))

        if len(coords) >= 3:
            polygons.append(coords)

    print(f"[KML] Loaded {len(polygons)} polygon(s) for {cc}")
    return polygons


def point_in_poly(lon, lat, poly):
    """
    Standard ray-casting point-in-polygon for (lon,lat).
    poly = list of (lon,lat).
    """
    inside = False
    n = len(poly)
    if n < 3:
        return False

    x, y = lon, lat
    for i in range(n):
        x1, y1 = poly[i]
        x2, y2 = poly[(i + 1) % n]
        # Check if edge crosses the horizontal ray
        if ((y1 > y) != (y2 > y)):
            # Compute x of intersection of edge with the ray at y
            try:
                x_intersect = x1 + (y - y1) * (x2 - x1) / (y2 - y1)
            except ZeroDivisionError:
                x_intersect = x1
            if x_intersect > x:
                inside = not inside
    return inside


def point_in_any(lon, lat, polygons):
    """Return True if point lies in any of the polygons."""
    for poly in polygons:
        if point_in_poly(lon, lat, poly):
            return True
    return False


def process_feed(feed, decisions, needs_review):
    slug = feed.get("slug")
    if not slug:
        return

    abo = feed.get("autoBorderOverrides") or {}
    if not abo.get("enabled"):
        return

    countries = abo.get("countries") or []
    countries = [str(c or "").strip().upper() for c in countries if c]

    if not countries:
        # nothing to do
        return

    tz_map = build_tz_map_for_feed(feed)

    # KML polygons per country
    country_polys = {
        cc: load_kml_polygons_for_country(cc)
        for cc in countries
    }

    # If all are empty, nothing to do
    if not any(country_polys.values()):
        return

    # Read original GTFS for this feed from site/<slug>/gtfs.zip
    gtfs_zip = ROOT / "site" / slug / "gtfs.zip"
    if not gtfs_zip.exists():
        print(f"[GTFS] No gtfs.zip for feed {slug} at {gtfs_zip} (skipping)")
        return

    print(f"[GTFS] Analysing stops for feed {slug} from {gtfs_zip}")

    with zipfile.ZipFile(str(gtfs_zip), "r") as zf:
        if "stops.txt" not in zf.namelist():
            print(f"[GTFS] stops.txt missing in {gtfs_zip} (skipping)")
            return

        with zf.open("stops.txt", "r") as f:
            reader = csv.DictReader(
                (line.decode("utf-8-sig") for line in f),
                delimiter=","
            )

            for row in reader:
                stop_id = (row.get("stop_id") or "").strip()
                if not stop_id:
                    continue

                lat_raw = row.get("stop_lat")
                lon_raw = row.get("stop_lon")
                try:
                    lat = float(lat_raw) if lat_raw not in (None, "") else None
                    lon = float(lon_raw) if lon_raw not in (None, "") else None
                except ValueError:
                    lat = lon = None

                tz = (row.get("stop_timezone") or row.get("stop_tz") or "").strip()

                if lat is None or lon is None:
                    continue  # cannot do KML checks

                # For each border country, check if the stop lies in its polygon(s)
                for cc, polys in country_polys.items():
                    if not polys:
                        continue
                    inside = point_in_any(lon, lat, polys)
                    if not inside:
                        continue

                    # The stop lies inside cc's geometry
                    if not tz:
                        # 1) NO TIMEZONE but inside KML -> manual review
                        needs_review.append({
                            "slug": slug,
                            "stop_id": stop_id,
                            "lat": lat,
                            "lon": lon,
                            "country": cc,
                            "reason": "inside_kml_no_timezone",
                        })
                        continue

                    tz_country = tz_map.get(tz, "UNKNOWN")

                    if tz_country == cc:
                        # timezone-country matches polygon country -> OK
                        continue

                    # 2) Timezone-country != polygon-country -> add decision
                    key = f"{slug}::{stop_id}"
                    decisions[key] = {
                        "newCountry": cc,
                        "oldCountry": tz_country,
                        "timezone": tz,
                        "lat": lat,
                        "lon": lon,
                        "reason": "kml_tz_mismatch",
                    }


def main():
    if not FEEDS_JSON.exists():
        print(f"ERROR: {FEEDS_JSON} not found")
        return

    feeds = load_feeds()

    # decisions: { "<slug>::<stop_id>": { newCountry, ... } }
    decisions = {}
    needs_review = []

    for feed in feeds:
        if isinstance(feed, str):
            # not an object; ignore
            continue
        process_feed(feed, decisions, needs_review)

    out = {
        "decisions": decisions,
        "needsManualReview": needs_review,
    }

    DECISIONS_JSON.parent.mkdir(parents=True, exist_ok=True)
    with DECISIONS_JSON.open("w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, sort_keys=True)
    print(f"Wrote decisions to {DECISIONS_JSON}")
    print(f"  total decisions: {len(decisions)}")
    print(f"  needsManualReview: {len(needs_review)}")


if __name__ == "__main__":
    main()