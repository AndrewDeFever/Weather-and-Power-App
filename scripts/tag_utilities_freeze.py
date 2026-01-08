#!/usr/bin/env python3
"""
Freeze-Mode Utility Tagger (Deterministic) — Multi-format (GeoJSON + Evergy packed geom.a)

Supports:
- GeoJSON FeatureCollection/list features (OGE/PSO/Oncor typical)
- Evergy packed format:
    { "file_title": null, "file_data": [ { ..., "geom": { "a": [<encoded strings>], "p": [...] } } ] }

Also supports:
- HTTP compression (gzip/deflate/br)
- Local file inputs via serviceareas_url: "file:./path.json"
- Cache (optional) for HTTP sources; file sources are read directly.

Run:
  python scripts/tag_utilities_freeze.py --sites app/data/sites.json --catalog app/data/utilities_catalog.json --report-out app/data/utility_tag_report.json --freeze-provenance --refresh
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import time
import urllib.request
import zlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

# Optional brotli support (install: pip install brotli)
try:
    import brotli  # type: ignore
except Exception:
    brotli = None  # type: ignore


# ----------------------------
# Geometry (pure python)
# ----------------------------

def point_in_ring(lon: float, lat: float, ring: List[Tuple[float, float]]) -> bool:
    if not ring:
        return False
    if ring[0] != ring[-1]:
        ring = ring + [ring[0]]

    inside = False
    x, y = lon, lat
    for i in range(len(ring) - 1):
        x1, y1 = ring[i]
        x2, y2 = ring[i + 1]
        if (y1 > y) != (y2 > y):
            denom = (y2 - y1) or 1e-30
            x_intersect = (x2 - x1) * (y - y1) / denom + x1
            if x < x_intersect:
                inside = not inside
    return inside


def point_in_polygon(lon: float, lat: float, polygon: List[List[Tuple[float, float]]]) -> bool:
    # polygon: [outer_ring, hole1, hole2, ...]
    if not polygon or not polygon[0]:
        return False
    if not point_in_ring(lon, lat, polygon[0]):
        return False
    for hole in polygon[1:]:
        if hole and point_in_ring(lon, lat, hole):
            return False
    return True


def point_in_multipolygon(lon: float, lat: float, multipoly: List[List[List[Tuple[float, float]]]]) -> bool:
    # multipoly: [ polygon, polygon, ... ], polygon: [ring, hole, ...]
    for poly in multipoly:
        if point_in_polygon(lon, lat, poly):
            return True
    return False


# ----------------------------
# Data structures
# ----------------------------

@dataclass(frozen=True)
class AreaGeom:
    utility_key: str
    area_id: str
    name: str
    geom_type: str  # "Polygon" or "MultiPolygon"
    polygon: Optional[List[List[Tuple[float, float]]]] = None
    multipolygon: Optional[List[List[List[Tuple[float, float]]]]] = None


def contains(area: AreaGeom, lon: float, lat: float) -> bool:
    if area.geom_type == "Polygon" and area.polygon is not None:
        return point_in_polygon(lon, lat, area.polygon)
    if area.geom_type == "MultiPolygon" and area.multipolygon is not None:
        return point_in_multipolygon(lon, lat, area.multipolygon)
    return False


# ----------------------------
# I/O helpers
# ----------------------------

def load_json_file(path: str) -> Any:
    # utf-8-sig transparently strips a UTF-8 BOM if present
    with open(path, "r", encoding="utf-8-sig") as f:
        return json.load(f)


def write_json_file(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, sort_keys=True, ensure_ascii=False)
        f.write("\n")


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def now_iso_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# ----------------------------
# HTTP + decoding helpers
# ----------------------------

def _decompress_if_needed(raw: bytes, content_encoding: str) -> Tuple[bytes, str]:
    enc = (content_encoding or "").strip().lower()
    if not enc:
        return raw, "no-encoding"

    encodings = [e.strip() for e in enc.split(",") if e.strip()]
    data = raw
    notes: List[str] = []

    for e in reversed(encodings):
        if e == "gzip":
            data = gzip.decompress(data)
            notes.append("gunzip")
        elif e == "deflate":
            try:
                data = zlib.decompress(data)
                notes.append("inflate(zlib)")
            except zlib.error:
                data = zlib.decompress(data, -zlib.MAX_WBITS)
                notes.append("inflate(raw)")
        elif e == "br":
            if brotli is None:
                raise RuntimeError(
                    "Response is brotli-compressed (Content-Encoding: br) but 'brotli' is not installed. "
                    "Run: pip install brotli"
                )
            data = brotli.decompress(data)
            notes.append("brotli")
        elif e in ("identity",):
            notes.append("identity")
        else:
            raise RuntimeError(f"Unsupported Content-Encoding: {content_encoding!r} (token {e!r})")

    return data, "+".join(notes) if notes else "encoding-unknown"


def decode_and_parse_json(raw: bytes, *, cache_key: str, url: str, content_type: str, content_encoding: str) -> Any:
    decoded_bytes, dec_note = _decompress_if_needed(raw, content_encoding)
    text = decoded_bytes.decode("utf-8", errors="replace").lstrip("\ufeff").strip()
    if not text:
        raise RuntimeError(
            f"Empty body after decode for {cache_key} url={url!r} "
            f"content_type={content_type!r} content_encoding={content_encoding!r} decode={dec_note!r}"
        )
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        snippet = text[:500].replace("\r", "").replace("\n", " ")
        raise RuntimeError(
            f"Non-JSON body after decode for {cache_key} url={url!r} "
            f"content_type={content_type!r} content_encoding={content_encoding!r} decode={dec_note!r}. "
            f"First 500 chars: {snippet!r}"
        ) from e


def http_get_bytes(url: str, *, timeout: int = 30) -> Tuple[bytes, Dict[str, str], int]:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "NOC-UtilityTaggerFreeze/1.3",
            "Accept": "application/json,text/plain,*/*",
            "Accept-Encoding": "gzip, deflate, br",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            headers = {k.lower(): v for k, v in resp.headers.items()}
            status = getattr(resp, "status", 200)
            return raw, headers, status
    except Exception as e:
        raise RuntimeError(f"HTTP GET failed url={url!r}: {e}") from e


def _normalize_file_url(file_url: str) -> str:
    # Accept: file:./x.json, file:.\x.json, file:/abs/path, file:C:\abs\path
    path = file_url[len("file:"):]
    path = path.strip()
    # If it's like file://..., strip leading slashes carefully
    if path.startswith("//"):
        path = path.lstrip("/")
    # Resolve relative paths from current working directory (repo root)
    if not os.path.isabs(path):
        path = os.path.abspath(path)
    return path


def fetch_json_any(
    url: str,
    cache_dir: str,
    cache_key: str,
    max_age_seconds: int,
    *,
    no_cache: bool,
    refresh: bool,
) -> Tuple[Any, Dict[str, Any]]:
    """
    Fetch JSON from either HTTP(S) or local file: URL.
    - For file: sources, no cache is used; read directly each run.
    - For HTTP(S), optional cache is applied.
    """
    if url.startswith("file:"):
        path = _normalize_file_url(url)
        if not os.path.exists(path):
            raise RuntimeError(f"file: source not found for {cache_key}: {path!r}")
        obj = load_json_file(path)
        meta = {
            "url": url,
            "file_path": path,
            "cache_hit": False,
            "http_status": None,
            "content_type": "application/json",
            "content_encoding": "",
            "fetched_at": int(time.time()),
            "timestamp_utc": now_iso_utc(),
        }
        return obj, meta

    # HTTP(S) with cache
    os.makedirs(cache_dir, exist_ok=True)
    cached_path = os.path.join(cache_dir, f"{cache_key}.json")
    meta_path = os.path.join(cache_dir, f"{cache_key}.meta.json")
    now = int(time.time())

    if not no_cache and not refresh and os.path.exists(cached_path) and os.path.exists(meta_path):
        try:
            meta = load_json_file(meta_path)
            fetched_at = int(meta.get("fetched_at", 0))
            if (now - fetched_at) <= max_age_seconds and os.path.getsize(cached_path) > 2:
                return load_json_file(cached_path), {**meta, "cache_hit": True}
        except Exception:
            pass

    raw, headers, status = http_get_bytes(url, timeout=30)
    ctype = headers.get("content-type", "")
    cenc = headers.get("content-encoding", "")
    obj = decode_and_parse_json(raw, cache_key=cache_key, url=url, content_type=ctype, content_encoding=cenc)

    meta = {
        "url": url,
        "cached_path": cached_path,
        "fetched_at": now,
        "sha256": sha256_bytes(raw),
        "content_type": ctype,
        "content_encoding": cenc,
        "http_status": status,
        "cache_hit": False,
        "timestamp_utc": now_iso_utc(),
    }

    if not no_cache:
        write_json_file(cached_path, obj)
        write_json_file(meta_path, meta)

    return obj, meta


# ----------------------------
# Serviceareas parsing
# ----------------------------

def _coords_to_ring(coords: List[List[float]]) -> List[Tuple[float, float]]:
    return [(float(pt[0]), float(pt[1])) for pt in coords]


# ---- Encoded polyline decoder (Google polyline algorithm) ----

def decode_polyline(encoded: str, precision: int) -> List[Tuple[float, float]]:
    """
    Decode an encoded polyline string into [(lat, lon), ...] in degrees.
    precision: 5 -> 1e-5, 6 -> 1e-6
    """
    index = 0
    lat = 0
    lon = 0
    coordinates: List[Tuple[float, float]] = []
    factor = 10 ** precision

    while index < len(encoded):
        # decode latitude
        shift = 0
        result = 0
        while True:
            if index >= len(encoded):
                return coordinates
            b = ord(encoded[index]) - 63
            index += 1
            result |= (b & 0x1F) << shift
            shift += 5
            if b < 0x20:
                break
        dlat = ~(result >> 1) if (result & 1) else (result >> 1)
        lat += dlat

        # decode longitude
        shift = 0
        result = 0
        while True:
            if index >= len(encoded):
                return coordinates
            b = ord(encoded[index]) - 63
            index += 1
            result |= (b & 0x1F) << shift
            shift += 5
            if b < 0x20:
                break
        dlon = ~(result >> 1) if (result & 1) else (result >> 1)
        lon += dlon

        coordinates.append((lat / factor, lon / factor))

    return coordinates


def _score_decoded_points(points_latlon: List[Tuple[float, float]]) -> float:
    """
    Heuristic sanity score for choosing precision 5 vs 6.
    We expect contiguous US-ish utility footprints. This is not "guessing utility";
    it's selecting the correct decoding scale.
    """
    if len(points_latlon) < 3:
        return -1e9

    lats = [p[0] for p in points_latlon]
    lons = [p[1] for p in points_latlon]
    min_lat, max_lat = min(lats), max(lats)
    min_lon, max_lon = min(lons), max(lons)

    # Must be within plausible Earth bounds
    if not (-90 <= min_lat <= 90 and -90 <= max_lat <= 90 and -180 <= min_lon <= 180 and -180 <= max_lon <= 180):
        return -1e9

    # Footprint should not be astronomically large
    lat_span = max_lat - min_lat
    lon_span = max_lon - min_lon
    if lat_span > 40 or lon_span > 80:
        return -1e6

    # Prefer spans that look utility-territory-sized (not microscopic, not planet-scale)
    # (Evergy spans multiple states: still under ~10-15 degrees)
    score = 0.0
    if 0.01 < lat_span < 25:
        score += 10.0
    if 0.01 < lon_span < 35:
        score += 10.0

    # Prefer points roughly in North America range (soft, not a hard gate)
    if 15 <= min_lat <= 60 and 15 <= max_lat <= 60:
        score += 2.0
    if -140 <= min_lon <= -50 and -140 <= max_lon <= -50:
        score += 2.0

    # More points means more plausible ring
    score += min(len(points_latlon), 2000) / 2000.0
    return score


def decode_evergy_geom_a_to_multipolygon(geom_a: List[str]) -> List[List[List[Tuple[float, float]]]]:
    """
    Evergy format: geom.a = [ encoded_string, encoded_string, ... ]
    We decode each string as an independent polygon ring and union them as a MultiPolygon.

    Output MultiPolygon in our internal form:
      multipoly = [ polygon, polygon, ...]
      polygon = [ ring ]  (no holes)
      ring = [(lon,lat), ...]  <-- note order for point-in-polygon (lon,lat)

    We auto-select precision 5 or 6 per segment using a heuristic.
    """
    multipoly: List[List[List[Tuple[float, float]]]] = []

    for seg in geom_a:
        if not isinstance(seg, str) or not seg:
            continue

        # Try precision 5 and 6 and pick the better one
        p5 = decode_polyline(seg, precision=5)
        p6 = decode_polyline(seg, precision=6)

        s5 = _score_decoded_points(p5)
        s6 = _score_decoded_points(p6)
        chosen = p6 if s6 > s5 else p5

        # Convert to lon,lat for geometry routines
        ring_lonlat = [(lon, lat) for (lat, lon) in chosen]

        # Need at least 3 points for a ring
        if len(ring_lonlat) >= 3:
            multipoly.append([ring_lonlat])

    if not multipoly:
        raise ValueError("Evergy geom.a decoded to zero usable rings. geom.a may not be polyline-encoded as expected.")
    return multipoly


def parse_serviceareas(utility_key: str, data: Any) -> List[AreaGeom]:
    """
    Multi-format parser:
    - GeoJSON FeatureCollection / list
    - Evergy packed format (file_title/file_data with geom.a)
    """

    # ---- Evergy packed format fast-path ----
    if isinstance(data, dict) and "file_data" in data and isinstance(data["file_data"], list):
        # Expect elements with geom.a
        out: List[AreaGeom] = []
        for idx, item in enumerate(data["file_data"]):
            if not isinstance(item, dict):
                continue
            geom = item.get("geom") if isinstance(item.get("geom"), dict) else None
            if geom and isinstance(geom.get("a"), list):
                geom_a = geom.get("a")
                title = str(item.get("title") or item.get("id") or f"{utility_key}:{idx}")
                area_id = str(item.get("id") or f"{utility_key}:{idx}")

                # Decode into MultiPolygon union
                multipoly = decode_evergy_geom_a_to_multipolygon(geom_a)  # type: ignore[arg-type]
                out.append(
                    AreaGeom(
                        utility_key=utility_key,
                        area_id=area_id,
                        name=title,
                        geom_type="MultiPolygon",
                        multipolygon=multipoly,
                    )
                )

        if out:
            return out
        # If file_data exists but doesn't contain geom.a, fall through to GeoJSON parsing

    # ---- GeoJSON-like parsing ----
    features: Optional[List[Any]] = None
    if isinstance(data, list):
        features = data
    elif isinstance(data, dict):
        if isinstance(data.get("features"), list):
            features = data["features"]
        elif isinstance(data.get("data"), dict) and isinstance(data["data"].get("features"), list):
            features = data["data"]["features"]

    if not isinstance(features, list) or not features:
        top_keys = list(data.keys()) if isinstance(data, dict) else type(data).__name__
        raise ValueError(f"Unrecognized serviceareas format for {utility_key}. Top-level keys/type: {top_keys}")

    out2: List[AreaGeom] = []
    for idx, feat in enumerate(features):
        if not isinstance(feat, dict):
            continue

        geom = feat.get("geometry") or {}
        gtype = geom.get("type")
        coords = geom.get("coordinates")
        props = feat.get("properties") or {}
        area_id = str(feat.get("id") or props.get("id") or props.get("serviceAreaId") or f"{utility_key}:{idx}")
        name = str(props.get("name") or props.get("label") or props.get("title") or area_id)

        if gtype == "Polygon" and isinstance(coords, list):
            poly: List[List[Tuple[float, float]]] = []
            for ring_coords in coords:
                if isinstance(ring_coords, list):
                    poly.append(_coords_to_ring(ring_coords))
            if poly:
                out2.append(AreaGeom(utility_key, area_id, name, "Polygon", polygon=poly))

        elif gtype == "MultiPolygon" and isinstance(coords, list):
            multipoly: List[List[List[Tuple[float, float]]]] = []
            for poly_coords in coords:
                poly: List[List[Tuple[float, float]]] = []
                if isinstance(poly_coords, list):
                    for ring_coords in poly_coords:
                        if isinstance(ring_coords, list):
                            poly.append(_coords_to_ring(ring_coords))
                multipoly.append(poly)
            if multipoly:
                out2.append(AreaGeom(utility_key, area_id, name, "MultiPolygon", multipolygon=multipoly))

    if not out2:
        raise ValueError(f"No usable Polygon/MultiPolygon geometries parsed for {utility_key}.")
    return out2


# ----------------------------
# Overrides + tagging
# ----------------------------

def load_overrides(path: Optional[str]) -> Dict[str, str]:
    if not path or not os.path.exists(path):
        return {}
    try:
        if os.path.getsize(path) == 0:
            return {}
    except Exception:
        pass

    try:
        raw = load_json_file(path)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Overrides file is not valid JSON: {path!r}. Use {{}} for empty.") from e

    out: Dict[str, str] = {}
    if isinstance(raw, dict):
        for k, v in raw.items():
            if isinstance(k, str) and isinstance(v, str):
                out[k] = v
    return out


def should_tag_site(site: Dict[str, Any], force: bool) -> bool:
    if force:
        return True
    u = site.get("utility")
    return (u is None) or (isinstance(u, str) and u.strip() == "")


def tag_sites_freeze_mode(
    sites: Dict[str, Any],
    areas_by_utility: Dict[str, List[AreaGeom]],
    overrides: Dict[str, str],
    *,
    force: bool,
    freeze_provenance: bool,
    provenance_source: str,
) -> Dict[str, Any]:
    report: Dict[str, Any] = {
        "summary": {
            "total_sites": len(sites),
            "eligible_for_tagging": 0,
            "missing_coords": 0,
            "matched_exactly_one": 0,
            "matched_zero": 0,
            "matched_multiple": 0,
            "utility_changes": 0,
            "overrides_applied": 0,
            "skipped_already_tagged": 0,
        },
        "missing_coords": [],
        "matched_zero": [],
        "matched_multiple": [],
        "changes": [],
        "overrides": [],
        "skipped": [],
    }

    tagged_at = now_iso_utc() if freeze_provenance else None

    for site_id, site in sites.items():
        if not isinstance(site, dict):
            continue

        # Overrides always win
        if site_id in overrides:
            new_u = overrides[site_id]
            old_u = site.get("utility")
            if old_u != new_u:
                site["utility"] = new_u
                report["summary"]["utility_changes"] += 1
                report["changes"].append({"site_id": site_id, "from": old_u, "to": new_u, "reason": "override"})
            report["summary"]["overrides_applied"] += 1
            report["overrides"].append({"site_id": site_id, "utility": new_u})

            if freeze_provenance:
                site["utility_tagged_at"] = tagged_at
                site["utility_source"] = provenance_source
                site["utility_method"] = "override"
            continue

        eligible = should_tag_site(site, force=force)
        if not eligible:
            report["summary"]["skipped_already_tagged"] += 1
            report["skipped"].append({"site_id": site_id, "utility": site.get("utility")})
            continue

        report["summary"]["eligible_for_tagging"] += 1

        lat = site.get("lat")
        lon = site.get("lon")
        if lat is None or lon is None:
            report["summary"]["missing_coords"] += 1
            report["missing_coords"].append({"site_id": site_id, "name": site.get("name")})
            continue

        try:
            latf = float(lat)
            lonf = float(lon)
        except Exception:
            report["summary"]["missing_coords"] += 1
            report["missing_coords"].append(
                {"site_id": site_id, "name": site.get("name"), "bad_values": {"lat": lat, "lon": lon}}
            )
            continue

        matches: List[str] = []
        for utility_key in sorted(areas_by_utility.keys()):
            for area in areas_by_utility[utility_key]:
                if contains(area, lonf, latf):
                    matches.append(utility_key)
                    break

        if len(matches) == 1:
            report["summary"]["matched_exactly_one"] += 1
            new_u = matches[0]
            old_u = site.get("utility")
            if old_u != new_u:
                site["utility"] = new_u
                report["summary"]["utility_changes"] += 1
                report["changes"].append({"site_id": site_id, "from": old_u, "to": new_u, "reason": "polygon_match"})
            if freeze_provenance:
                site["utility_tagged_at"] = tagged_at
                site["utility_source"] = provenance_source
                site["utility_method"] = "polygon"

        elif len(matches) == 0:
            report["summary"]["matched_zero"] += 1
            report["matched_zero"].append({"site_id": site_id, "name": site.get("name"), "lat": latf, "lon": lonf})

        else:
            report["summary"]["matched_multiple"] += 1
            report["matched_multiple"].append(
                {"site_id": site_id, "name": site.get("name"), "lat": latf, "lon": lonf, "matches": matches}
            )

    return report


# ----------------------------
# Main
# ----------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Freeze-mode utility tagger (writes utility into sites.json).")
    ap.add_argument("--sites", required=True, help="Path to sites.json (object keyed by site_id).")
    ap.add_argument("--catalog", required=True, help="Path to utilities_catalog.json.")
    ap.add_argument("--cache-dir", default=".cache/serviceareas", help="Cache directory for HTTP serviceareas.json.")
    ap.add_argument("--overrides", default=None, help="Optional overrides.json (site_id -> utility_key).")
    ap.add_argument("--max-age", type=int, default=7 * 86400, help="Cache max age seconds (default 7 days).")
    ap.add_argument("--report-out", default="utility_tag_report.json", help="Report output path.")
    ap.add_argument("--dry-run", action="store_true", help="Do not write sites.json back.")
    ap.add_argument("--force", action="store_true", help="Overwrite existing utilities (full refresh).")
    ap.add_argument("--freeze-provenance", action="store_true",
                    help="Write provenance fields into sites.json (utility_tagged_at/source/method).")
    ap.add_argument("--provenance-source", default="kubra_serviceareas",
                    help="utility_source value when --freeze-provenance is enabled.")
    ap.add_argument("--no-cache", action="store_true", help="Do not read or write HTTP cache; fetch live each run.")
    ap.add_argument("--refresh", action="store_true", help="Force refresh: fetch live and overwrite HTTP cache.")
    args = ap.parse_args()

    sites_obj = load_json_file(args.sites)
    if not isinstance(sites_obj, dict):
        raise ValueError("sites.json must be an object keyed by site_id")

    catalog = load_json_file(args.catalog)
    if not isinstance(catalog, dict):
        raise ValueError("utilities_catalog.json must be an object keyed by utility_key")

    overrides = load_overrides(args.overrides)

    areas_by_utility: Dict[str, List[AreaGeom]] = {}
    fetch_meta: Dict[str, Any] = {}
    utilities_loaded: Dict[str, int] = {}

    for utility_key in sorted(catalog.keys()):
        entry = catalog[utility_key]
        if not isinstance(entry, dict):
            continue
        url = entry.get("serviceareas_url")
        if not isinstance(url, str) or not url.strip():
            continue

        data, meta = fetch_json_any(
            url=url,
            cache_dir=args.cache_dir,
            cache_key=f"{utility_key}_serviceareas",
            max_age_seconds=args.max_age,
            no_cache=args.no_cache,
            refresh=args.refresh,
        )
        fetch_meta[utility_key] = meta

        areas = parse_serviceareas(utility_key, data)
        areas_by_utility[utility_key] = areas
        utilities_loaded[utility_key] = len(areas)

    report = tag_sites_freeze_mode(
        sites=sites_obj,
        areas_by_utility=areas_by_utility,
        overrides=overrides,
        force=args.force,
        freeze_provenance=args.freeze_provenance,
        provenance_source=args.provenance_source,
    )

    report["fetched"] = fetch_meta
    report["utilities_loaded"] = utilities_loaded
    report["run"] = {
        "force": args.force,
        "freeze_provenance": args.freeze_provenance,
        "provenance_source": args.provenance_source,
        "sites_path": args.sites,
        "catalog_path": args.catalog,
        "overrides_path": args.overrides,
        "max_age_seconds": args.max_age,
        "no_cache": args.no_cache,
        "refresh": args.refresh,
        "timestamp_utc": now_iso_utc(),
    }

    write_json_file(args.report_out, report)

    if not args.dry_run:
        write_json_file(args.sites, sites_obj)

    s = report["summary"]
    print(
        "Freeze-mode utility tagging complete:\n"
        f"- total_sites: {s['total_sites']}\n"
        f"- eligible_for_tagging: {s['eligible_for_tagging']}\n"
        f"- matched_exactly_one: {s['matched_exactly_one']}\n"
        f"- matched_zero: {s['matched_zero']}\n"
        f"- matched_multiple: {s['matched_multiple']}\n"
        f"- missing_coords: {s['missing_coords']}\n"
        f"- overrides_applied: {s['overrides_applied']}\n"
        f"- skipped_already_tagged: {s['skipped_already_tagged']}\n"
        f"- utility_changes: {s['utility_changes']}\n"
        f"Report: {args.report_out}\n"
        f"sites.json {'NOT ' if args.dry_run else ''}updated: {args.sites}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
