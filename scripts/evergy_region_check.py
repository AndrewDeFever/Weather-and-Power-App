#!/usr/bin/env python3
"""
Evergy Region Outage Verification Script

Purpose
- Iterate through EVERGY-tagged sites in data/sites.json
- Call app.providers.evergy_kubra.fetch_evergy_outages for each site
- Produce a compact, operator-friendly summary plus a JSON report

This is intended for validation / spot-checking against Evergy's public outage map:
  https://outagemap.evergy.com/

Notes
- The provider returns outage timestamps in UTC ISO-8601 ("...Z") or Kubra sentinel values
  (ETR-EXP / ETR-NULL). Any UI conversion to America/Chicago is done upstream.
- The Evergy provider module uses a 10-mile primary radius and a 25-mile fallback radius
  by default; you can still pass max_radius_km as an upper bound.

Usage
  python evergy_region_check.py --sites data/sites.json --out evergy_check.json
  python evergy_region_check.py --limit 10 --debug
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

# Allow running from repo root without installing
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

try:
    from app.providers.evergy_kubra import fetch_evergy_outages, EvergyKubraError
except Exception as e:
    print("ERROR: Unable to import app.providers.evergy_kubra.")
    print("Run this script from the project root or ensure PYTHONPATH includes the repo.")
    raise


def _load_sites(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError("sites.json must be a JSON object keyed by site_id")
    return data


def _is_evergy_site(rec: Dict[str, Any]) -> bool:
    util = rec.get("utility")
    if not isinstance(util, str):
        return False
    return util.strip().upper() == "EVERGY"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def run_check(
    sites_path: str,
    out_path: Optional[str],
    limit: Optional[int],
    debug: bool,
    max_radius_km: float,
    max_zoom: int,
    neighbor_depth: int,
    drill_neighbor_depth: int,
    per_site_pause_s: float,
) -> Dict[str, Any]:
    sites = _load_sites(sites_path)

    # Filter: EVERGY + enabled (default True if missing)
    evergy_sites: List[Dict[str, Any]] = []
    for site_id, rec in sites.items():
        if not isinstance(rec, dict):
            continue
        if not _is_evergy_site(rec):
            continue
        if rec.get("enabled") is False:
            continue
        lat = rec.get("lat")
        lon = rec.get("lon")
        if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
            continue
        evergy_sites.append(rec)

    evergy_sites.sort(key=lambda r: str(r.get("site_id") or r.get("name") or ""))

    if limit is not None:
        evergy_sites = evergy_sites[: max(0, int(limit))]

    results: List[Dict[str, Any]] = []
    ok = 0
    errored = 0
    sites_with_outages = 0

    for idx, site in enumerate(evergy_sites, start=1):
        site_id = str(site.get("site_id") or site.get("name") or f"site_{idx}")
        name = str(site.get("name") or site_id)
        lat = float(site["lat"])
        lon = float(site["lon"])

        print(f"[{idx:02d}/{len(evergy_sites):02d}] {site_id} ({name})  lat={lat:.5f} lon={lon:.5f}")

        row: Dict[str, Any] = {
            "site_id": site_id,
            "name": name,
            "lat": lat,
            "lon": lon,
            "utility": "EVERGY",
            "checked_at_utc": _utc_now_iso(),
            "ok": False,
            "error": None,
            "has_outage_nearby": False,
            "nearest": None,
            "outage_count": 0,
        }

        try:
            resp = fetch_evergy_outages(
                lat,
                lon,
                max_radius_km=max_radius_km,
                max_zoom=max_zoom,
                neighbor_depth=neighbor_depth,
                drill_neighbor_depth=drill_neighbor_depth,
                debug=debug,
            )
            outages = resp.get("outages") or []
            nearest = resp.get("nearest")

            row["ok"] = True
            row["outage_count"] = len(outages)
            row["has_outage_nearby"] = bool(nearest) or (len(outages) > 0)
            row["nearest"] = nearest
            ok += 1
            if row["has_outage_nearby"]:
                sites_with_outages += 1

            if nearest:
                print(
                    f"    nearest: id={nearest.get('id')} cust={nearest.get('customers_out')} "
                    f"etr={nearest.get('etr')} start={nearest.get('start_time')} "
                    f"dist_km={nearest.get('distance_km'):.2f}"
                )
            else:
                print("    nearest: None")

            print(f"    outages: {len(outages)}")
        except EvergyKubraError as e:
            row["error"] = f"EvergyKubraError: {e}"
            errored += 1
            print(f"    ERROR: {row['error']}")
        except Exception as e:
            row["error"] = f"{type(e).__name__}: {e}"
            errored += 1
            print(f"    ERROR: {row['error']}")

        results.append(row)

        if per_site_pause_s > 0 and idx < len(evergy_sites):
            time.sleep(per_site_pause_s)

    report: Dict[str, Any] = {
        "generated_at_utc": _utc_now_iso(),
        "sites_path": sites_path,
        "site_count_total": len(sites),
        "evergy_sites_checked": len(evergy_sites),
        "ok": ok,
        "errored": errored,
        "sites_with_outages": sites_with_outages,
        "results": results,
    }

    if out_path:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        print(f"\nWrote report: {out_path}")

    print(
        f"\nSUMMARY: checked={len(evergy_sites)} ok={ok} errored={errored} "
        f"sites_with_outages={sites_with_outages}"
    )
    return report


def main() -> None:
    p = argparse.ArgumentParser(description="Evergy outage verification via Kubra tiles.")
    p.add_argument("--sites", default="data/sites.json", help="Path to sites.json")
    p.add_argument("--out", default=None, help="Write JSON report to this path")
    p.add_argument("--limit", type=int, default=None, help="Limit number of Evergy sites checked")
    p.add_argument("--debug", action="store_true", help="Enable provider debug output (very verbose)")
    p.add_argument("--max-radius-km", type=float, default=50.0, help="Upper bound radius in km (provider uses 10mi/25mi policy)")
    p.add_argument("--max-zoom", type=int, default=12, help="Max zoom to drill (provider default: 12)")
    p.add_argument("--neighbor-depth", type=int, default=1, help="Tile neighborhood depth at entry zoom (default: 1)")
    p.add_argument("--drill-neighbor-depth", type=int, default=1, help="Tile neighborhood depth while drilling clusters (default: 1)")
    p.add_argument("--pause", type=float, default=0.15, help="Pause between sites (seconds) to be polite to Kubra/CDN")
    args = p.parse_args()

    run_check(
        sites_path=args.sites,
        out_path=args.out,
        limit=args.limit,
        debug=args.debug,
        max_radius_km=args.max_radius_km,
        max_zoom=args.max_zoom,
        neighbor_depth=args.neighbor_depth,
        drill_neighbor_depth=args.drill_neighbor_depth,
        per_site_pause_s=args.pause,
    )


if __name__ == "__main__":
    main()
